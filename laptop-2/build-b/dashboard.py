#!/usr/bin/env python3
"""dashboard.py: the live agent-traffic dashboard for Lab 4.

Builds on proxy.py. One process, two ports: the proxy forwards traffic unchanged
and the dashboard shows it in a browser. Standard library only.

    python3 dashboard.py                  # proxy :8041, dashboard :8042
    ANTHROPIC_BASE_URL=http://localhost:8041 claude
    python3 dashboard.py --upstream https://chatgpt.com/backend-api/codex

How forwarding is kept fast: a proxy thread only copies bytes and drops short
notes on a never-blocking queue. One recorder thread does all the parsing,
assembling, pairing and publishing, so a slow parse, a large snapshot or a slow
browser cannot delay the agent.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import http.client
import json
import queue
import re
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

# Same list as proxy.py: never forwarded, in either direction.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}
# §7 names the first three. The other two carry the same kind of secret.
SECRET_HEADERS = {"authorization", "x-api-key", "cookie", "set-cookie", "proxy-authorization"}

HERE = Path(__file__).resolve().parent
HTML_PATH = HERE / "dashboard.html"
PRICING_PATH = HERE / "pricing.json"

UPSTREAM = "https://api.anthropic.com"
QUIET = False
DEFAULT_BUFFER = 200
SUBSCRIBER_QUEUE = 5000
KEEPALIVE_SECONDS = 15
MAX_PLAIN_BODY = 32 * 1024 * 1024  # non-SSE reply bodies kept for display

TOOL_RESULT_ITEM = {"function_call_output", "custom_tool_call_output"}


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def iso(epoch: float) -> str:
    moment = datetime.fromtimestamp(epoch, timezone.utc)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def redact(value: str) -> str:
    """First 16 characters then `...`; a value that short is hidden entirely."""
    return value[:16] + "..." if len(value) > 16 else "..."


def redacted_headers(pairs) -> dict:
    out: dict = {}
    for name, value in pairs:
        if name.lower() in SECRET_HEADERS:
            value = redact(value)
        out[name] = f"{out[name]}, {value}" if name in out else value
    return out


def _no_constants(name: str):
    raise ValueError(name)  # NaN/Infinity would produce JSON that browsers reject


def parse_json(text):
    return json.loads(text, parse_constant=_no_constants)


def to_json_bytes(value) -> bytes:
    return json.dumps(value).encode("ascii")


# ---------------------------------------------------------------- SSE parsing

class SSEParser:
    """Incremental SSE parser. Chunks may split lines and UTF-8 characters."""

    _eol = re.compile(rb"\r\n|\n|\r")

    def __init__(self) -> None:
        self.buf = b""
        self.lines: list[bytes] = []

    def feed(self, chunk: bytes) -> list[dict]:
        self.buf += chunk
        events = []
        start = 0
        for match in self._eol.finditer(self.buf):
            # A trailing \r may be the first half of \r\n: wait for the next chunk.
            if match.group() == b"\r" and match.end() == len(self.buf):
                break
            line = self.buf[start:match.start()]
            start = match.end()
            if line:
                self.lines.append(line)
            elif self.lines:
                events.append(self._dispatch())
        self.buf = self.buf[start:]
        return [e for e in events if e]

    def close(self) -> list[dict]:
        """The stream ended: whatever is left is the last event."""
        if self.buf.strip(b"\r\n"):
            self.lines.append(self.buf.rstrip(b"\r\n"))
        self.buf = b""
        event = self._dispatch() if self.lines else None
        return [event] if event else []

    def _dispatch(self) -> dict | None:
        raw_lines = [ln.decode("utf-8", "replace") for ln in self.lines]
        self.lines = []
        name, data = None, []
        for line in raw_lines:
            if line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "event":
                name = value
            elif field == "data":
                data.append(value)
        if name is None and not data:
            return None  # comments only
        raw = "\n".join(raw_lines)
        event = {"event": name, "data": None, "raw": raw}
        if data:
            text = "\n".join(data)
            try:
                event["data"] = parse_json(text)
            except (ValueError, RecursionError):
                # §8: kept as a raw string, never interrupts anything.
                event["data"] = text
                if text.strip() != "[DONE]":
                    event["parse_error"] = True
        return event


# ------------------------------------------------------------------ assembling

def empty_assembled() -> dict:
    return {"model": None, "content": [], "stop_reason": None, "usage": None}


def merge_usage(current: dict | None, new: dict) -> dict:
    """message_delta repeats totals, so later values replace earlier ones."""
    merged = dict(current or {})
    merged.update({k: v for k, v in new.items() if v is not None})
    return merged


def codex_usage(usage: dict) -> dict:
    """Responses API input_tokens includes cached tokens; Messages API does not."""
    cached = (usage.get("input_tokens_details") or {}).get("cached_tokens") or 0
    return {
        "input_tokens": (usage.get("input_tokens") or 0) - cached,
        "output_tokens": usage.get("output_tokens") or 0,
        "cache_read_input_tokens": cached,
        "cache_creation_input_tokens": 0,
    }


def parse_arguments(text: str, block: dict) -> None:
    try:
        block["input"] = parse_json(text)
    except (ValueError, RecursionError):
        block["input"] = {}
        block["input_raw"] = text


class Assembler:
    """Turns stream events into `assembled`, and reports each change as small
    patches, so the browser never has to know the stream formats."""

    def __init__(self) -> None:
        self.result = empty_assembled()
        self.positions: dict = {}
        self.partial: dict[int, str] = {}
        self.finished = False  # saw the event that ends a healthy stream
        self.kind: str | None = None

    # Each apply returns (patches, first_token, finished_tool_blocks, error).
    def apply(self, name, data):
        if not isinstance(data, dict):
            return [], False, [], None
        kind = data.get("type") or name or ""
        if kind.startswith("response."):
            self.kind = "codex"
            return self._codex(kind, data)
        self.kind = self.kind or "anthropic"
        return self._anthropic(kind, data)

    def _meta(self, **values) -> dict:
        self.result.update(values)
        return {"op": "meta", "set": copy.deepcopy(values)}

    def _block(self, key, block: dict) -> tuple[int, dict]:
        position = self.positions.get(key)
        if position is None:
            position = len(self.result["content"])
            self.positions[key] = position
            self.result["content"].append(block)
        else:
            self.result["content"][position] = block
        return position, {"op": "block", "i": position, "block": copy.deepcopy(block)}

    def _append(self, position: int, field: str, text: str) -> dict:
        block = self.result["content"][position]
        block[field] = (block.get(field) or "") + text
        return {"op": "append", "i": position, "field": field, "text": text}

    def _partial(self, position: int, text: str) -> dict:
        self.partial[position] = self.partial.get(position, "") + text
        count = len(self.partial[position])
        self.result["content"][position]["partial_chars"] = count
        return {"op": "partial", "i": position, "chars": count}

    def _close_block(self, position: int) -> None:
        block = self.result["content"][position]
        block.pop("partial_chars", None)
        text = self.partial.pop(position, None)
        if text:
            parse_arguments(text, block)

    def _anthropic(self, kind: str, data: dict):
        patches: list = []
        first = False
        tools: list = []
        error = None
        if kind == "message_start":
            message = data.get("message") or {}
            values = {k: message[k] for k in ("id", "model", "role") if message.get(k) is not None}
            values["usage"] = merge_usage(self.result["usage"], message.get("usage") or {})
            patches.append(self._meta(**values))
        elif kind == "content_block_start":
            block = dict(data.get("content_block") or {})
            _, patch = self._block(data.get("index"), block)
            patches.append(patch)
        elif kind == "content_block_delta":
            position = self.positions.get(data.get("index"))
            delta = data.get("delta") or {}
            if position is not None:
                first = True
                dtype = delta.get("type")
                if dtype == "text_delta":
                    patches.append(self._append(position, "text", delta.get("text") or ""))
                elif dtype == "thinking_delta":
                    patches.append(self._append(position, "thinking", delta.get("thinking") or ""))
                elif dtype == "signature_delta":
                    first = False
                    patches.append(self._append(position, "signature", delta.get("signature") or ""))
                elif dtype == "input_json_delta":
                    patches.append(self._partial(position, delta.get("partial_json") or ""))
                elif dtype == "citations_delta":
                    block = self.result["content"][position]
                    block.setdefault("citations", []).append(delta.get("citation"))
                    patches.append({"op": "block", "i": position, "block": copy.deepcopy(block)})
        elif kind == "content_block_stop":
            position = self.positions.get(data.get("index"))
            if position is not None:
                self._close_block(position)
                block = self.result["content"][position]
                patches.append({"op": "block", "i": position, "block": copy.deepcopy(block)})
                if block.get("type") == "tool_use":
                    tools.append(block)
        elif kind == "message_delta":
            values = {}
            delta = data.get("delta") or {}
            for field in ("stop_reason", "stop_sequence"):
                if delta.get(field) is not None:
                    values[field] = delta[field]
            if data.get("usage"):
                values["usage"] = merge_usage(self.result["usage"], data["usage"])
            patches.append(self._meta(**values))
        elif kind == "message_stop":
            self.finished = True
        elif kind == "error":
            detail = data.get("error") or {}
            error = f"{detail.get('type', 'error')}: {detail.get('message', '')}"
            self.finished = True
        return patches, first, tools, error

    def _codex_item(self, data: dict, item: dict | None = None):
        """Block for one output item, keyed by output_index."""
        item = item or data.get("item") or {}
        key = ("out", data.get("output_index"))
        kind = item.get("type")
        if kind in ("function_call", "custom_tool_call"):
            return key, {"type": "tool_use", "id": item.get("call_id"), "name": item.get("name"), "input": {}}
        if kind == "reasoning":
            return key, {"type": "thinking", "thinking": ""}
        if kind == "message":
            return key, {"type": "text", "text": ""}
        return None, None

    def _codex(self, kind: str, data: dict):
        patches: list = []
        first = False
        tools: list = []
        error = None
        if kind in ("response.created", "response.in_progress"):
            response = data.get("response") or {}
            if response.get("model"):
                patches.append(self._meta(model=response["model"], id=response.get("id")))
        elif kind == "response.output_item.added":
            key, block = self._codex_item(data)
            if block:
                patches.append(self._block(key, block)[1])
        elif kind in ("response.output_text.delta", "response.refusal.delta",
                      "response.reasoning_summary_text.delta", "response.reasoning_text.delta"):
            field = "thinking" if "reasoning" in kind else "text"
            key = ("out", data.get("output_index"))
            if key not in self.positions:
                patches.append(self._block(key, {"type": field, field: ""})[1])
            first = True
            patches.append(self._append(self.positions[key], field, data.get("delta") or ""))
        elif kind in ("response.function_call_arguments.delta", "response.custom_tool_call_input.delta"):
            position = self.positions.get(("out", data.get("output_index")))
            if position is not None:
                first = True
                patches.append(self._partial(position, data.get("delta") or ""))
        elif kind == "response.output_item.done":
            item = data.get("item") or {}
            key, block = self._codex_item(data)
            if block:
                position = self.positions.get(key)
                if position is None:
                    position, patch = self._block(key, block)
                    patches.append(patch)
                if block["type"] == "tool_use":
                    self.partial.pop(position, None)
                    current = self.result["content"][position]
                    current.pop("partial_chars", None)
                    if item.get("type") == "custom_tool_call":
                        current["input"] = item.get("input")
                    else:
                        parse_arguments(item.get("arguments") or "{}", current)
                    patches.append({"op": "block", "i": position, "block": copy.deepcopy(current)})
                    tools.append(current)
                elif block["type"] == "text" and not self.result["content"][position].get("text"):
                    text = "".join(p.get("text") or "" for p in item.get("content") or [])
                    self.result["content"][position]["text"] = text
                    patches.append({"op": "block", "i": position,
                                    "block": copy.deepcopy(self.result["content"][position])})
        elif kind in ("response.completed", "response.incomplete", "response.failed"):
            response = data.get("response") or {}
            self.finished = True
            values = {}
            if response.get("model"):
                values["model"] = response["model"]
            if response.get("usage"):
                values["usage"] = codex_usage(response["usage"])
            has_tool = any(b.get("type") == "tool_use" for b in self.result["content"])
            values["stop_reason"] = "tool_use" if has_tool and kind == "response.completed" else response.get("status")
            patches.append(self._meta(**values))
            if kind == "response.failed":
                detail = response.get("error") or {}
                error = f"{detail.get('code', 'error')}: {detail.get('message', '')}"
        return patches, first, tools, error

    def finalize(self) -> None:
        for position in list(self.partial):
            self._close_block(position)
        for block in self.result["content"]:
            block.pop("partial_chars", None)

    def from_body(self, body) -> list[dict]:
        """A non-streaming reply. Returns its finished tool_use blocks."""
        if not isinstance(body, dict):
            return []
        if body.get("type") == "message":
            for key in ("id", "model", "role", "stop_reason", "stop_sequence"):
                if body.get(key) is not None:
                    self.result[key] = body[key]
            self.result["usage"] = body.get("usage")
            self.result["content"] = list(body.get("content") or [])
        elif body.get("object") == "response":
            self.result["model"] = body.get("model")
            if body.get("usage"):
                self.result["usage"] = codex_usage(body["usage"])
            for item in body.get("output") or []:
                _, block = self._codex_item({}, item)
                if not block:
                    continue
                if block["type"] == "tool_use":
                    parse_arguments(item.get("arguments") or "{}", block)
                elif block["type"] == "text":
                    block["text"] = "".join(p.get("text") or "" for p in item.get("content") or [])
                self.result["content"].append(block)
            has_tool = any(b["type"] == "tool_use" for b in self.result["content"])
            self.result["stop_reason"] = "tool_use" if has_tool else body.get("status")
        return [b for b in self.result["content"] if b.get("type") == "tool_use"]


# ------------------------------------------------------------- request shapes

def normalize_codex(body: dict) -> dict:
    """Codex (Responses API) as system/tools/messages. messages[i] is input[i]."""
    system = []
    if body.get("instructions"):
        system.append({"type": "text", "text": body["instructions"]})
    tools = []
    for tool in body.get("tools") or []:
        tools.append({"name": tool.get("name") or tool.get("type"), "description": tool.get("description"),
                      "input_schema": tool.get("parameters")})
    items = body.get("input")
    if isinstance(items, str):
        items = [{"type": "message", "role": "user", "content": items}]
    messages = [_codex_message(item) for item in items or []]
    params = {k: v for k, v in body.items() if k not in ("instructions", "tools", "input")}
    return {"system": system, "tools": tools, "messages": messages, "params": params}


def _codex_message(item) -> dict:
    if not isinstance(item, dict):
        return {"role": "user", "content": str(item)}
    kind = item.get("type") or "message"
    if kind == "message":
        content = item.get("content")
        if isinstance(content, list):
            blocks = []
            for part in content:
                ptype = part.get("type") if isinstance(part, dict) else None
                if ptype in ("input_text", "output_text"):
                    blocks.append({"type": "text", "text": part.get("text") or ""})
                elif ptype == "input_image":
                    url = part.get("image_url") or ""
                    media = url[5:].split(";")[0] if url.startswith("data:") else "url"
                    blocks.append({"type": "image", "source": {"media_type": media, "data": url}})
                else:
                    blocks.append(part)
            content = blocks
        return {"role": item.get("role") or "user", "content": content}
    if kind in ("function_call", "custom_tool_call"):
        block = {"type": "tool_use", "id": item.get("call_id"), "name": item.get("name"), "input": {}}
        if kind == "custom_tool_call":
            block["input"] = item.get("input")
        else:
            parse_arguments(item.get("arguments") or "{}", block)
        return {"role": "assistant", "content": [block]}
    if kind in TOOL_RESULT_ITEM:
        return {"role": "user", "content": [{"type": "tool_result", "tool_use_id": item.get("call_id"),
                                             "content": item.get("output")}]}
    if kind == "reasoning":
        text = "\n".join(p.get("text") or "" for p in item.get("summary") or [] if isinstance(p, dict))
        return {"role": "assistant", "content": [{"type": "thinking", "thinking": text}]}
    return {"role": item.get("role") or "assistant", "content": [item]}


def request_messages(body, normalized) -> list:
    source = normalized if normalized else body
    messages = source.get("messages") if isinstance(source, dict) else None
    return [m for m in messages if isinstance(m, dict)] if isinstance(messages, list) else []


def is_model_call(method: str, path: str, body) -> bool:
    """Token counting carries whole conversations too, but nothing is answered."""
    if method != "POST" or not isinstance(body, dict) or "count_tokens" in path:
        return False
    return isinstance(body.get("messages"), list) or ("input" in body and "model" in body)


def strip_cache_control(value):
    if isinstance(value, dict):
        return {k: strip_cache_control(v) for k, v in value.items() if k != "cache_control"}
    if isinstance(value, list):
        return [strip_cache_control(v) for v in value]
    return value


def digest(value) -> str:
    text = json.dumps(value, sort_keys=True)
    return hashlib.sha1(text.encode("utf-8", "surrogatepass")).hexdigest()[:16]


def message_digest(message: dict) -> str:
    """Same message, same digest. Harnesses move cache_control between turns and
    may send a bare string where they later send a text block."""
    content = message.get("content")
    if isinstance(content, str):
        content = [{"type": "text", "text": content}]
    return digest([message.get("role"), strip_cache_control(content)])


def conversation_key(body, normalized, messages: list) -> dict:
    source = normalized if normalized else body
    system = source.get("system")
    if isinstance(system, str):
        system = [{"type": "text", "text": system}]
    texts = []
    for block in system or []:
        text = block.get("text", "") if isinstance(block, dict) else str(block)
        # Claude Code puts a per-request billing line in system; it is not the prompt.
        if not text.startswith("x-anthropic-billing-header"):
            texts.append(text)
    return {"system": digest(texts), "messages": [message_digest(m) for m in messages]}


def blocks_of(message: dict) -> list:
    content = message.get("content")
    return [b for b in content if isinstance(b, dict)] if isinstance(content, list) else []


def result_size(content) -> int:
    """Characters of text in a tool result."""
    if content is None:
        return 0
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        total = 0
        for part in content:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                total += len(part["text"])
            else:
                total += len(json.dumps(part))
        return total
    return len(json.dumps(content))


# --------------------------------------------------------------------- pricing

class Pricing:
    """pricing.json, re-read when it changes. A broken edit keeps the last good table."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.mtime = None
        self.table: dict = {}

    def _refresh(self) -> None:
        try:
            mtime = self.path.stat().st_mtime_ns
            if mtime != self.mtime:
                self.mtime = mtime
                self.table = json.loads(self.path.read_text("utf-8"))
        except (OSError, ValueError) as e:
            print(f"pricing.json: {e}", file=sys.stderr, flush=True)

    def cost(self, model, usage) -> float | None:
        self._refresh()
        if not model or not usage:
            return None
        entry = self.table.get(model) or self.table.get(re.sub(r"-\d{8}$", "", model))
        if not isinstance(entry, dict):
            return None
        total = 0.0
        for field, price in (("input_tokens", "input"), ("output_tokens", "output"),
                             ("cache_read_input_tokens", "cache_read"),
                             ("cache_creation_input_tokens", "cache_write")):
            rate = entry.get(price)
            if not isinstance(rate, (int, float)):
                return None
            total += (usage.get(field) or 0) * rate
        return round(total / 1_000_000, 8)


# ------------------------------------------------- store, event bus, recorder

class Subscriber:
    def __init__(self) -> None:
        self.queue: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE)
        self.lost_events = False


class Recording:
    """What a proxy thread holds. Every method returns at once."""

    def __init__(self, store: "Store", t0: float) -> None:
        self.store = store
        self.t0 = t0
        self.exchange: dict | None = None
        self.parser: SSEParser | None = None
        self.assembler = Assembler()
        self.plain = bytearray()

    def response(self, status, headers, now):
        self.store.post(self.store.on_response, self, status, headers, now)

    def chunk(self, data: bytes, now: float):
        self.store.post(self.store.on_chunk, self, data, now)

    def finish(self, error, now: float, wall: float):
        self.store.post(self.store.on_finish, self, error, now, wall)


class Store:
    """Ring buffer of exchanges, the tool index, and the event bus.

    Only the recorder thread changes exchanges, but readers (a browser connecting,
    an export) copy under the lock. Publishing happens under the same lock, so a
    subscriber's snapshot and its first live event can have no gap or overlap.
    """

    def __init__(self, capacity: int, pricing: Pricing) -> None:
        self.capacity = capacity
        self.pricing = pricing
        self.lock = threading.Lock()
        self.jobs: queue.SimpleQueue = queue.SimpleQueue()
        self.exchanges: OrderedDict[int, dict] = OrderedDict()
        self.tools: dict[str, dict] = {}
        self.subscribers: set[Subscriber] = set()
        self.next_id = 1
        threading.Thread(target=self._work, daemon=True, name="recorder").start()

    def post(self, fn, *args) -> None:
        self.jobs.put((fn, args))  # SimpleQueue.put never blocks

    def _work(self) -> None:
        while True:
            fn, args = self.jobs.get()
            try:
                with self.lock:
                    fn(*args)
            except Exception as e:  # a recording problem must never reach the proxy
                print(f"[{stamp()}] dashboard: {type(e).__name__} in {fn.__name__}: {e}",
                      file=sys.stderr, flush=True)

    # ---- bus

    def publish(self, name: str, payload) -> None:
        """Never blocks. A full subscriber loses the event and is resynced later."""
        for sub in self.subscribers:
            try:
                sub.queue.put_nowait((name, payload))
            except queue.Full:
                sub.lost_events = True

    def subscribe(self) -> tuple[Subscriber, list, dict]:
        sub = Subscriber()
        with self.lock:
            snapshot, tools = self._copy_state()
            self.subscribers.add(sub)
        return sub, snapshot, tools

    def unsubscribe(self, sub: Subscriber) -> None:
        with self.lock:
            self.subscribers.discard(sub)

    def resync(self, sub: Subscriber) -> tuple[list, dict]:
        """Events were lost for this subscriber: replace its queue by a snapshot."""
        with self.lock:
            while True:
                try:
                    sub.queue.get_nowait()
                except queue.Empty:
                    break
            sub.lost_events = False
            return self._copy_state()

    # ---- reading

    @staticmethod
    def _public(ex: dict, full: bool = False) -> dict:
        out = dict(ex)
        out["response_events"] = list(ex["response_events"])
        out["assembled"] = copy.deepcopy(ex["assembled"])
        out["timing"] = dict(ex["timing"])
        if not full:
            out.pop("request_raw", None)  # only for the Raw tab, the API and exports
        return out

    def _copy_state(self) -> tuple[list, dict]:
        return ([self._public(e) for e in self.exchanges.values()],
                {k: dict(v) for k, v in self.tools.items()})

    def exchange(self, exchange_id: int) -> dict | None:
        with self.lock:
            ex = self.exchanges.get(exchange_id)
            return self._public(ex, full=True) if ex else None

    def export(self) -> list:
        with self.lock:
            return [self._public(e, full=True) for e in self.exchanges.values()]

    def tool_index(self) -> dict:
        with self.lock:
            return {k: dict(v) for k, v in self.tools.items()}

    def clear(self) -> None:
        with self.lock:
            self.exchanges.clear()
            self.tools.clear()
            self.publish("snapshot", [])
            self.publish("tool_index", {})

    # ---- recording (recorder thread, lock held)

    def start(self, method, path, headers, body: bytes, t0: float, wall: float) -> Recording:
        rec = Recording(self, t0)
        self.post(self.on_started, rec, method, path, headers, body, wall)
        return rec

    def on_started(self, rec, method, path, headers, body_bytes, wall) -> None:
        text = body_bytes.decode("utf-8", "replace")
        request_body = text if body_bytes else None
        if body_bytes:
            try:
                request_body = parse_json(body_bytes)
            except (ValueError, RecursionError):
                pass
        model_call = is_model_call(method, path, request_body)
        normalized = None
        if model_call and "messages" not in request_body:
            normalized = normalize_codex(request_body)
        messages = request_messages(request_body, normalized) if model_call else []
        ex = {
            "id": self.next_id,
            "started_at": iso(wall),
            "ended_at": None,
            "method": method,
            "path": path,
            "status": None,
            "request_headers": headers,
            "request_body": request_body,
            "request_raw": text,
            "normalized_request": normalized,
            "is_model_call": model_call,
            "conversation": conversation_key(request_body, normalized, messages) if model_call else None,
            "response_headers": {},
            "response_events": [],
            "response_body": None,
            "assembled": rec.assembler.result,
            "timing": {"ttfb_ms": None, "ttft_ms": None, "total_ms": None},
            "cost_usd": None,
            "error": None,
        }
        self.next_id += 1
        rec.exchange = ex
        self.exchanges[ex["id"]] = ex
        self.publish("exchange_started", {
            "id": ex["id"], "time": ex["started_at"], "method": method, "path": path,
            "headers": headers, "body": request_body, "normalized_request": normalized,
            "is_model_call": model_call, "conversation": ex["conversation"],
        })
        if model_call:
            self._scan_request(ex, messages, wall)
        while len(self.exchanges) > self.capacity:
            self._evict_oldest()

    def _evict_oldest(self) -> None:
        old_id, _ = self.exchanges.popitem(last=False)
        for key in [k for k, v in self.tools.items() if v["call_exchange"] == old_id]:
            del self.tools[key]
        self.publish("exchange_evicted", {"id": old_id})

    def _alive(self, rec: Recording) -> bool:
        return rec.exchange is not None and rec.exchange["id"] in self.exchanges

    def on_response(self, rec, status, headers, now) -> None:
        ex = rec.exchange
        ex["status"] = status
        ex["response_headers"] = headers
        ex["timing"]["ttfb_ms"] = round((now - rec.t0) * 1000)
        content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "")
        if "text/event-stream" in content_type.lower():
            rec.parser = SSEParser()
        if self._alive(rec):
            self.publish("exchange_response", {"id": ex["id"], "status": status, "headers": headers,
                                               "timing": dict(ex["timing"])})

    def on_chunk(self, rec, data: bytes, now: float) -> None:
        if rec.parser is None:
            room = MAX_PLAIN_BODY - len(rec.plain)
            if room > 0:
                rec.plain += data[:room]
            return
        for event in rec.parser.feed(data):
            self._on_event(rec, event, now)

    def _on_event(self, rec, event: dict, now: float) -> None:
        ex = rec.exchange
        ex["response_events"].append(event)
        patches, first, tools, error = rec.assembler.apply(event["event"], event["data"])
        alive = self._alive(rec)
        if first and ex["timing"]["ttft_ms"] is None:
            ex["timing"]["ttft_ms"] = round((now - rec.t0) * 1000)
        if error:
            ex["error"] = error
        if alive:
            self.publish("exchange_delta", {"id": ex["id"], "event": event, "patches": patches,
                                            "ttft_ms": ex["timing"]["ttft_ms"]})
            for block in tools:
                self._index_tool_call(ex, block)
            if error:
                self.publish("exchange_error", {"id": ex["id"], "message": error})

    def on_finish(self, rec, error, now: float, wall: float) -> None:
        ex = rec.exchange
        if rec.parser is not None:
            for event in rec.parser.close():
                self._on_event(rec, event, now)
        rec.assembler.finalize()
        extra_tools: list = []
        if rec.parser is None and rec.plain:
            text = bytes(rec.plain).decode("utf-8", "replace")
            try:
                ex["response_body"] = parse_json(text)
            except (ValueError, RecursionError):
                ex["response_body"] = text
            if len(rec.plain) >= MAX_PLAIN_BODY:
                ex["response_body_truncated"] = True
            extra_tools = rec.assembler.from_body(ex["response_body"])
        if not error and rec.parser is not None and ex["response_events"] and not rec.assembler.finished \
                and rec.assembler.kind:
            error = "stream ended before its final event"
        ex["ended_at"] = iso(wall)
        ex["timing"]["total_ms"] = round((now - rec.t0) * 1000)
        result = rec.assembler.result
        request = ex["request_body"] if isinstance(ex["request_body"], dict) else {}
        ex["cost_usd"] = self.pricing.cost(result.get("model") or request.get("model"), result.get("usage"))
        if error:
            ex["error"] = error
        if not self._alive(rec):
            return
        for block in extra_tools:
            self._index_tool_call(ex, block)
        payload = {"id": ex["id"], "ended_at": ex["ended_at"], "assembled": copy.deepcopy(result),
                   "timing": dict(ex["timing"]), "cost_usd": ex["cost_usd"], "error": ex["error"],
                   "response_body": ex["response_body"]}
        if error:
            self.publish("exchange_error", {**payload, "message": error})
        else:
            self.publish("exchange_done", payload)
        self._reply_ended(ex, wall)

    # ---- tool index

    def _index_tool_call(self, ex: dict, block: dict) -> None:
        tool_id = block.get("id")
        if not tool_id or tool_id in self.tools:
            return
        body = ex["normalized_request"] or ex["request_body"]
        count = len(body.get("messages") or []) if isinstance(body, dict) else 0
        self.tools[tool_id] = {
            "tool_use_id": tool_id, "name": block.get("name"), "input": block.get("input"),
            "call_exchange": ex["id"], "call_message": count,
            "result_exchange": None, "result_message": None, "result": None, "is_error": False,
            "result_size": None, "status": "pending", "call_ended_at": None, "call_ended_epoch": None,
            "result_started_at": None, "call_to_result_ms": None,
        }
        self.publish("tool_update", dict(self.tools[tool_id]))

    def _reply_ended(self, ex: dict, wall: float) -> None:
        """Call-to-result time starts here, at the end of the reply."""
        for entry in self.tools.values():
            if entry["call_exchange"] == ex["id"]:
                entry["call_ended_at"] = ex["ended_at"]
                entry["call_ended_epoch"] = wall
                self.publish("tool_update", dict(entry))

    def _scan_request(self, ex: dict, messages: list, wall: float) -> None:
        results: dict = {}
        calls: set = set()
        for index, message in enumerate(messages):
            for block in blocks_of(message):
                if block.get("type") == "tool_result" and block.get("tool_use_id") not in results:
                    results[block.get("tool_use_id")] = (block, index)
                elif block.get("type") == "tool_use":
                    calls.add(block.get("id"))
        # The whole history is resent every turn, so only the first request that
        # carries a result pairs it. Unknown ids (from before a clear, or before
        # the proxy started) stay out of the index.
        for tool_id, (block, index) in results.items():
            entry = self.tools.get(tool_id)
            if entry and entry["result_exchange"] is None and entry["call_exchange"] < ex["id"]:
                self._pair(entry, ex, block, index, wall)
        for tool_id, entry in self.tools.items():
            if entry["status"] == "pending" and entry["call_exchange"] < ex["id"] \
                    and tool_id not in results and self._moved_on(entry, ex, calls, tool_id):
                entry["status"] = "no_result"
                self.publish("tool_update", dict(entry))

    def _pair(self, entry: dict, ex: dict, block: dict, index: int, wall: float) -> None:
        content = block.get("content")
        entry.update(result_exchange=ex["id"], result_message=index, result=content,
                     is_error=bool(block.get("is_error")), result_size=result_size(content),
                     status="paired", result_started_at=ex["started_at"])
        if entry["call_ended_epoch"] is not None:
            entry["call_to_result_ms"] = max(0, round((wall - entry["call_ended_epoch"]) * 1000))
        self.publish("tool_paired", dict(entry))

    def _moved_on(self, entry: dict, ex: dict, calls: set, tool_id: str) -> bool:
        """Only this conversation can move a call on. A parallel subagent's
        requests share neither the call nor the history, so they never count."""
        if tool_id in calls:
            return True  # the history holds the call, and no result answers it
        origin = self.exchanges.get(entry["call_exchange"])
        if not origin or not origin["conversation"] or origin["ended_at"] is None:
            return False
        before, now = origin["conversation"], ex["conversation"]
        count = len(before["messages"])
        # Same history as the call's request, then a different next message:
        # the harness dropped the reply. Equal length is a plain retry.
        return (before["system"] == now["system"] and len(now["messages"]) > count
                and now["messages"][:count] == before["messages"])


STORE: Store = None  # type: ignore[assignment]


# ----------------------------------------------------------------------- proxy

def say(*args, **kwargs) -> None:
    """Terminal output, as in proxy.py. It may never break forwarding."""
    if QUIET:
        return
    try:
        print(*args, **kwargs)
    except (OSError, ValueError):
        pass


class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def do_POST(self) -> None:
        self.relay()

    def do_GET(self) -> None:
        self.relay()

    def do_HEAD(self) -> None:
        self.relay()

    def _bad_gateway(self, rec: Recording, log: str, message: str, recorded: str) -> None:
        say(f"\n[{stamp()}] !!! {log}", flush=True)
        try:
            self.send_error(502, message)
        except OSError:
            pass
        self.close_connection = True
        rec.finish(recorded, time.monotonic(), time.time())

    def relay(self) -> None:
        t0, wall = time.monotonic(), time.time()
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        safe_headers = redacted_headers(self.headers.items())
        rec = STORE.start(self.command, self.path, safe_headers, body, t0, wall)

        # 1. Print what the agent sent.
        if not QUIET:
            say(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
            for name, value in self.headers.items():
                if name.lower() in SECRET_HEADERS:
                    value = redact(value)
                say(f"    {name}: {value}")
            if body:
                try:
                    say(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
                except ValueError:
                    say(body.decode("utf-8", "replace"))

        # 2. Forward it, headers intact.
        target = urlsplit(UPSTREAM)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
        except OSError as e:
            self._bad_gateway(rec, f"cannot reach {UPSTREAM}: {e}", f"proxy: cannot reach {UPSTREAM}: {e}",
                              f"upstream unreachable: {e}")
            return
        try:
            upstream.sock.settimeout(600)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
            try:
                upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
                reply = upstream.getresponse()
            except (OSError, http.client.HTTPException) as e:
                self._bad_gateway(rec, f"upstream failed: {e}", f"proxy: upstream failed: {e}",
                                  f"upstream failed: {e}")
                return
            rec.response(reply.status, redacted_headers(reply.getheaders()), time.monotonic())
            self._stream_back(rec, reply)
        finally:
            upstream.close()

    def _stream_back(self, rec: Recording, reply) -> None:
        # 3. Stream the reply back as it arrives, printing it on the way.
        say(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        error = None
        try:
            self.send_response(reply.status, reply.reason)
            for name, value in reply.getheaders():
                if name.lower() not in SKIP and name.lower() != "content-encoding":
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
        except OSError:
            error = "client disconnected"
        self.close_connection = True
        if self.command != "HEAD":
            while not error:
                try:
                    chunk = reply.read1(4096)  # read1: pass on each chunk as it lands
                except (OSError, http.client.HTTPException) as e:
                    error = f"upstream failed mid-stream: {e}"
                    break
                if not chunk:
                    break
                now = time.monotonic()
                try:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                except OSError:
                    error = "client disconnected"
                rec.chunk(chunk, now)
                if not QUIET:
                    try:
                        sys.stdout.write(chunk.decode("utf-8", "replace"))
                        sys.stdout.flush()
                    except (OSError, ValueError):
                        pass
        rec.finish(error, time.monotonic(), time.time())
        if error:
            say(f"\n[{stamp()}] !!! {error}", flush=True)
        elif self.command != "HEAD":
            say(f"\n[{stamp()}] --- done", flush=True)


# ------------------------------------------------------------ dashboard server

CSP = ("default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; "
       "base-uri 'none'; form-action 'none'")
LOCAL_HOSTS = {"localhost", "127.0.0.1", "[::1]"}


class Dashboard(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def _local_only(self) -> bool:
        """Rejects other sites reaching this port, by DNS rebinding or a cross-site POST."""
        host = (self.headers.get("host") or "").rsplit(":", 1)[0]
        origin = self.headers.get("origin")
        if host in LOCAL_HOSTS and (not origin or urlsplit(origin).netloc == self.headers.get("host")):
            return True
        self._send(403, b'{"error":"local requests only"}', "application/json")
        return False

    def _send(self, status: int, body: bytes, content_type: str, extra: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, value, status: int = 200, extra: dict | None = None) -> None:
        self._send(status, to_json_bytes(value), "application/json", extra)

    def do_GET(self) -> None:
        if not self._local_only():
            return
        path = urlsplit(self.path).path
        match = re.fullmatch(r"/api/exchanges/(\d+)", path)
        if path == "/":
            try:
                html = HTML_PATH.read_bytes()
            except OSError as e:
                self._send(500, str(e).encode(), "text/plain; charset=utf-8")
                return
            self._send(200, html, "text/html; charset=utf-8", {"Content-Security-Policy": CSP})
        elif path == "/events":
            self._events()
        elif match:
            ex = STORE.exchange(int(match.group(1)))
            if ex:
                self._json(ex)
            else:
                self._json({"error": "not in the buffer"}, 404)
        elif path == "/api/export":
            name = f"traffic-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
            self._json(STORE.export(), extra={"Content-Disposition": f'attachment; filename="{name}"'})
        elif path == "/api/tools":
            self._json(STORE.tool_index())
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        if not self._local_only():
            return
        length = int(self.headers.get("content-length") or 0)
        if length:
            self.rfile.read(length)
        if urlsplit(self.path).path == "/api/clear":
            STORE.clear()
            self._json({"ok": True})
        else:
            self._json({"error": "not found"}, 404)

    def _write_event(self, name: str, payload) -> None:
        self.wfile.write(b"event: " + name.encode() + b"\ndata: " + to_json_bytes(payload) + b"\n\n")

    def _write_state(self, snapshot: list, tools: dict) -> None:
        self._write_event("snapshot", snapshot)
        self._write_event("tool_index", tools)

    def _events(self) -> None:
        self.close_connection = True
        sub, snapshot, tools = STORE.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            self.wfile.write(b"retry: 1500\n\n")
            self._write_state(snapshot, tools)
            del snapshot, tools
            while True:
                if sub.lost_events:
                    self._write_state(*STORE.resync(sub))
                try:
                    name, payload = sub.queue.get(timeout=KEEPALIVE_SECONDS)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    continue
                self._write_event(name, payload)
        except OSError:
            pass  # the browser went away; nothing else depends on it
        finally:
            STORE.unsubscribe(sub)


class Server(ThreadingHTTPServer):
    request_queue_size = 128  # agents burst parallel requests


def main() -> None:
    global UPSTREAM, QUIET, STORE
    parser = argparse.ArgumentParser(description="Forward an agent's API traffic and show it live in a browser.")
    parser.add_argument("--port", type=int, default=8041, help="proxy port")
    parser.add_argument("--ui-port", type=int, default=8042, help="dashboard port")
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--quiet", action="store_true", help="do not print traffic to the terminal")
    parser.add_argument("--buffer", type=int, default=DEFAULT_BUFFER, help="exchanges kept in memory")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    QUIET = args.quiet
    STORE = Store(max(1, args.buffer), Pricing(PRICING_PATH))
    try:
        proxy = Server(("127.0.0.1", args.port), Tap)
        dashboard = Server(("127.0.0.1", args.ui_port), Dashboard)
    except OSError as e:
        sys.exit(f"cannot listen: {e}")
    threading.Thread(target=dashboard.serve_forever, daemon=True, name="dashboard").start()
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"dashboard on http://localhost:{args.ui_port}")
    print("run the agent with:")
    print(f"ANTHROPIC_BASE_URL=http://localhost:{args.port} claude", flush=True)
    try:
        proxy.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
