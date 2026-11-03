#!/usr/bin/env python3
"""Live agent-traffic dashboard: proxy.py plus an event bus and a browser UI.

    python3 dashboard.py [--port 8041] [--ui-port 8042] [--upstream URL] [--quiet]
    ANTHROPIC_BASE_URL=http://localhost:8041 claude

Standard library only. Forwarding matches proxy.py. Everything the dashboard
does happens on other threads: a proxy thread only copies bytes and drops a
message on an unbounded queue, so neither a slow browser, a blocked terminal,
nor a large snapshot can delay the agent.

    proxy thread --queue--> Recorder thread --Bus--> one bounded queue per browser
                 --queue--> Terminal thread

Choices made where SPEC.md is open (section in brackets):
  [3.2] --quiet silences traffic printing only; the startup lines always print.
        The base-URL line uses the real --port. --buffer N is an added flag.
  [3.2/7] The terminal redacts cookie too. This differs from proxy.py, because
        AC 6 forbids a full token in logs.
  [7]   proxy-authorization and set-cookie are also redacted. A secret of 16
        characters or fewer shows as "..." because its first 16 would be all of it.
  [4]   Added fields: tool_use blocks carry "id"; response_body holds non-SSE
        replies; request_view holds a Codex body mapped to Messages shape;
        reason is the status text; timing.tokens_per_s. Each response_events
        item is {event, data, raw, t_ms}, with parse_error when data is a raw string.
        ids are not reset by /api/clear, so old links never point at a new exchange.
  [4]   Timing runs from when the request reaches the proxy. ttfb stops when
        upstream status and headers arrive; ttft stops at the first non-empty
        text, thinking or tool-input delta (null for non-streamed replies);
        tokens/s is output_tokens over total-ttft, or over total without a ttft.
  [4]   Usage merges message_start and message_delta by overwrite, since delta
        counts are cumulative. A zero or null never overwrites a known value.
  [5]   Payload keys follow the table: exchange_started has id, time, method,
        path, headers, body; exchange_delta has id, event. Extra keys are added
        where the UI needs them. Added events: tools_snapshot (the tool index,
        sent straight after snapshot, since snapshot itself is a list of
        exchanges), tool_updated (new pending call, or no_result), cleared,
        exchange_evicted.
  [5]   An ended exchange emits exactly one of exchange_done or exchange_error.
        HTTP status >= 400 and an SSE error event count as errors. The partial
        reply is kept. After a client drop the upstream read stops, as in proxy.py.
  [5]   /api/export is a plain JSON list of exchanges. /api/exchanges/<id> also
        carries request_raw, the exact body text, used by the Raw tab copy button.
  [6.4] A call is indexed only from a reply, never from resent history, so a
        restart or clear does not refill the Tools view. A result pairs once, on
        first sight. no_result means a later request resends the call without a
        result, or extends the call's own request prefix without the call. A
        late result still flips it to paired.
  [6.4] Result size is characters of text, plus JSON length for non-text blocks.
  [6.5] A model in pricing.json matches exactly, or after removing a trailing
        -YYYYMMDD. An entry needs all four prices. Anything else costs null.
  [8]   Non-JSON or undecodable requests are stored as a string; gzip and deflate
        (and zstd on Python 3.14+) are decompressed for display only.
  [7]   The dashboard requires a localhost Host header, and POST a localhost Origin
        when one is sent, so another web page cannot read or clear the traffic.
  [11]  Open questions: no disk persistence; the separate UI port stays, as drawn in 3.
  Unchanged from proxy.py: only GET, POST and HEAD are relayed (others get 501),
  duplicate request headers collapse, and Server/Date headers are added.
"""

from __future__ import annotations

import argparse
import codecs
import collections
import copy
import http.client
import itertools
import json
import os
import queue
import re
import sys
import threading
import time
import zlib
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))
PRICING_PATH = os.path.join(HERE, "pricing.json")
HTML_PATH = os.path.join(HERE, "dashboard.html")

# Copied from proxy.py: never forwarded.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}
SECRET_HEADERS = {"authorization", "x-api-key", "cookie", "proxy-authorization", "set-cookie"}
TOOL_CALL_TYPES = {"tool_use"}
SUBSCRIBER_QUEUE = 10000
SSE_SEPARATOR = re.compile(rb"\r\n\r\n|\n\n|\r\r")


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def redact(value: str) -> str:
    return value[:16] + "..." if len(value) > 16 else "..."


def headers_dict(items) -> dict:
    out: dict = {}
    for name, value in items:
        if name.lower() in SECRET_HEADERS:
            value = redact(value)
        out[name] = f"{out[name]}, {value}" if name in out else value
    return out


def to_json(value) -> str:
    # ensure_ascii keeps lone surrogates in model output from breaking the encode.
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _reject_constant(name: str):
    raise ValueError(name)  # NaN and Infinity are not valid JSON for the browser


def parse_json(text: str):
    """Return parsed JSON. Raises ValueError for anything a browser couldn't parse."""
    try:
        return json.loads(text, parse_constant=_reject_constant)
    except RecursionError as e:
        raise ValueError("too deep") from e


def decode_request(body: bytes, headers) -> tuple:
    """(request_body as stored, exact text). Display only; the bytes forwarded are untouched."""
    if not body:
        return None, ""
    encoding = ""
    for name, value in headers:
        if name.lower() == "content-encoding":
            encoding = value.strip().lower()
    data = body
    try:
        if encoding in ("gzip", "deflate"):
            data = zlib.decompress(body, 47)
        elif encoding == "zstd":
            from compression import zstd  # Python 3.14+
            data = zstd.decompress(body)
    except Exception:
        note = f"<{len(body)} bytes, content-encoding: {encoding}, not decodable here>"
        return note, note
    text = data.decode("utf-8", "replace")
    try:
        return parse_json(text), text
    except ValueError:
        return text, text


# --- Terminal ---------------------------------------------------------------


class Terminal:
    """Prints proxy.py's output on its own thread, so a blocked stdout can't stall a proxy thread."""

    def __init__(self, quiet: bool):
        self.quiet = quiet
        self.q: queue.Queue = queue.Queue(maxsize=100000)
        if not quiet:
            threading.Thread(target=self._run, daemon=True, name="terminal").start()

    def _run(self) -> None:
        while True:
            fn, args = self.q.get()
            try:
                sys.stdout.write(fn(*args))
                sys.stdout.flush()
            except Exception:
                pass

    def emit(self, fn, *args) -> None:
        if self.quiet:
            return
        try:
            self.q.put_nowait((fn, args))
        except queue.Full:
            pass

    def text(self, text: str) -> None:
        self.emit(str, text)

    def request(self, when: str, method: str, path: str, headers, body: bytes) -> None:
        self.emit(self._format_request, when, method, path, headers, body)

    @staticmethod
    def _format_request(when, method, path, headers, body) -> str:
        lines = [f"\n{'=' * 78}\n[{when}] >>> {method} {path}"]
        for name, value in headers:
            if name.lower() in SECRET_HEADERS:
                value = redact(value)
            lines.append(f"    {name}: {value}")
        if body:
            try:
                lines.append(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
            except (ValueError, RecursionError):
                lines.append(body.decode("utf-8", "replace"))
        return "\n".join(lines) + "\n"


# --- Event bus --------------------------------------------------------------


class Bus:
    """Thread-safe fan-out. publish never blocks: a full subscriber queue loses the event."""

    def __init__(self):
        self.lock = threading.Lock()
        self.subscribers: set = set()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE)
        with self.lock:
            self.subscribers.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            self.subscribers.discard(q)

    def publish(self, name: str, payload) -> None:
        with self.lock:
            targets = list(self.subscribers)
        if not targets:
            return
        frame = sse_frame(name, payload)
        for q in targets:
            try:
                q.put_nowait(frame)
            except queue.Full:
                pass


def sse_frame(name: str, payload) -> bytes:
    return f"event: {name}\ndata: {to_json(payload)}\n\n".encode("ascii")


# --- Pricing ----------------------------------------------------------------


class Pricing:
    """pricing.json, re-read when its mtime changes. A broken edit keeps the last good table."""

    def __init__(self, path: str):
        self.path = path
        self.mtime = None
        self.table: dict = {}

    def _refresh(self) -> None:
        try:
            mtime = os.stat(self.path).st_mtime
            if mtime != self.mtime:
                self.mtime = mtime
                with open(self.path, encoding="utf-8") as f:
                    table = json.load(f)
                if isinstance(table, dict):
                    self.table = table
        except (OSError, ValueError):
            pass

    def cost(self, model, usage: dict):
        self._refresh()
        if not model or not usage:
            return None
        entry = self.table.get(model) or self.table.get(re.sub(r"-\d{8}$", "", model))
        if not isinstance(entry, dict):
            return None
        total = 0.0
        for price_key, usage_key in (
            ("input", "input_tokens"),
            ("output", "output_tokens"),
            ("cache_read", "cache_read_input_tokens"),
            ("cache_write", "cache_creation_input_tokens"),
        ):
            price = entry.get(price_key)
            if isinstance(price, bool) or not isinstance(price, (int, float)):
                return None
            total += (usage.get(usage_key) or 0) * price
        return round(total / 1_000_000, 8)


# --- Request shape ----------------------------------------------------------


def blocks_of(message) -> list:
    if not isinstance(message, dict):
        return []
    content = message.get("content")
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if isinstance(content, list):
        return [b for b in content if isinstance(b, dict)]
    return []


def is_codex_body(body) -> bool:
    return isinstance(body, dict) and "messages" not in body and isinstance(body.get("input"), (list, str))


def codex_view(body: dict) -> dict:
    """Maps a Responses-API body to the shape of a Messages body, for pairing and display."""
    system = []
    if isinstance(body.get("instructions"), str):
        system.append({"type": "text", "text": body["instructions"]})
    tools = []
    for tool in body.get("tools") or []:
        if isinstance(tool, dict):
            tools.append({
                "name": tool.get("name") or tool.get("type") or "?",
                "description": tool.get("description") or "",
                "input_schema": tool.get("parameters") or tool,
            })
    messages: list = []

    def add(role: str, block: dict, item, join: bool) -> None:
        if join and messages and messages[-1]["role"] == role and messages[-1].get("_joinable"):
            messages[-1]["content"].append(block)
            messages[-1]["raw_items"].append(item)
        else:
            messages.append({"role": role, "content": [block], "raw_items": [item], "_joinable": True})

    items = body.get("input")
    if isinstance(items, str):
        items = [{"type": "message", "role": "user", "content": items}]
    for item in items or []:
        if not isinstance(item, dict):
            continue
        kind = item.get("type", "message")
        if kind == "message":
            role = item.get("role", "user")
            blocks = []
            content = item.get("content")
            for part in [content] if isinstance(content, str) else (content or []):
                if isinstance(part, str):
                    blocks.append({"type": "text", "text": part})
                elif isinstance(part, dict) and part.get("type") in ("input_text", "output_text", "text"):
                    blocks.append({"type": "text", "text": part.get("text", "")})
                elif isinstance(part, dict) and part.get("type") == "input_image":
                    blocks.append({"type": "image", "source": {"type": "url", "url": part.get("image_url")}})
                elif isinstance(part, dict):
                    blocks.append(part)
            messages.append({"role": role, "content": blocks, "raw_items": [item], "_joinable": role == "assistant"})
        elif kind == "reasoning":
            summary = "\n".join(s.get("text", "") for s in item.get("summary") or [] if isinstance(s, dict))
            add("assistant", {"type": "thinking", "thinking": summary}, item, True)
        elif kind in ("function_call", "custom_tool_call"):
            raw = item.get("arguments", item.get("input", ""))
            try:
                value = parse_json(raw) if isinstance(raw, str) else raw
            except ValueError:
                value = raw
            add("assistant", {"type": "tool_use", "id": item.get("call_id"), "name": item.get("name"), "input": value}, item, True)
        elif kind in ("function_call_output", "custom_tool_call_output"):
            output = item.get("output")
            if isinstance(output, dict) and "content" in output:
                output = output["content"]
            add("user", {"type": "tool_result", "tool_use_id": item.get("call_id"), "content": output}, item, True)
        else:
            add("assistant", dict(item), item, True)
    for message in messages:
        message.pop("_joinable", None)
    return {"system": system, "tools": tools, "messages": messages}


def view_of(body) -> dict:
    if is_codex_body(body):
        return codex_view(body)
    if not isinstance(body, dict):
        return {"system": [], "tools": [], "messages": []}
    system = body.get("system")
    if isinstance(system, str):
        system = [{"type": "text", "text": system}]
    messages = body.get("messages")
    return {
        "system": system if isinstance(system, list) else [],
        "tools": body.get("tools") if isinstance(body.get("tools"), list) else [],
        "messages": messages if isinstance(messages, list) else [],
    }


def strip_cache_control(value):
    """Harnesses move cache_control between turns, so it must not break conversation matching."""
    if isinstance(value, dict):
        return {k: strip_cache_control(v) for k, v in value.items() if k != "cache_control"}
    if isinstance(value, list):
        return [strip_cache_control(v) for v in value]
    return value


def result_size(content) -> int:
    if content is None:
        return 0
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        total = 0
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                total += len(part.get("text") or "")
            else:
                total += len(to_json(part))
        return total
    return len(to_json(content))


# --- Stream parsing ---------------------------------------------------------


class SSEParser:
    """Splits a byte stream into SSE events. Handles events and UTF-8 split across chunks."""

    def __init__(self):
        self.buf = bytearray()

    def feed(self, chunk: bytes) -> list:
        self.buf.extend(chunk)
        out = []
        while True:
            match = SSE_SEPARATOR.search(self.buf)
            if not match:
                return out
            out.append(self.parse(bytes(self.buf[: match.end()])))
            del self.buf[: match.end()]

    def flush(self) -> list:
        rest = bytes(self.buf)
        self.buf.clear()
        return [self.parse(rest)] if rest.strip() else []

    @staticmethod
    def parse(raw: bytes) -> dict:
        text = raw.decode("utf-8", "replace")
        name = None
        data_lines = []
        for line in re.split(r"\r\n|\n|\r", text):
            if line.startswith(":") or not line:
                continue
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "event":
                name = value
            elif field == "data":
                data_lines.append(value)
        data = "\n".join(data_lines)
        event = {"event": name, "data": data, "raw": text}
        if not data_lines:
            return event  # a comment or a bare event name: nothing to parse
        try:
            event["data"] = parse_json(data)
        except ValueError:
            if data.strip() != "[DONE]":
                event["parse_error"] = True
        return event


class Assembler:
    """Builds exchange["assembled"] from stream events and describes each change as patches.

    The browser applies the patches, so assembly logic lives only here. Patch
    index "i" is the block's position in content, i.e. the order the model produced.
    """

    def __init__(self, assembled: dict, on_tool):
        self.a = assembled
        self.on_tool = on_tool
        self.pos: dict = {}
        self.open: set = set()
        self.complete = False
        self.saw_message_start = False
        self.error = None
        self.got_token = False

    def _start(self, key, block: dict, patches: list) -> dict:
        i = len(self.a["content"])
        self.pos[key] = i
        self.a["content"].append(block)
        self.open.add(i)
        patches.append({"op": "start", "i": i, "block": copy.deepcopy(block)})
        return block

    def _append(self, key, field: str, text: str, patches: list) -> None:
        i = self.pos.get(key)
        if i is None or not isinstance(text, str):
            return
        block = self.a["content"][i]
        block[field] = (block.get(field) or "") + text
        patches.append({"op": "append", "i": i, "field": field, "text": text})
        if text and field in ("text", "thinking", "partial_json"):
            self.got_token = True

    def _stop(self, key, patches: list, incomplete: bool = False) -> None:
        i = self.pos.get(key)
        if i is None or i not in self.open:
            return
        self.open.discard(i)
        block = self.a["content"][i]
        if "partial_json" in block:
            partial = block.pop("partial_json")
            if partial.strip():
                try:
                    block["input"] = parse_json(partial)
                except ValueError:
                    block["input_raw"] = partial
                    block["input_error"] = "input JSON did not parse"
        if incomplete:
            block["incomplete"] = True
        patches.append({"op": "stop", "i": i, "block": copy.deepcopy(block)})
        if block.get("type") in TOOL_CALL_TYPES and block.get("id"):
            self.on_tool(block)

    def _meta(self, patches: list, **fields) -> None:
        patch = {"op": "meta"}
        for key, value in fields.items():
            if value is not None:
                self.a[key] = value
                patch[key] = value
        patch["usage"] = dict(self.a["usage"])
        patches.append(patch)

    def _merge_usage(self, usage) -> None:
        if not isinstance(usage, dict):
            return
        for key, value in usage.items():
            if value is None or (value == 0 and self.a["usage"].get(key)):
                continue
            self.a["usage"][key] = value

    def feed(self, name, data) -> list:
        """Returns patches. Never relied on by forwarding; the caller guards exceptions."""
        if not isinstance(data, dict):
            return []
        kind = data.get("type") or name
        if isinstance(kind, str) and kind.startswith("response."):
            return self._feed_codex(kind, data)
        patches: list = []
        if kind == "message_start":
            message = data.get("message") or {}
            self.saw_message_start = True
            self._merge_usage(message.get("usage"))
            self._meta(patches, model=message.get("model"), id=message.get("id"))
        elif kind == "content_block_start":
            block = dict(data.get("content_block") or {})
            if block.get("type") in ("tool_use", "server_tool_use", "mcp_tool_use"):
                block.setdefault("input", {})
                block["partial_json"] = ""
            self._start(data.get("index"), block, patches)
        elif kind == "content_block_delta":
            key = data.get("index")
            delta = data.get("delta") or {}
            dtype = delta.get("type")
            if dtype == "text_delta":
                self._append(key, "text", delta.get("text"), patches)
            elif dtype == "thinking_delta":
                self._append(key, "thinking", delta.get("thinking"), patches)
            elif dtype == "signature_delta":
                self._append(key, "signature", delta.get("signature"), patches)
            elif dtype == "input_json_delta":
                self._append(key, "partial_json", delta.get("partial_json"), patches)
            elif dtype == "citations_delta" and self.pos.get(key) is not None:
                self.a["content"][self.pos[key]].setdefault("citations", []).append(delta.get("citation"))
        elif kind == "content_block_stop":
            self._stop(data.get("index"), patches)
        elif kind == "message_delta":
            delta = data.get("delta") or {}
            self._merge_usage(data.get("usage"))
            self._meta(patches, stop_reason=delta.get("stop_reason"), stop_sequence=delta.get("stop_sequence"))
        elif kind == "message_stop":
            self.complete = True
        elif kind == "error":
            error = data.get("error")
            message = error.get("message") if isinstance(error, dict) else data.get("message")
            self.error = str(message or error or "upstream stream error")
        return patches

    def _feed_codex(self, kind: str, data: dict) -> list:
        patches: list = []
        index = data.get("output_index")
        if kind in ("response.created", "response.in_progress"):
            response = data.get("response") or {}
            self._meta(patches, model=response.get("model"), id=response.get("id"))
        elif kind == "response.output_item.added":
            item = data.get("item") or {}
            itype = item.get("type")
            if itype in ("function_call", "custom_tool_call"):
                self._start(("item", index), {
                    "type": "tool_use", "id": item.get("call_id"), "name": item.get("name"),
                    "input": {}, "partial_json": "",
                }, patches)
            elif itype == "reasoning":
                self._start(("item", index), {"type": "thinking", "thinking": ""}, patches)
            elif itype != "message":
                self._start(("item", index), dict(item), patches)
        elif kind == "response.output_text.delta":
            key = ("text", index, data.get("content_index"))
            if key not in self.pos:
                self._start(key, {"type": "text", "text": ""}, patches)
            self._append(key, "text", data.get("delta"), patches)
        elif kind in ("response.reasoning_summary_text.delta", "response.reasoning_text.delta"):
            self._append(("item", index), "thinking", data.get("delta"), patches)
        elif kind in ("response.function_call_arguments.delta", "response.custom_tool_call_input.delta"):
            self._append(("item", index), "partial_json", data.get("delta"), patches)
        elif kind == "response.output_item.done":
            item = data.get("item") or {}
            itype = item.get("type")
            key = ("item", index)
            if itype in ("function_call", "custom_tool_call") and key in self.pos:
                final = item.get("arguments", item.get("input"))
                if isinstance(final, str):
                    self.a["content"][self.pos[key]]["partial_json"] = final
            elif itype == "message":
                for ci, part in enumerate(item.get("content") or []):
                    text_key = ("text", index, ci)
                    if text_key not in self.pos and isinstance(part, dict) and part.get("text"):
                        self._start(text_key, {"type": "text", "text": part["text"]}, patches)
                    self._stop(text_key, patches)
            self._stop(key, patches)
        elif kind in ("response.completed", "response.incomplete", "response.failed"):
            response = data.get("response") or {}
            usage = response.get("usage") or {}
            cached = (usage.get("input_tokens_details") or {}).get("cached_tokens") or 0
            if usage:
                # OpenAI's input_tokens includes cached tokens; Anthropic's does not.
                self._merge_usage({
                    "input_tokens": max(0, (usage.get("input_tokens") or 0) - cached),
                    "output_tokens": usage.get("output_tokens"),
                    "cache_read_input_tokens": cached,
                    "cache_creation_input_tokens": 0,
                })
            has_tool = any(b.get("type") == "tool_use" for b in self.a["content"])
            reason = (response.get("incomplete_details") or {}).get("reason")
            stop = reason or ("tool_use" if has_tool else response.get("status"))
            self._meta(patches, model=response.get("model"), stop_reason=stop)
            self.complete = True
            if kind == "response.failed":
                error = response.get("error") or {}
                self.error = str(error.get("message") or error or "response failed")
        return patches

    def finish(self, patches: list) -> None:
        """Closes blocks a cut-off stream left open, so partial tool input is still visible."""
        for key, i in list(self.pos.items()):
            if i in self.open:
                self._stop(key, patches, incomplete=True)


# --- Exchange store ---------------------------------------------------------


def public(exchange: dict) -> dict:
    return {k: v for k, v in exchange.items() if not k.startswith("_")}


class Recorder:
    """Owns all dashboard state. All mutations run on one thread, holding self.lock."""

    def __init__(self, capacity: int, bus: Bus, pricing: Pricing):
        self.capacity = capacity
        self.bus = bus
        self.pricing = pricing
        self.lock = threading.Lock()
        self.q: queue.Queue = queue.Queue()
        self.ids = itertools.count(1)
        self.by_id: dict = {}  # insertion order is id order
        self.tools: dict = {}
        self.pending: set = set()
        self.orphans: collections.OrderedDict = collections.OrderedDict()
        self.active: dict = {}
        threading.Thread(target=self._run, daemon=True, name="recorder").start()

    def post(self, fn, *args) -> None:
        self.q.put((fn, args))

    def _run(self) -> None:
        while True:
            fn, args = self.q.get()
            try:
                with self.lock:
                    fn(*args)
            except Exception as e:  # recording must never stop, whatever arrives
                print(f"[{stamp()}] dashboard: {fn.__name__} failed: {e!r}", file=sys.stderr, flush=True)

    # Reads, from dashboard threads.

    def connect(self) -> tuple:
        """Atomic snapshot plus subscription, so a client neither misses nor double-counts."""
        with self.lock:
            frames = [
                sse_frame("snapshot", [public(e) for e in self.by_id.values()]),
                sse_frame("tools_snapshot", self.tools),
            ]
            return frames, self.bus.subscribe()

    def get(self, exchange_id: int):
        with self.lock:
            exchange = self.by_id.get(exchange_id)
            if exchange is None:
                return None
            out = public(exchange)
            out["request_raw"] = exchange["_request_raw"]
            return to_json(out)

    def export(self) -> str:
        with self.lock:
            return to_json([public(e) for e in self.by_id.values()])

    def clear(self) -> None:
        with self.lock:
            self.by_id.clear()
            self.tools.clear()
            self.pending.clear()
            self.orphans.clear()
            self.bus.publish("cleared", {})

    # Writes, posted by proxy threads with their own timestamps.

    def on_started(self, xid, wall, t0, method, path, headers, body) -> None:
        stored, raw = decode_request(body, headers)
        view = view_of(stored)
        exchange = {
            "id": xid,
            "started_at": wall,
            "ended_at": None,
            "method": method,
            "path": path,
            "status": None,
            "reason": None,
            "request_headers": headers_dict(headers),
            "request_body": stored,
            "request_view": {k: view[k] for k in ("system", "tools", "messages")} if is_codex_body(stored) else None,
            "response_headers": {},
            "response_events": [],
            "response_body": None,
            "assembled": {"model": None, "content": [], "stop_reason": None, "usage": {}},
            "timing": {"ttfb_ms": None, "ttft_ms": None, "total_ms": None, "tokens_per_s": None},
            "cost_usd": None,
            "error": None,
            "_request_raw": raw,
            "_t0": t0,
            "_end": None,
            "_view": view,
        }
        self.by_id[xid] = exchange
        self.active[xid] = {
            "ex": exchange,
            "sse": SSEParser(),
            "asm": Assembler(exchange["assembled"], lambda block, x=xid: self._register_tool(x, block)),
            "is_sse": False,
            "body": bytearray(),
        }
        self.bus.publish("exchange_started", {
            "id": xid, "time": wall, "method": method, "path": path,
            "headers": exchange["request_headers"], "body": stored,
            "request_view": exchange["request_view"],
        })
        self._index_request(exchange, view)
        while len(self.by_id) > self.capacity:
            self._evict(next(iter(self.by_id)))

    def on_response(self, xid, t, status, reason, headers) -> None:
        state = self.active[xid]
        exchange = state["ex"]
        exchange["status"] = status
        exchange["reason"] = reason
        exchange["response_headers"] = headers_dict(headers)
        exchange["timing"]["ttfb_ms"] = int((t - exchange["_t0"]) * 1000)
        for name, value in headers:
            if name.lower() == "content-type" and "event-stream" in value.lower():
                state["is_sse"] = True
        self.bus.publish("exchange_response", {
            "id": xid, "status": status, "reason": reason, "headers": exchange["response_headers"],
            "ttfb_ms": exchange["timing"]["ttfb_ms"],
        })

    def on_chunk(self, xid, t, chunk) -> None:
        state = self.active[xid]
        if not state["is_sse"]:
            state["body"].extend(chunk)
            return
        for event in state["sse"].feed(chunk):
            self._handle_event(state, event, t)

    def _handle_event(self, state, event, t) -> None:
        exchange = state["ex"]
        event["t_ms"] = int((t - exchange["_t0"]) * 1000)
        exchange["response_events"].append(event)
        patches = []
        asm = state["asm"]
        if not event.get("parse_error"):
            try:
                patches = asm.feed(event["event"], event["data"])
            except Exception as e:  # malformed shape: keep the raw event and carry on
                event["parse_error"] = True
                event["assembler_error"] = repr(e)
        payload = {"id": exchange["id"], "event": event, "patches": patches}
        if asm.got_token and exchange["timing"]["ttft_ms"] is None:
            exchange["timing"]["ttft_ms"] = event["t_ms"]
            payload["ttft_ms"] = event["t_ms"]
        self.bus.publish("exchange_delta", payload)

    def on_finish(self, xid, t, wall, error) -> None:
        state = self.active.pop(xid)
        exchange = state["ex"]
        asm = state["asm"]
        for event in state["sse"].flush():
            self._handle_event(state, event, t)
        patches: list = []
        asm.finish(patches)
        if state["body"]:
            self._finish_plain_body(exchange, asm, bytes(state["body"]))
        if not error:
            error = asm.error or exchange["error"]
        if not error and asm.saw_message_start and not asm.complete:
            error = "stream ended before message_stop"
        if not error and (exchange["status"] or 0) >= 400:
            error = f"HTTP {exchange['status']}"

        timing = exchange["timing"]
        timing["total_ms"] = int((t - exchange["_t0"]) * 1000)
        output = exchange["assembled"]["usage"].get("output_tokens")
        ttft = timing["ttft_ms"]
        window = timing["total_ms"] - ttft if ttft else timing["total_ms"]
        if window <= 0:  # a burst inside one millisecond has no measurable rate of its own
            window = timing["total_ms"]
        if isinstance(output, (int, float)) and output and window > 0:
            timing["tokens_per_s"] = round(output / (window / 1000), 1)
        exchange["ended_at"] = wall
        exchange["error"] = error
        exchange["_end"] = t
        body = exchange["request_body"]
        model = exchange["assembled"]["model"] or (body.get("model") if isinstance(body, dict) else None)
        exchange["cost_usd"] = self.pricing.cost(model, exchange["assembled"]["usage"])
        self._settle_wait_times(xid)

        common = {
            "id": xid, "ended_at": wall, "assembled": exchange["assembled"], "timing": timing,
            "cost_usd": exchange["cost_usd"], "response_body": exchange["response_body"],
            "status": exchange["status"],
        }
        if error:
            self.bus.publish("exchange_error", {**common, "message": error})
        else:
            self.bus.publish("exchange_done", common)

    def _finish_plain_body(self, exchange, asm, data: bytes) -> None:
        text = data.decode("utf-8", "replace")
        exchange["response_body"] = text
        try:
            parsed = parse_json(text)
        except ValueError:
            return
        if not isinstance(parsed, dict):
            return
        if parsed.get("type") == "message":  # not count_tokens, whose usage-like field isn't a reply
            assembled = exchange["assembled"]
            assembled["model"] = parsed.get("model")
            assembled["stop_reason"] = parsed.get("stop_reason")
            assembled["id"] = parsed.get("id")
            assembled["content"] = [b for b in parsed.get("content") or [] if isinstance(b, dict)]
            asm._merge_usage(parsed.get("usage"))
            for block in assembled["content"]:
                if block.get("type") in TOOL_CALL_TYPES and block.get("id"):
                    self._register_tool(exchange["id"], block)
        error = parsed.get("error")
        if error:
            message = error.get("message") if isinstance(error, dict) else error
            exchange["error"] = f"HTTP {exchange['status']}: {message}"

    # Tool pairing.

    def _register_tool(self, xid, block) -> None:
        tool_id = block["id"]
        if xid not in self.by_id or tool_id in self.tools:
            return
        entry = {
            "tool_use_id": tool_id,
            "name": block.get("name"),
            "input": block.get("input"),
            "input_raw": block.get("input_raw"),
            "call_exchange": xid,
            "result_exchange": None,
            "result": None,
            "is_error": False,
            "result_size": None,
            "wait_ms": None,
            "status": "pending",
        }
        self.tools[tool_id] = entry
        self.pending.add(tool_id)
        self.bus.publish("tool_updated", entry)
        # The next request can overtake this block on the recorder queue.
        orphan = self.orphans.pop(tool_id, None)
        if orphan:
            self._pair(tool_id, orphan[0], orphan[1])

    def _index_request(self, exchange, view) -> None:
        messages = [m for m in view["messages"] if isinstance(m, dict)]
        last = len(messages) - 1
        uses, results = set(), {}
        for pos, message in enumerate(messages):
            for block in blocks_of(message):
                kind = block.get("type")
                if kind == "tool_result" and block.get("tool_use_id") is not None:
                    results.setdefault(block["tool_use_id"], (block, pos))
                elif kind in TOOL_CALL_TYPES and pos < last:
                    uses.add(block.get("id"))
        for tool_id, (block, pos) in results.items():
            entry = self.tools.get(tool_id)
            if entry is None:
                if pos == last:
                    self.orphans[tool_id] = (exchange["id"], block)
                    while len(self.orphans) > 1000:
                        self.orphans.popitem(last=False)
            elif entry["status"] != "paired":
                self._pair(tool_id, exchange["id"], block)
        for tool_id in list(self.pending):
            entry = self.tools[tool_id]
            if tool_id in results or entry["call_exchange"] >= exchange["id"]:
                continue
            if tool_id in uses or self._extends_call_request(entry, view, messages):
                entry["status"] = "no_result"
                self.pending.discard(tool_id)
                self.bus.publish("tool_updated", entry)

    def _extends_call_request(self, entry, view, messages) -> bool:
        call = self.by_id.get(entry["call_exchange"])
        if call is None:
            return False
        older = call["_view"]
        count = len(older["messages"])
        if not count or len(messages) <= count:
            return False
        return (
            strip_cache_control(older["system"]) == strip_cache_control(view["system"])
            and strip_cache_control(older["messages"]) == strip_cache_control(messages[:count])
        )

    def _pair(self, tool_id, result_xid, block) -> None:
        entry = self.tools[tool_id]
        content = block.get("content")
        entry.update(
            result_exchange=result_xid,
            result=content,
            is_error=bool(block.get("is_error")),
            result_size=result_size(content),
            status="paired",
        )
        self.pending.discard(tool_id)
        entry["wait_ms"] = self._wait_ms(entry)
        self.bus.publish("tool_paired", dict(entry))

    def _wait_ms(self, entry):
        """End of the reply to start of the request carrying the result (SPEC 6.5)."""
        call = self.by_id.get(entry["call_exchange"])
        result = self.by_id.get(entry["result_exchange"])
        if not call or not result or call["_end"] is None:
            return None
        return max(0, int((result["_t0"] - call["_end"]) * 1000))

    def _settle_wait_times(self, xid) -> None:
        for entry in self.tools.values():
            if entry["call_exchange"] == xid and entry["status"] == "paired" and entry["wait_ms"] is None:
                entry["wait_ms"] = self._wait_ms(entry)
                self.bus.publish("tool_paired", dict(entry))

    def _evict(self, xid) -> None:
        del self.by_id[xid]
        for tool_id in [t for t, e in self.tools.items() if e["call_exchange"] == xid]:
            del self.tools[tool_id]
            self.pending.discard(tool_id)
        self.bus.publish("exchange_evicted", {"id": xid})


# --- Proxy ------------------------------------------------------------------


class Tap(BaseHTTPRequestHandler):
    """proxy.py's relay, with a tap on it. The tap only queues."""

    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    upstream_url = "https://api.anthropic.com"
    recorder: Recorder = None  # type: ignore[assignment]
    terminal: Terminal = None  # type: ignore[assignment]

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def do_POST(self) -> None:
        self.relay()

    def do_GET(self) -> None:
        self.relay()

    def do_HEAD(self) -> None:
        self.relay()

    def _fail(self, xid, label: str, error) -> None:
        upstream = self.upstream_url
        self.terminal.text(f"\n[{stamp()}] !!! {label} {upstream}: {error}\n")
        self.recorder.post(self.recorder.on_finish, xid, time.monotonic(), now_iso(), f"upstream unreachable: {error}")
        try:
            self.send_error(502, f"proxy: {label} {upstream}: {error}")
        except OSError:
            pass
        self.close_connection = True

    def relay(self) -> None:
        t0 = time.monotonic()
        wall = now_iso()
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        recorder = self.recorder
        terminal = self.terminal
        header_items = list(self.headers.items())

        xid = next(recorder.ids)
        recorder.post(recorder.on_started, xid, wall, t0, self.command, self.path, header_items, body)
        terminal.request(stamp(), self.command, self.path, header_items, body)

        target = urlsplit(self.upstream_url)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
        except OSError as e:
            self._fail(xid, "cannot reach", e)
            return
        try:
            upstream.sock.settimeout(600)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()
        except (OSError, http.client.HTTPException) as e:
            upstream.close()
            self._fail(xid, "failed talking to", e)
            return

        reply_headers = reply.getheaders()
        recorder.post(recorder.on_response, xid, time.monotonic(), reply.status, reply.reason, reply_headers)
        terminal.text(f"\n[{stamp()}] <<< {reply.status} {reply.reason}\n")

        error = None
        try:
            self.send_response(reply.status, reply.reason)
            for name, value in reply_headers:
                if name.lower() not in SKIP and name.lower() != "content-encoding":
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                error = self._stream(xid, reply)
        except OSError as e:
            error = f"client disconnected: {e}"
        finally:
            upstream.close()
        self.close_connection = True
        if error or self.command != "HEAD":  # proxy.py prints no "done" line for HEAD
            terminal.text(f"\n[{stamp()}] --- {error or 'done'}\n")
        recorder.post(recorder.on_finish, xid, time.monotonic(), now_iso(), error)

    def _stream(self, xid, reply):
        """Copies the reply. Bytes reach the client before anything else sees them."""
        recorder = self.recorder
        terminal = self.terminal
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        while True:
            try:
                chunk = reply.read1(4096)  # read1: pass on each chunk as it lands
            except (OSError, http.client.HTTPException) as e:
                return f"upstream stream failed: {e!r}"
            if not chunk:
                if reply.length:  # a declared Content-Length was not met
                    return "upstream closed before the full reply arrived"
                return None
            t = time.monotonic()
            try:
                self.wfile.write(chunk)
                self.wfile.flush()
            except OSError as e:
                recorder.post(recorder.on_chunk, xid, t, chunk)
                return f"client disconnected: {e}"
            recorder.post(recorder.on_chunk, xid, t, chunk)
            terminal.text(decoder.decode(chunk))


# --- Dashboard server -------------------------------------------------------


class Dashboard(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    recorder: Recorder = None  # type: ignore[assignment]

    CSP = (
        "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
        "connect-src 'self'; base-uri 'none'; form-action 'none'"
    )

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def _host_ok(self) -> bool:
        host = urlsplit("//" + (self.headers.get("host") or "")).hostname
        return host in ("localhost", "127.0.0.1", "::1")

    def _send(self, status: int, body: bytes, content_type: str, extra=()) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        for name, value in extra:
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, status: int, text: str, extra=()) -> None:
        self._send(status, text.encode("ascii"), "application/json", extra)

    def do_GET(self) -> None:
        if not self._host_ok():
            self._json(403, '{"error":"forbidden host"}')
            return
        path = urlsplit(self.path).path
        if path == "/":
            try:
                with open(HTML_PATH, "rb") as f:
                    page = f.read()
            except OSError as e:
                self._json(500, to_json({"error": str(e)}))
                return
            self._send(200, page, "text/html; charset=utf-8", [("Content-Security-Policy", self.CSP)])
        elif path == "/events":
            self._events()
        elif path == "/api/export":
            name = "traffic-" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".json"
            self._json(200, self.recorder.export(), [("Content-Disposition", f'attachment; filename="{name}"')])
        elif re.fullmatch(r"/api/exchanges/\d+", path):
            text = self.recorder.get(int(path.rsplit("/", 1)[1]))
            if text is None:
                self._json(404, '{"error":"not in buffer"}')
            else:
                self._json(200, text)
        elif path == "/favicon.ico":
            self._send(204, b"", "image/x-icon")
        else:
            self._json(404, '{"error":"not found"}')

    do_HEAD = do_GET

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        if length:
            self.rfile.read(length)
        origin = self.headers.get("origin")
        origin_host = urlsplit(origin).hostname if origin else None
        if not self._host_ok() or (origin and origin_host not in ("localhost", "127.0.0.1", "::1")):
            self._json(403, '{"error":"forbidden"}')
            return
        if urlsplit(self.path).path == "/api/clear":
            self.recorder.clear()
            self._json(200, '{"ok":true}')
        else:
            self._json(404, '{"error":"not found"}')

    def _events(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command == "HEAD":
            return
        self.connection.settimeout(60)  # a stuck browser ends here, not in the recorder
        frames, q = self.recorder.connect()
        try:
            for frame in frames:
                self.wfile.write(frame)
            while True:
                try:
                    frame = q.get(timeout=15)
                except queue.Empty:
                    frame = b": ping\n\n"
                self.wfile.write(frame)
        except OSError:
            pass
        finally:
            self.recorder.bus.unsubscribe(q)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward an agent's API traffic and show it live in a browser.")
    parser.add_argument("--port", type=int, default=8041, help="proxy port")
    parser.add_argument("--ui-port", type=int, default=8042, help="dashboard port")
    parser.add_argument("--upstream", default=Tap.upstream_url, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--quiet", action="store_true", help="turn off terminal printing of traffic")
    parser.add_argument("--buffer", type=int, default=200, help="exchanges kept in memory (default: 200)")
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")

    recorder = Recorder(max(1, args.buffer), Bus(), Pricing(PRICING_PATH))
    Tap.upstream_url = args.upstream.rstrip("/")
    Tap.recorder = recorder
    Tap.terminal = Terminal(args.quiet)
    Dashboard.recorder = recorder
    try:
        proxy_server = Server(("127.0.0.1", args.port), Tap)
        ui_server = Server(("127.0.0.1", args.ui_port), Dashboard)
    except OSError as e:
        sys.exit(f"cannot bind port: {e}")
    threading.Thread(target=ui_server.serve_forever, daemon=True, name="dashboard").start()

    print(f"proxy listening on http://localhost:{args.port}  ->  {Tap.upstream_url}", flush=True)
    print(f"dashboard on http://localhost:{args.ui_port}", flush=True)
    print(f"ANTHROPIC_BASE_URL=http://localhost:{args.port} claude", flush=True)
    try:
        proxy_server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
