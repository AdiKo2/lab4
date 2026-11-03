#!/usr/bin/env python3
"""Live agent-traffic dashboard: proxy.py plus a browser view of every exchange.

    python3 dashboard.py [--port 8041] [--ui-port 8042] [--upstream URL] [--quiet] [--buffer 200]
    ANTHROPIC_BASE_URL=http://localhost:8041 claude

How it fits together. Each proxy thread forwards bytes exactly as proxy.py does
and only ever drops raw facts onto an unbounded in-memory queue, which cannot
block. One Recorder thread owns all state: it parses bodies and SSE, assembles
replies, pairs tools, computes cost and publishes frames to each browser's
bounded queue. A slow dashboard or a parse failure therefore cannot reach the
forwarding path.

Fields and events beyond SPEC §4/§5, all additive:
  exchange.response_body   the reply when it is not SSE (JSON or text)
  exchange.view            Codex requests rewritten into Messages shape
  exchange.conversation    {prev_exchange, new_from}, the basis of the diff view
  exchange_delta.patch     how to apply the event to `assembled`
  tool_index, tool_update  whole tool index, and any change to one entry
  exchange_evicted, cleared  keep browsers in step with the ring buffer
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import http.client
import itertools
import json
import os
import queue
import re
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

# Same hop-by-hop list as proxy.py.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}
# §7 names the first three. The others carry credentials too, and AC6 says no
# full token may appear anywhere.
SECRET_HEADERS = {
    "authorization", "x-api-key", "cookie",
    "set-cookie", "proxy-authorization", "api-key", "x-auth-token", "x-goog-api-key",
}

UPSTREAM = "https://api.anthropic.com"
QUIET = False
HERE = os.path.dirname(os.path.abspath(__file__))
SUBSCRIBER_QUEUE = 2000
KEEPALIVE_SECONDS = 15


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def iso(ts: float) -> str:
    moment = datetime.fromtimestamp(ts, timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def redact(value: str) -> str:
    """First 16 characters then "...", but never more than half of a short value."""
    return value[: min(16, len(value) // 2)] + "..."


def redact_headers(items) -> dict:
    out: dict = {}
    for name, value in items:
        if name.lower() in SECRET_HEADERS:
            value = redact(value)
        out[name] = f"{out[name]}, {value}" if name in out else value
    return out


def say(*args, **kwargs) -> None:
    """Terminal output. A closed or non-UTF-8 terminal must never stop forwarding."""
    if QUIET:
        return
    try:
        print(*args, **kwargs)
    except UnicodeError:
        text = " ".join(str(a) for a in args)
        print(text.encode("ascii", "backslashreplace").decode(), **kwargs)
    except (OSError, ValueError):
        pass


def say_chunk(chunk: bytes) -> None:
    if QUIET:
        return
    try:
        sys.stdout.write(chunk.decode("utf-8", "replace"))
        sys.stdout.flush()
    except (UnicodeError, OSError, ValueError):
        pass


# --------------------------------------------------------------------------- SSE parsing

class SSEParser:
    """Incremental, works on bytes so a multi-byte character can span two chunks."""

    BOUNDARY = re.compile(rb"\r\n\r\n|\n\n|\r\r")

    def __init__(self) -> None:
        self.buf = b""

    def feed(self, chunk: bytes) -> list:
        self.buf += chunk
        events = []
        while match := self.BOUNDARY.search(self.buf):
            raw, self.buf = self.buf[: match.start()], self.buf[match.end():]
            if raw.strip():
                events.append(parse_sse_event(raw))
        return events

    def flush(self) -> list:
        raw, self.buf = self.buf, b""
        return [parse_sse_event(raw)] if raw.strip() else []


def parse_sse_event(raw: bytes) -> dict:
    """A failed parse keeps the raw string (§8) and never raises."""
    text = raw.decode("utf-8", "replace")
    name, data_lines = None, []
    for line in text.splitlines():
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            value = line[5:]
            data_lines.append(value[1:] if value.startswith(" ") else value)
    event = {"event": name, "data": None, "raw": text}
    if data_lines:
        try:
            event["data"] = json.loads("\n".join(data_lines))
        except ValueError:
            event["parse_error"] = True
    return event


# --------------------------------------------------------------------------- reply assembly

def merge_usage(target: dict, usage) -> None:
    if isinstance(usage, dict):
        target.update({k: v for k, v in usage.items() if v is not None})


def codex_usage(usage) -> dict:
    """Responses counts cached tokens inside input_tokens; Messages counts them apart."""
    if not isinstance(usage, dict):
        return {}
    cached = (usage.get("input_tokens_details") or {}).get("cached_tokens") or 0
    return {
        "input_tokens": max((usage.get("input_tokens") or 0) - cached, 0),
        "output_tokens": usage.get("output_tokens") or 0,
        "cache_read_input_tokens": cached,
        "cache_creation_input_tokens": 0,
    }


def parse_args_text(text):
    try:
        return json.loads(text) if text and text.strip() else {}
    except ValueError:
        return text


def codex_item_to_block(item: dict, streaming: bool = False) -> dict:
    kind = item.get("type")
    if kind == "message":
        parts = item.get("content") or []
        return {"type": "text", "text": "".join(p.get("text", "") for p in parts if isinstance(p, dict))}
    if kind in ("function_call", "custom_tool_call"):
        text = item.get("arguments", item.get("input", "")) or ""
        block = {"type": "tool_use", "id": item.get("call_id"), "name": item.get("name")}
        if streaming and not text:
            return {**block, "input": {}, "partial_json": ""}
        return {**block, "input": parse_args_text(text)}
    if kind == "local_shell_call":
        return {"type": "tool_use", "id": item.get("call_id"), "name": "local_shell", "input": item.get("action")}
    if kind == "reasoning":
        summary = item.get("summary") or []
        return {"type": "thinking", "thinking": "\n".join(s.get("text", "") for s in summary if isinstance(s, dict))}
    return dict(item)


class Assembler:
    """Turns upstream events into `assembled`, returning a patch the browser applies.

    Browsers hold no protocol knowledge: a patch only says "set block i" or
    "append to a field of block i", so Messages and Responses share one UI path.
    """

    def __init__(self, assembled: dict) -> None:
        self.a = assembled
        self.first_token = False
        self.completed_tools: list = []
        self.opened = False  # saw message_start or response.created
        self.closed = False  # saw message_stop or the terminal response event
        self.error = None

    def _set(self, i: int, block: dict) -> dict:
        content = self.a["content"]
        while len(content) <= i:
            content.append({"type": "unknown"})
        content[i] = block
        if block.get("type") == "tool_use" and "partial_json" not in block:
            self.completed_tools.append(block)
        return {"i": i, "set": block}

    def _append(self, i: int, field: str, text: str) -> dict:
        content = self.a["content"]
        while len(content) <= i:
            content.append({"type": "unknown"})
        content[i][field] = content[i].get(field, "") + text
        if text:
            self.first_token = True
        return {"i": i, "field": field, "append": text}

    def feed(self, event: dict):
        data = event.get("data")
        if not isinstance(data, dict):
            return None
        kind = event.get("event") or data.get("type") or ""
        if data.get("type") == "error" or kind == "error":
            error = data.get("error") or data
            self.error = f"{error.get('type', 'error')}: {error.get('message', json.dumps(error))}"
            return {"error": self.error}
        if kind.startswith("response."):
            return self._feed_codex(kind, data)
        return self._feed_messages(kind, data)

    def _feed_messages(self, kind: str, data: dict):
        a = self.a
        if kind == "message_start":
            self.opened = True
            message = data.get("message") or {}
            a["model"] = message.get("model", a["model"])
            merge_usage(a["usage"], message.get("usage"))
            return {"model": a["model"], "usage": a["usage"]}
        if kind == "content_block_start":
            block = dict(data.get("content_block") or {})
            if block.get("type") in ("tool_use", "server_tool_use", "mcp_tool_use"):
                block["partial_json"] = ""
            return {"blocks": [self._set(data.get("index", 0), block)]}
        if kind == "content_block_delta":
            i, delta = data.get("index", 0), data.get("delta") or {}
            for delta_type, field, source in (
                ("text_delta", "text", "text"),
                ("thinking_delta", "thinking", "thinking"),
                ("input_json_delta", "partial_json", "partial_json"),
            ):
                if delta.get("type") == delta_type:
                    return {"blocks": [self._append(i, field, delta.get(source, ""))]}
            if delta.get("type") == "signature_delta":
                content = a["content"]
                if i < len(content):
                    content[i]["signature"] = content[i].get("signature", "") + delta.get("signature", "")
                    return {"blocks": [{"i": i, "set": content[i]}]}
            if delta.get("type") == "citations_delta" and i < len(a["content"]):
                a["content"][i].setdefault("citations", []).append(delta.get("citation"))
                return {"blocks": [{"i": i, "set": a["content"][i]}]}
            return None
        if kind == "content_block_stop":
            i = data.get("index", 0)
            if i < len(a["content"]) and "partial_json" in a["content"][i]:
                return {"blocks": [self._finish_input(i)]}
            return None
        if kind == "message_delta":
            delta = data.get("delta") or {}
            for field in ("stop_reason", "stop_sequence"):
                if field in delta:
                    a[field] = delta[field]
            merge_usage(a["usage"], data.get("usage"))
            return {"stop_reason": a.get("stop_reason"), "usage": a["usage"]}
        if kind == "message_stop":
            self.closed = True
        return None

    def _finish_input(self, i: int) -> dict:
        block = dict(self.a["content"][i])
        text = block.pop("partial_json", "")
        if text.strip():
            try:
                block["input"] = json.loads(text)
            except ValueError:
                block["input_raw"] = text  # upstream cut off mid-JSON: keep what came
        return self._set(i, block)

    def _feed_codex(self, kind: str, data: dict):
        a = self.a
        if kind in ("response.created", "response.in_progress"):
            self.opened = True
            a["model"] = (data.get("response") or {}).get("model", a["model"])
            return {"model": a["model"]}
        i = data.get("output_index", 0)
        if kind == "response.output_item.added":
            return {"blocks": [self._set(i, codex_item_to_block(data.get("item") or {}, streaming=True))]}
        if kind == "response.output_item.done":
            return {"blocks": [self._set(i, codex_item_to_block(data.get("item") or {}))]}
        if kind == "response.output_text.delta":
            return {"blocks": [self._append(i, "text", data.get("delta", ""))]}
        if kind in ("response.reasoning_summary_text.delta", "response.reasoning_text.delta"):
            return {"blocks": [self._append(i, "thinking", data.get("delta", ""))]}
        if kind in ("response.function_call_arguments.delta", "response.custom_tool_call_input.delta"):
            return {"blocks": [self._append(i, "partial_json", data.get("delta", ""))]}
        if kind in ("response.completed", "response.incomplete", "response.failed"):
            self.closed = True
            response = data.get("response") or {}
            a["model"] = response.get("model", a["model"])
            a["usage"] = codex_usage(response.get("usage"))
            has_tool = any(b.get("type") == "tool_use" for b in a["content"])
            a["stop_reason"] = "tool_use" if has_tool else response.get("status")
            if kind == "response.failed":
                self.error = json.dumps(response.get("error") or "response.failed")
            patch = {"model": a["model"], "usage": a["usage"], "stop_reason": a["stop_reason"]}
            return {**patch, "error": self.error} if self.error else patch
        return None

    def finish(self) -> list:
        """A stream that was cut off can leave tool input unfinished."""
        return [self._finish_input(i) for i, b in enumerate(self.a["content"]) if "partial_json" in b]

    def incomplete(self) -> bool:
        return self.opened and not self.closed and not self.error

    def from_json(self, body) -> None:
        """A non-streaming reply. Other JSON (count_tokens, errors) has no reply to assemble."""
        if not isinstance(body, dict):
            return
        a = self.a
        if body.get("type") == "message":
            a["model"] = body.get("model")
            a["content"] = body.get("content") or []
            a["stop_reason"] = body.get("stop_reason")
            merge_usage(a["usage"], body.get("usage"))
            self.completed_tools += [b for b in a["content"] if b.get("type") == "tool_use"]
        elif body.get("object") == "response":
            a["model"] = body.get("model")
            for i, item in enumerate(body.get("output") or []):
                self._set(i, codex_item_to_block(item))
            a["usage"] = codex_usage(body.get("usage"))
            has_tool = any(b.get("type") == "tool_use" for b in a["content"])
            a["stop_reason"] = "tool_use" if has_tool else body.get("status")


def new_assembled() -> dict:
    return {"model": None, "content": [], "stop_reason": None, "usage": {}}


# --------------------------------------------------------------------------- request views

def codex_view(body) -> dict | None:
    """Rewrite a Responses request into Messages shape, one message per input item,
    so positions stay stable. Returns None for a Messages request."""
    if not isinstance(body, dict) or "messages" in body or "input" not in body:
        return None
    items = body["input"]
    if isinstance(items, str):
        items = [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": items}]}]
    if not isinstance(items, list):
        return None
    tools = []
    for tool in body.get("tools") or []:
        if isinstance(tool, dict):
            tools.append({
                "name": tool.get("name") or tool.get("type"),
                "description": tool.get("description", ""),
                "input_schema": tool.get("parameters", tool),
            })
    return {
        "system": body.get("instructions"),
        "tools": tools,
        "messages": [codex_item_to_message(item) for item in items],
        "raw_messages": items,
    }


def codex_item_to_message(item) -> dict:
    if not isinstance(item, dict):
        return {"role": "user", "content": str(item)}
    kind = item.get("type", "message")
    if kind == "message":
        content = item.get("content")
        if isinstance(content, list):
            blocks = []
            for part in content:
                part_type = part.get("type") if isinstance(part, dict) else None
                if part_type in ("input_text", "output_text", "text"):
                    blocks.append({"type": "text", "text": part.get("text", "")})
                elif part_type == "input_image":
                    blocks.append({"type": "image", "source": {"type": "url", "url": part.get("image_url", "")}})
                else:
                    blocks.append(part)
            content = blocks
        return {"role": item.get("role", "user"), "content": content}
    if kind in ("function_call", "custom_tool_call", "local_shell_call", "reasoning"):
        return {"role": "assistant", "content": [codex_item_to_block(item)]}
    if kind in ("function_call_output", "custom_tool_call_output", "local_shell_call_output"):
        return {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": item.get("call_id"), "content": item.get("output")}
        ]}
    return {"role": item.get("role", "assistant"), "content": [dict(item)]}


def strip_cache_control(value):
    if isinstance(value, dict):
        return {k: strip_cache_control(v) for k, v in value.items() if k != "cache_control"}
    if isinstance(value, list):
        return [strip_cache_control(v) for v in value]
    return value


def digest(value) -> str:
    return hashlib.sha1(json.dumps(value, sort_keys=True).encode("ascii")).hexdigest()


def message_hash(message) -> str:
    """Identity of a message for §6.7. Harnesses move cache_control between turns and
    may drop old thinking, so neither makes a resent message count as different."""
    if not isinstance(message, dict):
        return digest(message)
    content = message.get("content")
    if isinstance(content, str):
        content = [{"type": "text", "text": content}]
    if isinstance(content, list):
        content = [
            strip_cache_control(b) for b in content
            if not (isinstance(b, dict) and b.get("type") in ("thinking", "redacted_thinking"))
        ]
    return digest([message.get("role"), content])


def system_hash(system) -> str:
    """The per-request billing header some harnesses add is not part of the prompt."""
    if isinstance(system, list):
        system = [
            strip_cache_control(b) for b in system
            if not (isinstance(b, dict) and str(b.get("text", "")).startswith("x-anthropic-billing-header"))
        ]
    return digest(system)


def content_size(content) -> int:
    """Size of a tool result in characters."""
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        return sum(
            len(b["text"]) if isinstance(b, dict) and isinstance(b.get("text"), str)
            else len(json.dumps(b))
            for b in content
        )
    return 0 if content is None else len(json.dumps(content))


# --------------------------------------------------------------------------- pricing

class Pricing:
    """pricing.json, re-read when it changes so edits apply without a restart."""

    FIELDS = ("input", "output", "cache_read", "cache_write")

    def __init__(self, path: str) -> None:
        self.path, self.mtime, self.table = path, None, {}

    def _reload(self) -> None:
        try:
            mtime = os.path.getmtime(self.path)
            if mtime != self.mtime:
                with open(self.path, encoding="utf-8") as f:
                    table = json.load(f)
                self.mtime, self.table = mtime, table
        except (OSError, ValueError) as e:
            print(f"pricing.json not usable, keeping the last good copy: {e}", file=sys.stderr)

    def cost(self, model, usage: dict):
        """None for an unknown model or a reply with no usage: never a guessed price.
        A dated id like claude-x-20260101 uses its base entry; nothing fuzzier."""
        self._reload()
        if not model or not usage:
            return None
        entry = self.table.get(model) or self.table.get(re.sub(r"-(\d{8}|latest)$", "", model))
        if not isinstance(entry, dict) or any(not isinstance(entry.get(f), (int, float)) for f in self.FIELDS):
            return None
        total = (
            (usage.get("input_tokens") or 0) * entry["input"]
            + (usage.get("output_tokens") or 0) * entry["output"]
            + (usage.get("cache_read_input_tokens") or 0) * entry["cache_read"]
            + (usage.get("cache_creation_input_tokens") or 0) * entry["cache_write"]
        )
        return round(total / 1_000_000, 6)


# --------------------------------------------------------------------------- recorder and bus

class Subscriber:
    """One browser. The queue is bounded; when it is full, frames are dropped and the
    browser gets a fresh snapshot once it catches up, so a drop cannot leave it wrong."""

    def __init__(self) -> None:
        self.queue: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE)
        self.dropped = False

    def offer(self, frame: bytes) -> None:
        try:
            self.queue.put_nowait(frame)
        except queue.Full:
            self.dropped = True


def frame(name: str, payload) -> bytes:
    return f"event: {name}\ndata: {json.dumps(payload)}\n\n".encode("ascii")


class Recorder(threading.Thread):
    """Single owner of all state. Other threads only post() onto its queue, so state
    needs no locks and subscribing is atomic with taking the snapshot."""

    def __init__(self, capacity: int, pricing: Pricing) -> None:
        super().__init__(daemon=True, name="recorder")
        self.inbox: queue.SimpleQueue = queue.SimpleQueue()
        self.capacity = capacity
        self.pricing = pricing
        self.ring: dict = {}     # id -> exchange, oldest first
        self.meta: dict = {}     # id -> hashes etc., never serialized
        self.active: dict = {}   # id -> per-stream parser state
        self.tools: dict = {}    # tool_use id -> §4 index entry
        self.subscribers: list = []

    def post(self, method: str, *args) -> None:
        self.inbox.put((method, args))

    def call(self, fn, timeout: float = 30):
        """Run fn on the recorder thread and wait for the answer (dashboard side only)."""
        answer: queue.SimpleQueue = queue.SimpleQueue()
        self.post("call", fn, answer)
        return answer.get(timeout=timeout)

    def run(self) -> None:
        while True:
            method, args = self.inbox.get()
            try:
                getattr(self, "on_" + method)(*args)
            except Exception:  # noqa: BLE001 - one bad exchange must not stop recording
                traceback.print_exc()

    def on_call(self, fn, answer) -> None:
        answer.put(fn())

    def publish(self, name: str, payload) -> None:
        if self.subscribers:
            data = frame(name, payload)
            for subscriber in self.subscribers:
                subscriber.offer(data)

    # ---- browsers

    def snapshot_frames(self) -> list:
        return [
            b"event: snapshot\ndata: " + self.dump_buffer() + b"\n\n",
            frame("tool_index", self.tools),
        ]

    def dump_buffer(self) -> bytes:
        # One exchange at a time: the GIL is released between dumps, so a large
        # snapshot cannot hold up a forwarding thread for its whole duration.
        return b"[" + b",".join(json.dumps(x).encode("ascii") for x in list(self.ring.values())) + b"]"

    def on_subscribe(self, subscriber: Subscriber) -> None:
        for data in self.snapshot_frames():
            subscriber.offer(data)
        self.subscribers.append(subscriber)

    def on_unsubscribe(self, subscriber: Subscriber) -> None:
        if subscriber in self.subscribers:
            self.subscribers.remove(subscriber)

    def on_resync(self, subscriber: Subscriber) -> None:
        try:
            while True:
                subscriber.queue.get_nowait()
        except queue.Empty:
            pass
        subscriber.dropped = False
        for data in self.snapshot_frames():
            subscriber.offer(data)

    def on_clear(self) -> None:
        # Ids keep rising so old links can never point at a different exchange. An
        # exchange still in flight leaves the buffer too; browsers ignore its events.
        self.ring.clear()
        self.meta.clear()
        self.tools.clear()
        self.publish("cleared", {})

    # ---- exchanges

    def on_started(self, xid: int, ts: float, method: str, path: str, headers: list, body: bytes) -> None:
        request_body = None
        if body:
            try:
                request_body = json.loads(body)
            except (ValueError, RecursionError):
                request_body = body.decode("utf-8", "replace")
        view = codex_view(request_body)
        source = view or (request_body if isinstance(request_body, dict) else {})
        messages = source.get("messages")
        messages = messages if isinstance(messages, list) else []

        meta = {
            "system": system_hash(source.get("system")),
            "hashes": [message_hash(m) for m in messages],
            "t0": ts,
            "ended_ts": None,
        }
        exchange = {
            "id": xid,
            "started_at": iso(ts),
            "ended_at": None,
            "method": method,
            "path": path,
            "status": None,
            "request_headers": redact_headers(headers),
            "request_body": request_body,
            "response_headers": {},
            "response_events": [],
            "response_body": None,
            "assembled": new_assembled(),
            "timing": {"ttfb_ms": None, "ttft_ms": None, "total_ms": None},
            "cost_usd": None,
            "error": None,
            "view": view,
            "conversation": self.find_previous(meta),
        }
        self.ring[xid] = exchange
        self.meta[xid] = meta
        self.active[xid] = {
            "exchange": exchange, "assembler": Assembler(exchange["assembled"]), "parser": None,
            "body": bytearray(), "received": 0, "expected": None, "t0": ts,
        }
        self.publish("exchange_started", {
            "id": xid, "time": exchange["started_at"], "method": method, "path": path,
            "headers": exchange["request_headers"], "body": request_body,
            "view": view, "conversation": exchange["conversation"],
        })
        self.scan_tool_results(xid, messages, meta, ts)
        self.evict()

    def find_previous(self, meta: dict) -> dict:
        """The longest earlier request whose messages are a prefix of this one with the
        same system prompt; on a tie the most recent, so a retry diffs against itself."""
        hashes = meta["hashes"]
        best_id, best_len = None, 0
        for xid, other in self.meta.items():
            n = len(other["hashes"])
            if n and n <= len(hashes) and n >= best_len and other["system"] == meta["system"] \
                    and other["hashes"] == hashes[:n] and (n > best_len or best_id is None or xid > best_id):
                best_id, best_len = xid, n
        return {"prev_exchange": best_id, "new_from": best_len}

    def scan_tool_results(self, xid: int, messages: list, meta: dict, ts: float) -> None:
        carried = set()
        for position, message in enumerate(messages):
            content = message.get("content") if isinstance(message, dict) else None
            for block in content if isinstance(content, list) else []:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    carried.add(block.get("tool_use_id"))
                    self.pair(block, xid, position, ts)
        # §6.4 "moves on": the request extends the one that asked for the tool, yet has
        # no result for it. An identical length is a retry, which has not moved on.
        for tool_id, entry in self.tools.items():
            call = self.meta.get(entry["call_exchange"])
            if entry["status"] != "pending" or tool_id in carried or call is None:
                continue
            n = len(call["hashes"])
            if call["system"] == meta["system"] and len(meta["hashes"]) > n and meta["hashes"][:n] == call["hashes"]:
                entry["status"] = "no_result"
                self.publish("tool_update", {"tool_use_id": tool_id, "entry": entry})

    def pair(self, block: dict, xid: int, position: int, ts: float) -> None:
        tool_id = block.get("tool_use_id")
        entry = self.tools.get(tool_id)
        if entry is None or entry["result_exchange"] is not None or entry["call_exchange"] == xid:
            return  # never captured the call, or already paired by an earlier request
        ended = (self.meta.get(entry["call_exchange"]) or {}).get("ended_ts")
        entry.update(
            result_exchange=xid,
            result=block.get("content"),
            is_error=bool(block.get("is_error")),
            status="paired",
            result_message_index=position,
            result_size=content_size(block.get("content")),
            wait_ms=round((ts - ended) * 1000) if ended else None,
        )
        self.publish("tool_paired", {
            "tool_use_id": tool_id,
            "call_exchange": entry["call_exchange"],
            "result_exchange": xid,
            "result": entry["result"],
            "entry": entry,
        })

    def register_tools(self, xid: int, assembler: Assembler) -> None:
        tools, assembler.completed_tools = assembler.completed_tools, []
        for block in tools:
            tool_id = block.get("id")
            if tool_id and tool_id not in self.tools:
                self.tools[tool_id] = {
                    "name": block.get("name"),
                    "input": block.get("input"),
                    "call_exchange": xid,
                    "result_exchange": None,
                    "result": None,
                    "is_error": False,
                    "status": "pending",
                    "result_message_index": None,
                    "result_size": None,
                    "wait_ms": None,
                }
                self.publish("tool_update", {"tool_use_id": tool_id, "entry": self.tools[tool_id]})

    def evict(self) -> None:
        while len(self.ring) > self.capacity:
            oldest = next(iter(self.ring))
            del self.ring[oldest]
            self.meta.pop(oldest, None)
            # An entry lives as long as the last exchange it refers to.
            gone = [
                t for t, e in self.tools.items()
                if (e["result_exchange"] if e["result_exchange"] is not None else e["call_exchange"]) == oldest
            ]
            for tool_id in gone:
                del self.tools[tool_id]
            self.publish("exchange_evicted", {"id": oldest})

    def on_response(self, xid: int, ts: float, status: int, headers: list) -> None:
        state = self.active[xid]
        exchange = state["exchange"]
        exchange["status"] = status
        exchange["response_headers"] = redact_headers(headers)
        exchange["timing"]["ttfb_ms"] = round((ts - state["t0"]) * 1000)
        lowered = {k.lower(): v for k, v in headers}
        if "text/event-stream" in lowered.get("content-type", "").lower():
            state["parser"] = SSEParser()
        length = lowered.get("content-length", "")
        state["expected"] = int(length) if length.isdigit() and exchange["method"] != "HEAD" else None
        self.publish("exchange_response", {
            "id": xid, "status": status, "headers": exchange["response_headers"], "timing": exchange["timing"],
        })

    def on_chunk(self, xid: int, ts: float, chunk: bytes) -> None:
        state = self.active[xid]
        state["received"] += len(chunk)
        if state["parser"] is None:
            state["body"] += chunk
            return
        for event in state["parser"].feed(chunk):
            self.handle_event(xid, state, event, ts)

    def handle_event(self, xid: int, state: dict, event: dict, ts: float) -> None:
        exchange, assembler = state["exchange"], state["assembler"]
        exchange["response_events"].append(event)
        patch = None
        try:
            patch = assembler.feed(event)
        except Exception as e:  # noqa: BLE001 - an odd event stays in response_events
            event["assemble_error"] = str(e)
        if assembler.first_token and exchange["timing"]["ttft_ms"] is None:
            exchange["timing"]["ttft_ms"] = round((ts - state["t0"]) * 1000)
            patch = {**(patch or {}), "timing": exchange["timing"]}
        self.publish("exchange_delta", {"id": xid, "event": event, "patch": patch})
        self.register_tools(xid, assembler)
        if assembler.error:
            self.set_error(xid, exchange, f"upstream error: {assembler.error}")

    def set_error(self, xid: int, exchange: dict, message: str) -> None:
        if exchange["error"] is None:
            exchange["error"] = message
            self.publish("exchange_error", {"id": xid, "message": message})

    def on_end(self, xid: int, ts: float, error) -> None:
        state = self.active.pop(xid)
        exchange, assembler = state["exchange"], state["assembler"]
        if state["parser"] is not None:
            for event in state["parser"].flush():
                self.handle_event(xid, state, event, ts)
        elif state["body"]:
            text = bytes(state["body"]).decode("utf-8", "replace")
            try:
                exchange["response_body"] = json.loads(text)
            except (ValueError, RecursionError):
                exchange["response_body"] = text
            try:
                assembler.from_json(exchange["response_body"])
            except (AttributeError, TypeError) as e:  # valid JSON, unexpected shape: still finish
                exchange["assembled"]["content"] = []
                print(f"exchange {xid}: could not assemble reply: {e}", file=sys.stderr)
        assembler.finish()
        self.register_tools(xid, assembler)
        if error:
            self.set_error(xid, exchange, error)
        if state["expected"] is not None and state["received"] < state["expected"]:
            self.set_error(xid, exchange, f"upstream closed early: {state['received']} of {state['expected']} bytes")
        if assembler.incomplete():
            self.set_error(xid, exchange, "stream ended before its final event")
        exchange["ended_at"] = iso(ts)
        exchange["timing"]["total_ms"] = round((ts - state["t0"]) * 1000)
        assembled = exchange["assembled"]
        model = assembled["model"] or (
            exchange["request_body"].get("model") if isinstance(exchange["request_body"], dict) else None
        )
        exchange["cost_usd"] = self.pricing.cost(model, assembled["usage"])
        if xid in self.meta:
            self.meta[xid]["ended_ts"] = ts
        # Sent on failure too: it is what stops the row's spinner.
        self.publish("exchange_done", {
            "id": xid, "ended_at": exchange["ended_at"], "assembled": assembled, "status": exchange["status"],
            "timing": exchange["timing"], "cost_usd": exchange["cost_usd"], "error": exchange["error"],
            "response_body": exchange["response_body"],
        })


RECORDER: Recorder = None  # type: ignore[assignment]
_ids = itertools.count(1)
_ids_lock = threading.Lock()


def next_id() -> int:
    with _ids_lock:
        return next(_ids)


# --------------------------------------------------------------------------- proxy (:8041)

class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # we print our own lines

    def do_POST(self) -> None:
        self.relay()

    def do_GET(self) -> None:
        self.relay()

    def do_HEAD(self) -> None:
        self.relay()

    def relay(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        xid = next_id()
        RECORDER.post("started", xid, time.time(), self.command, self.path, self.headers.items(), body)

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
        # Connect with a short timeout (a lost SYN must not stall the agent for
        # minutes); the long timeout is for reading a slow stream.
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
        except OSError as e:
            say(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}", flush=True)
            RECORDER.post("end", xid, time.time(), f"upstream unreachable: {e}")
            self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            self.close_connection = True
            return
        try:
            self.forward(xid, upstream, target, body)
        finally:
            upstream.close()
            self.close_connection = True

    def forward(self, xid: int, upstream, target, body: bytes) -> None:
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        try:
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()
        except (OSError, http.client.HTTPException) as e:
            say(f"\n[{stamp()}] !!! upstream failed: {e}", flush=True)
            RECORDER.post("end", xid, time.time(), f"upstream failed before answering: {e}")
            try:
                self.send_error(502, f"proxy: upstream failed: {e}")
            except OSError:
                pass
            return
        RECORDER.post("response", xid, time.time(), reply.status, reply.getheaders())

        # 3. Stream the reply back as it arrives, printing it on the way.
        say(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        try:
            self.send_response(reply.status, reply.reason)
            for name, value in reply.getheaders():
                if name.lower() not in SKIP and name.lower() != "content-encoding":
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
        except OSError as e:
            RECORDER.post("end", xid, time.time(), f"client disconnected: {e}")
            return
        if self.command == "HEAD":
            RECORDER.post("end", xid, time.time(), None)
            return
        while True:
            try:
                chunk = reply.read1(4096)  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
            except (OSError, http.client.HTTPException) as e:
                say(f"\n[{stamp()}] !!! upstream failed mid-stream: {e}", flush=True)
                RECORDER.post("end", xid, time.time(), f"upstream failed mid-stream: {e or type(e).__name__}")
                return
            if not chunk:
                break
            RECORDER.post("chunk", xid, time.time(), chunk)
            try:
                self.wfile.write(chunk)
                self.wfile.flush()
            except OSError as e:
                say(f"\n[{stamp()}] !!! client disconnected: {e}", flush=True)
                RECORDER.post("end", xid, time.time(), f"client disconnected: {e}")
                return
            say_chunk(chunk)
        say(f"\n[{stamp()}] --- done", flush=True)
        RECORDER.post("end", xid, time.time(), None)


# --------------------------------------------------------------------------- dashboard (:8042)

class UI(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    LOCAL_NAMES = {"localhost", "127.0.0.1", "::1"}
    CSP = (
        "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
        "connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'"
    )

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def _local(self) -> bool:
        """Binding to 127.0.0.1 does not stop a web page reaching us: refuse rebound
        Host names, and cross-site POSTs to /api/clear."""
        try:
            if urlsplit("//" + self.headers.get("host", "")).hostname not in self.LOCAL_NAMES:
                return False
            origin = self.headers.get("origin")
            return not origin or urlsplit(origin).hostname in self.LOCAL_NAMES
        except ValueError:
            return False

    def _send(self, code: int, body: bytes, ctype: str = "application/json", extra: dict | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, payload) -> None:
        self._send(code, json.dumps(payload).encode("ascii"))

    def do_GET(self) -> None:
        if not self._local():
            return self._json(403, {"error": "local requests only"})
        path = urlsplit(self.path).path
        if path == "/":
            try:
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    page = f.read()
            except OSError as e:
                return self._json(500, {"error": f"cannot read dashboard.html: {e}"})
            return self._send(200, page, "text/html; charset=utf-8", {"Content-Security-Policy": self.CSP})
        if path == "/events":
            return self._events()
        if path == "/api/export":
            name = f"traffic-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
            body = RECORDER.call(RECORDER.dump_buffer)
            return self._send(200, body, extra={"Content-Disposition": f'attachment; filename="{name}"'})
        if match := re.fullmatch(r"/api/exchanges/(\d+)", path):
            xid = int(match.group(1))
            body = RECORDER.call(lambda: json.dumps(RECORDER.ring[xid]).encode("ascii") if xid in RECORDER.ring else None)
            return self._send(200, body) if body else self._json(404, {"error": f"no exchange {xid} in the buffer"})
        if path == "/favicon.ico":
            return self._send(204, b"", "image/x-icon")
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        if length:
            self.rfile.read(length)
        if not self._local():
            return self._json(403, {"error": "local requests only"})
        if urlsplit(self.path).path == "/api/clear":
            RECORDER.post("clear")
            return self._json(200, {"ok": True})
        self._json(404, {"error": "not found"})

    def _events(self) -> None:
        self.close_connection = True
        subscriber = Subscriber()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        RECORDER.post("subscribe", subscriber)
        try:
            self.wfile.write(b"retry: 1000\n\n")
            while True:
                if subscriber.dropped:
                    subscriber.dropped = False
                    RECORDER.post("resync", subscriber)
                try:
                    self.wfile.write(subscriber.queue.get(timeout=KEEPALIVE_SECONDS))
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
        except OSError:
            pass  # the tab closed or reloaded; the agent never notices
        finally:
            RECORDER.post("unsubscribe", subscriber)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def handle_error(self, request, client_address) -> None:
        kind = sys.exc_info()[0]
        if not (kind and issubclass(kind, ConnectionError)):
            super().handle_error(request, client_address)


def main() -> None:
    global UPSTREAM, QUIET, RECORDER
    parser = argparse.ArgumentParser(description="Forward an agent's API traffic and show it live in a browser.")
    parser.add_argument("--port", type=int, default=8041, help="proxy port")
    parser.add_argument("--ui-port", type=int, default=8042, help="dashboard port")
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--quiet", action="store_true", help="no per-request terminal output")
    parser.add_argument("--buffer", type=int, default=200, help="exchanges kept in memory")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    QUIET = args.quiet

    RECORDER = Recorder(max(args.buffer, 1), Pricing(os.path.join(HERE, "pricing.json")))
    RECORDER.start()
    try:
        proxy = Server(("127.0.0.1", args.port), Tap)
        ui = Server(("127.0.0.1", args.ui_port), UI)
    except OSError as e:
        sys.exit(f"cannot bind: {e}")
    threading.Thread(target=ui.serve_forever, daemon=True, name="ui").start()
    # The banner shows even with --quiet: without it the URLs are nowhere to be found.
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"dashboard on http://localhost:{args.ui_port}")
    print(f"ANTHROPIC_BASE_URL=http://localhost:{args.port} claude", flush=True)
    try:
        proxy.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
