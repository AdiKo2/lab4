#!/usr/bin/env python3
"""dashboard.py: the agent-traffic proxy with a live browser dashboard.

One process, two ports:

    :8041  the proxy. Forwards every request unchanged, exactly like proxy.py.
    :8042  the dashboard. Serves dashboard.html and a Server-Sent Events stream.

    python3 dashboard.py
    ANTHROPIC_BASE_URL=http://localhost:8041 claude
    open http://localhost:8042

    python3 dashboard.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
    python3 dashboard.py --quiet                                            # no terminal dump

Standard library only; nothing to install.
"""

from __future__ import annotations

import argparse
import copy
import functools
import gzip
import hashlib
import http.client
import json
import os
import queue
import re
import sys
import threading
import time
import traceback
import zlib
from collections import OrderedDict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))

# Hop-by-hop headers are never forwarded. `accept-encoding` is dropped so the
# API answers in plain text that we can parse and show. (Same list as proxy.py.)
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}
# Headers whose values are secrets: stored and printed as the first 16 chars + "...".
SECRET_HEADERS = {"authorization", "x-api-key", "cookie", "set-cookie", "proxy-authorization"}

UPSTREAM = "https://api.anthropic.com"
QUIET = False
BUFFER_SIZE = 200
SUBSCRIBER_QUEUE = 5000          # events buffered per browser before it gets a fresh snapshot
# The event that ends a complete stream. A stream that stops without one failed.
FINAL_EVENTS = {"message_stop", "response.completed", "response.incomplete", "response.failed"}


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def redact(name: str, value: str) -> str:
    if name.lower() in SECRET_HEADERS:
        return value[:16] + "..." if len(value) > 16 else "..."
    return value


def redacted_headers(items) -> dict:
    out: dict = {}
    for name, value in items:
        value = redact(name, value)
        out[name] = out[name] + ", " + value if name in out else value
    return out


def header(headers: dict, name: str):
    name = name.lower()
    for k, v in (headers or {}).items():
        if k.lower() == name:
            return v
    return None


def warn(msg: str) -> None:
    print(f"[dashboard] {msg}", file=sys.stderr, flush=True)


def guarded(fn):
    """The dashboard must never break forwarding: log and swallow any error."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:  # noqa: BLE001
            warn(f"internal error in {fn.__name__}:\n{traceback.format_exc()}")
            return None
    return wrapper


# --------------------------------------------------------------------------
# Pricing


class Pricing:
    """pricing.json, re-read whenever the file changes."""

    def __init__(self, path: str):
        self.path = path
        self.mtime = None
        self.table: dict = {}
        self.lock = threading.Lock()

    def _reload(self) -> None:
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            self.table = {}
            return
        if mtime == self.mtime:
            return
        self.mtime = mtime
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            self.table = {k: v for k, v in data.items() if not k.startswith("_") and isinstance(v, dict)}
        except (OSError, ValueError) as e:
            warn(f"cannot read {self.path}: {e} (keeping previous prices)")

    def lookup(self, model):
        """Exact match, or the name without a -YYYYMMDD suffix."""
        if not isinstance(model, str) or not model:
            return None
        with self.lock:
            self._reload()
            return self.table.get(model) or self.table.get(re.sub(r"-\d{8}$", "", model))

    def cost(self, model, usage):
        """An estimate: long-context rates aren't modeled."""
        price = self.lookup(model)
        if not price or not isinstance(usage, dict):
            return None
        n = lambda d, k: (d or {}).get(k) or 0  # noqa: E731
        split = usage.get("cache_creation")
        if isinstance(split, dict):
            write_5m = n(split, "ephemeral_5m_input_tokens")
            write_1h = n(split, "ephemeral_1h_input_tokens")
        else:
            write_5m, write_1h = n(usage, "cache_creation_input_tokens"), 0
        return (
            n(usage, "input_tokens") * price.get("input", 0)
            + n(usage, "output_tokens") * price.get("output", 0)
            + n(usage, "cache_read_input_tokens") * price.get("cache_read", 0)
            + write_5m * price.get("cache_write_5m", 0)
            + write_1h * price.get("cache_write_1h", 0)
        ) / 1_000_000


PRICING = Pricing(os.path.join(HERE, "pricing.json"))


# --------------------------------------------------------------------------
# Upstream stream parsing


class SSEParser:
    """Splits a byte stream into (event, data) pairs."""

    BOUNDARY = re.compile(rb"\r?\n\r?\n")

    def __init__(self):
        self.buf = b""

    def feed(self, chunk: bytes):
        self.buf += chunk
        out = []
        while True:
            m = self.BOUNDARY.search(self.buf)
            if not m:
                return out
            block, self.buf = self.buf[: m.start()], self.buf[m.end():]
            out.extend(self._parse(block))

    def flush(self):
        block, self.buf = self.buf, b""
        return self._parse(block) if block.strip() else []

    @staticmethod
    def _parse(block: bytes):
        name, data = None, []
        for line in block.decode("utf-8", "replace").splitlines():
            if not line or line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "event":
                name = value
            elif field == "data":
                data.append(value)
        if name is None and not data:
            return []
        return [(name or "message", "\n".join(data))]


def apply_patch(a: dict, p: dict) -> None:
    """Apply one patch to an `assembled` reply. dashboard.html has the same function."""
    op = p.get("op")
    content = a.setdefault("content", [])
    if op == "meta":
        for k, v in p["set"].items():
            if v is not None:
                a[k] = v
    elif op == "usage":
        usage = a.setdefault("usage", {})
        for k, v in p["set"].items():
            if v is not None:
                usage[k] = v
    elif op == "block":
        i = p["i"]
        while len(content) <= i:
            content.append({"type": "unknown"})
        content[i] = dict(p["block"])
    elif op in ("append", "field", "finish"):
        i = p["i"]
        if not (0 <= i < len(content)):
            return
        b = content[i]
        if op == "append":
            b[p["field"]] = (b.get(p["field"]) or "") + p["text"]
        elif op == "field":
            b[p["field"]] = p["value"]
        else:
            b["input"] = p["input"]
            b.pop("_partial_json", None)
    elif op == "error":
        a["error"] = p["error"]


def parse_tool_input(raw: str):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except ValueError:
        return {"_raw": raw}


class Assembler:
    """Turns upstream SSE events (Anthropic Messages or OpenAI Responses/Codex)
    into patches on the `assembled` reply. The patches are also sent to the
    browser, so both sides build the same reply with the same rules."""

    def __init__(self):
        self.codex_blocks: dict = {}  # Codex output_index -> our content index
        self.first_token = False      # set at the first non-empty text, thinking or tool-input delta

    def feed(self, name: str, d, assembled: dict) -> list:
        if not isinstance(d, dict):
            return []
        t = d.get("type") or name or ""
        if t.startswith("response.") or name.startswith("response."):
            return self._codex(t, d, assembled)
        return self._anthropic(t, d, assembled)

    def _anthropic(self, t: str, d: dict, a: dict) -> list:
        if t == "message_start":
            m = d.get("message") or {}
            out = [{"op": "meta", "set": {"model": m.get("model"), "id": m.get("id")}}]
            if isinstance(m.get("usage"), dict):
                out.append({"op": "usage", "set": m["usage"]})
            return out
        if t == "content_block_start":
            i = d.get("index", len(a.get("content", [])))
            block = dict(d.get("content_block") or {"type": "unknown"})
            if block.get("type") in ("tool_use", "server_tool_use", "mcp_tool_use"):
                block["_partial_json"] = ""
            return [{"op": "block", "i": i, "block": block}]
        if t == "content_block_delta":
            i = d.get("index", 0)
            delta = d.get("delta") or {}
            dt = delta.get("type")
            field, key = {"text_delta": ("text", "text"), "input_json_delta": ("_partial_json", "partial_json"),
                          "thinking_delta": ("thinking", "thinking")}.get(dt, (None, None))
            if field:
                text = delta.get(key) or ""
                if text:
                    self.first_token = True
                return [{"op": "append", "i": i, "field": field, "text": text}]
            if dt == "signature_delta":
                return [{"op": "field", "i": i, "field": "signature", "value": delta.get("signature")}]
            return []
        if t == "content_block_stop":
            i = d.get("index", 0)
            content = a.get("content") or []
            if 0 <= i < len(content) and "_partial_json" in content[i]:
                raw = content[i]["_partial_json"]
                parsed = parse_tool_input(raw) if raw else content[i].get("input", {})
                return [{"op": "finish", "i": i, "input": parsed}]
            return []
        if t == "message_delta":
            delta = d.get("delta") or {}
            out = [{"op": "meta", "set": {"stop_reason": delta.get("stop_reason"),
                                          "stop_sequence": delta.get("stop_sequence")}}]
            if isinstance(d.get("usage"), dict):
                out.append({"op": "usage", "set": d["usage"]})
            return out
        if t == "error":
            return [{"op": "error", "error": d.get("error") or d}]
        return []

    def _codex(self, t: str, d: dict, a: dict) -> list:
        out: list = []
        resp = d.get("response") if isinstance(d.get("response"), dict) else None
        if t in ("response.created", "response.in_progress") and resp:
            return [{"op": "meta", "set": {"model": resp.get("model"), "id": resp.get("id")}}]
        oi = d.get("output_index")
        if t == "response.output_item.added":
            item = d.get("item") or {}
            it = item.get("type")
            if it == "message":
                block = {"type": "text", "text": ""}
            elif it in ("function_call", "custom_tool_call"):
                block = {"type": "tool_use", "id": item.get("call_id") or item.get("id"),
                         "name": item.get("name"), "input": {},
                         "_partial_json": item.get("arguments") or item.get("input") or ""}
            elif it == "reasoning":
                block = {"type": "thinking", "thinking": ""}
            else:
                block = {"type": it or "unknown", "item": item}
            i = len(a.get("content") or [])
            self.codex_blocks[oi] = i
            return [{"op": "block", "i": i, "block": block}]
        i = self.codex_blocks.get(oi)
        field = {"response.output_text.delta": "text",
                 "response.function_call_arguments.delta": "_partial_json",
                 "response.custom_tool_call_input.delta": "_partial_json",
                 "response.reasoning_summary_text.delta": "thinking",
                 "response.reasoning_text.delta": "thinking"}.get(t)
        if field and i is not None:
            text = d.get("delta") or ""
            if text:
                self.first_token = True
            return [{"op": "append", "i": i, "field": field, "text": text}]
        if t == "response.output_item.done" and i is not None:
            item = d.get("item") or {}
            if item.get("type") in ("function_call", "custom_tool_call"):
                raw = item.get("arguments") if item.get("type") == "function_call" else item.get("input")
                if raw is None:
                    raw = (a.get("content") or [{}])[i].get("_partial_json", "")
                return [{"op": "finish", "i": i, "input": parse_tool_input(raw)}]
            if item.get("type") not in ("message", "reasoning"):
                return [{"op": "block", "i": i, "block": {"type": item.get("type") or "unknown", "item": item}}]
            return []
        if t in ("response.completed", "response.incomplete", "response.failed") and resp:
            u = resp.get("usage") or {}
            if u:
                cached = (u.get("input_tokens_details") or {}).get("cached_tokens") or 0
                out.append({"op": "usage", "set": {
                    "input_tokens": max((u.get("input_tokens") or 0) - cached, 0),
                    "cache_read_input_tokens": cached,
                    "cache_creation_input_tokens": 0,
                    "output_tokens": u.get("output_tokens") or 0,
                }})
            has_tool = any(b.get("type") == "tool_use" for b in a.get("content") or [])
            stop = "tool_use" if has_tool else {"completed": "end_turn"}.get(resp.get("status"), resp.get("status"))
            out.append({"op": "meta", "set": {"model": resp.get("model"), "stop_reason": stop}})
            if resp.get("error"):
                out.append({"op": "error", "error": resp["error"]})
            return out
        if t in ("error", "response.error"):
            return [{"op": "error", "error": d.get("error") or d}]
        return out


# --------------------------------------------------------------------------
# Requests: tool calls and results, for pairing


def tool_items(body):
    """Yield (message_index, kind, data) for every client tool call/result in a
    request body. kind is "use" or "result"."""
    if not isinstance(body, dict):
        return
    if isinstance(body.get("messages"), list):  # Anthropic Messages
        for mi, m in enumerate(body["messages"]):
            content = m.get("content") if isinstance(m, dict) else None
            if not isinstance(content, list):
                continue
            for b in content:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "tool_use":
                    yield mi, "use", {"id": b.get("id"), "name": b.get("name"), "input": b.get("input")}
                elif b.get("type") == "tool_result":
                    yield mi, "result", {"id": b.get("tool_use_id"), "content": b.get("content"),
                                         "is_error": bool(b.get("is_error"))}
    elif isinstance(body.get("input"), list):  # OpenAI Responses (Codex)
        for mi, it in enumerate(body["input"]):
            if not isinstance(it, dict):
                continue
            t = it.get("type")
            if t == "function_call":
                yield mi, "use", {"id": it.get("call_id"), "name": it.get("name"),
                                  "input": parse_tool_input(it.get("arguments") or "")}
            elif t == "custom_tool_call":
                yield mi, "use", {"id": it.get("call_id"), "name": it.get("name"), "input": {"_raw": it.get("input")}}
            elif t in ("function_call_output", "custom_tool_call_output"):
                yield mi, "result", {"id": it.get("call_id"), "content": it.get("output"), "is_error": False}


def message_count(body) -> int:
    if isinstance(body, dict):
        for key in ("messages", "input"):
            if isinstance(body.get(key), list):
                return len(body[key])
    return 0


def content_size(content) -> int:
    if content is None:
        return 0
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        total = 0
        for b in content:
            if isinstance(b, dict) and b.get("type") == "text":
                total += len(b.get("text") or "")
            else:
                total += len(json.dumps(b, ensure_ascii=False))
        return total
    return len(json.dumps(content, ensure_ascii=False))


def decode_body(raw: bytes, headers):
    """Parsed JSON, or text, or None when there is no body."""
    if not raw:
        return None
    enc = (header(headers, "content-encoding") or "").lower()
    try:
        if enc == "gzip":
            raw = gzip.decompress(raw)
        elif enc == "deflate":
            raw = zlib.decompress(raw)
    except (OSError, zlib.error):
        pass
    text = raw.decode("utf-8", "replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def error_info(status, headers: dict, body, stream_error):
    """Error type and message, plus retry-after and rate-limit headers."""
    failed = (status is not None and status >= 400) or stream_error is not None
    if not failed:
        return None
    err = stream_error
    if err is None and isinstance(body, dict):
        err = body.get("error") if isinstance(body.get("error"), dict) else None
        if err is None and body.get("detail"):
            err = {"type": "error", "message": str(body["detail"])}
    info = {"status": status, "type": None, "message": None}
    if isinstance(err, dict):
        info["type"] = err.get("type") or err.get("code")
        info["message"] = err.get("message")
    elif isinstance(body, str) and body.strip():
        info["message"] = body.strip()[:500]
    info["retry_after"] = header(headers, "retry-after")
    info["ratelimit"] = {k: v for k, v in (headers or {}).items()
                         if k.lower().startswith(("anthropic-ratelimit-", "x-ratelimit-"))}
    return info


# --------------------------------------------------------------------------
# The store: ring buffer + tool index + event bus


class Subscriber:
    def __init__(self):
        self.q: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE)
        self.overflow = False   # lost events: gets a fresh snapshot before anything else


def is_model_call(method: str, path: str, body) -> bool:
    """A POST carrying `messages` (or Codex `input` and `model`), not count_tokens."""
    if method != "POST" or not isinstance(body, dict) or "count_tokens" in path:
        return False
    return isinstance(body.get("messages"), list) or (isinstance(body.get("input"), list) and bool(body.get("model")))


def failure_message(ex: dict, stream_error, streamed: bool, final_seen: bool):
    """Why an exchange that got an answer still failed (§4), or None."""
    def describe(err):
        if isinstance(err, dict):
            return ": ".join(str(x) for x in (err.get("type") or err.get("code"), err.get("message")) if x)
        return str(err)
    if ex["status"] is not None and ex["status"] >= 400:
        info = ex["error_info"] or {}
        detail = describe({"type": info.get("type"), "message": info.get("message")})
        return f"HTTP {ex['status']}" + (f" {detail}" if detail else "")
    if stream_error is not None:
        return "stream error: " + (describe(stream_error) or "unknown")
    if streamed and not final_seen:
        return "stream ended without its final event"
    return None


class Store:
    """Every mutation and its event happen under one lock, so a browser that
    subscribes gets a snapshot plus exactly the events that came after it."""

    def __init__(self, size: int):
        self.lock = threading.RLock()
        self.size = size
        self.exchanges: "OrderedDict[int, dict]" = OrderedDict()
        self.tools: "OrderedDict[str, dict]" = OrderedDict()
        self.subscribers: list = []
        self.next_id = 1                 # never resets, not even on clear
        self.evicted = 0                 # exchanges dropped by the ring buffer since the last clear
        self.side: dict = {}             # id -> {"t0", "assembler", "hash", "raw", "stream_error", "final"}
        self.failed_by_hash: dict = {}   # request body hash -> nearest failed exchange not yet retried

    # -- event bus --------------------------------------------------------

    def publish(self, event: str, payload) -> None:
        """Never blocks. A subscriber whose queue is full stops receiving
        events; its connection then sends it a fresh snapshot (see resync)."""
        if not self.subscribers:
            return
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
        msg = f"event: {event}\ndata: {data}\n\n".encode("utf-8")
        for sub in self.subscribers:
            if sub.overflow:
                continue
            try:
                sub.q.put_nowait(msg)
            except queue.Full:
                sub.overflow = True

    def subscribe(self):
        with self.lock:
            sub = Subscriber()
            self.subscribers.append(sub)
            state = self._copy_state()
        return sub, state

    def resync(self, sub):
        """For a subscriber that lost events: drop what is queued, start over."""
        with self.lock:
            while True:
                try:
                    sub.q.get_nowait()
                except queue.Empty:
                    break
            sub.overflow = False
            return self._copy_state()

    def unsubscribe(self, sub) -> None:
        with self.lock:
            if sub in self.subscribers:
                self.subscribers.remove(sub)

    def _copy_state(self) -> dict:
        """A copy that later mutations cannot touch. Request bodies are never
        mutated after they are stored, so they are shared, not copied."""
        exchanges = [self._copy_one(ex) for ex in self.exchanges.values()]
        return {"exchanges": exchanges, "tools": {k: dict(v) for k, v in self.tools.items()},
                "buffer_size": self.size, "evicted": self.evicted}

    @staticmethod
    def _copy_one(ex: dict) -> dict:
        c = dict(ex)
        c["response_events"] = list(ex["response_events"])
        c["assembled"] = copy.deepcopy(ex["assembled"])
        c["timing"] = dict(ex["timing"])
        return c

    def export(self) -> dict:
        with self.lock:
            state = self._copy_state()
        state["exported_at"] = iso(time.time())
        return state

    def get(self, xid: int):
        with self.lock:
            ex = self.exchanges.get(xid)
            return self._copy_one(ex) if ex is not None else None

    def request_raw(self, xid: int):
        """The request body exactly as the harness sent it."""
        with self.lock:
            side = self.side.get(xid)
            return side["raw"] if side else None

    @guarded
    def clear(self) -> None:
        """In-flight exchanges leave the buffer too; their later events are
        ignored, and their forwarding is not touched."""
        with self.lock:
            self.exchanges.clear()
            self.tools.clear()
            self.side.clear()
            self.failed_by_hash.clear()
            self.evicted = 0
            self.publish("cleared", {})

    # -- exchange lifecycle (called from proxy threads) -----------------------

    @guarded
    def start(self, method: str, path: str, header_items, raw_body: bytes, t0: float):
        headers = redacted_headers(header_items)
        body = decode_body(raw_body, headers)
        body_hash = hashlib.sha1(raw_body).hexdigest() if raw_body else None
        with self.lock:
            xid = self.next_id
            self.next_id += 1
            ex = {
                "id": xid,
                "started_at": iso(t0),
                "ended_at": None,
                "method": method,
                "path": path,
                "status": None,
                "reason": None,
                "kind": "api" if raw_body else "check",
                "model_call": is_model_call(method, path, body),
                "request_headers": headers,
                "request_body": body,
                "response_headers": {},
                "response_events": [],
                "response_body": None,
                "assembled": {"content": []},
                "timing": {"ttfb_ms": None, "ttft_ms": None, "total_ms": None},
                "cost_usd": None,
                "error": None,
                "error_info": None,
                "retry_of": None,
                "retried_by": None,
            }
            self.side[xid] = {"t0": t0, "assembler": Assembler(), "hash": body_hash, "raw": raw_body,
                              "stream_error": None, "final": False}
            # A failed request is linked to one retry only: the nearest one.
            prev = self.failed_by_hash.pop(body_hash, None) if body_hash else None
            if prev in self.exchanges:
                ex["retry_of"] = prev
                self.exchanges[prev]["retried_by"] = xid
            self.exchanges[xid] = ex
            self._evict()
            self.publish("exchange_started", ex)
            if prev in self.exchanges:
                self.publish("exchange_meta", {"id": prev, "retried_by": xid})
            if ex["model_call"]:
                self._pair_tools(ex, t0)
        return xid

    def _evict(self) -> None:
        while len(self.exchanges) > self.size:
            old, _ = self.exchanges.popitem(last=False)
            self.side.pop(old, None)
            self.evicted += 1
            self.publish("exchange_evicted", {"id": old})
        live = self.exchanges
        for tid in [k for k, v in self.tools.items()
                    if v.get("call_exchange") not in live and v.get("result_exchange") not in live]:
            del self.tools[tid]

    def _pair_tools(self, ex: dict, t0: float) -> None:
        body = ex["request_body"]
        uses, results, results_in = {}, {}, {}
        for mi, kind, item in tool_items(body):
            if item.get("id") is None:
                continue
            if kind == "use":
                uses[item["id"]] = (mi, item)
            else:
                results.setdefault(item["id"], (mi, item))
                results_in.setdefault(mi, set()).add(item["id"])
        for tid, (mi, res) in results.items():
            entry = self.tools.get(tid)
            if entry is None:
                # The call came from before this session (or was evicted).
                use = uses.get(tid, (None, {}))[1]
                entry = {"id": tid, "name": use.get("name"), "input": use.get("input"),
                         "call_exchange": None, "call_ended_ms": None}
                self.tools[tid] = entry
            elif entry.get("status") == "paired":
                continue  # results are resent every turn; the first request pairs
            call_ended = entry.get("call_ended_ms")
            entry.update({
                "result_exchange": ex["id"],
                "result_message": mi,
                "result": res.get("content"),
                "is_error": res.get("is_error", False),
                "result_size": content_size(res.get("content")),
                "wait_ms": round(t0 * 1000 - call_ended) if call_ended else None,
                "status": "paired",
            })
            self.publish("tool_paired", {
                "tool_use_id": tid, "call_exchange": entry.get("call_exchange"),
                "result_exchange": ex["id"], "result": entry["result"], "entry": entry,
            })
        # No result: the call is in this history, and the message after it
        # doesn't answer it. A retry repeats an earlier body, so it doesn't count.
        if ex["retry_of"] is not None:
            return
        last = message_count(body) - 1
        for tid, (mi, _use) in uses.items():
            entry = self.tools.get(tid)
            if entry and entry.get("status") == "pending" and mi < last and tid not in results_in.get(mi + 1, ()):
                entry["status"] = "no_result"
                self.publish("tool_updated", {"tool_use_id": tid, "entry": entry})

    @guarded
    def response(self, xid, status: int, reason: str, header_items) -> None:
        if xid is None:
            return
        with self.lock:
            ex = self.exchanges.get(xid)
            if ex is None:
                return
            ex["status"] = status
            ex["reason"] = reason
            ex["response_headers"] = redacted_headers(header_items)
            ex["timing"]["ttfb_ms"] = round((time.time() - self.side[xid]["t0"]) * 1000)
            self.publish("exchange_response", {"id": xid, "status": status, "reason": reason,
                                               "headers": ex["response_headers"], "timing": ex["timing"]})

    @guarded
    def delta(self, xid, name: str, data: str) -> None:
        if xid is None:
            return
        try:
            parsed = json.loads(data)
        except ValueError:
            parsed = None
        event = {"event": name, "data": parsed if parsed is not None else data}
        with self.lock:
            ex = self.exchanges.get(xid)
            if ex is None:
                return
            side = self.side[xid]
            ex["response_events"].append(event)
            if name in FINAL_EVENTS or (isinstance(parsed, dict) and parsed.get("type") in FINAL_EVENTS):
                side["final"] = True
            asm = side["assembler"]
            had_token = asm.first_token
            patches = asm.feed(name, parsed, ex["assembled"]) if parsed is not None else []
            for p in patches:
                apply_patch(ex["assembled"], p)
                if p["op"] == "error":
                    side["stream_error"] = p["error"] if isinstance(p["error"], dict) else {"message": str(p["error"])}
            payload = {"id": xid, "event": event, "patches": patches}
            if asm.first_token and not had_token:
                ex["timing"]["ttft_ms"] = round((time.time() - side["t0"]) * 1000)
                payload["timing"] = ex["timing"]
            self.publish("exchange_delta", payload)

    def _close(self, ex: dict, t_end: float) -> None:
        """What every ending does: tidy the reply, stamp the time, price it."""
        asm = ex["assembled"]
        for b in asm.get("content") or []:   # a stream cut short leaves raw tool input
            if "_partial_json" in b:
                b["input"] = parse_tool_input(b.pop("_partial_json"))
        ex["ended_at"] = iso(t_end)
        ex["timing"]["total_ms"] = round((t_end - self.side[ex["id"]]["t0"]) * 1000)
        model = asm.get("model")
        if not model and isinstance(ex["request_body"], dict):
            model = ex["request_body"].get("model")
        ex["cost_usd"] = PRICING.cost(model, asm.get("usage"))

    def _ending(self, ex: dict) -> dict:
        return {"id": ex["id"], "ended_at": ex["ended_at"], "status": ex["status"], "assembled": ex["assembled"],
                "timing": ex["timing"], "cost_usd": ex["cost_usd"], "error_info": ex["error_info"],
                "response_body": ex["response_body"]}

    @guarded
    def finish(self, xid, raw_body, streamed: bool) -> None:
        """The upstream reply ended. Publishes exchange_done, or exchange_error
        if the reply was a failure (§4)."""
        if xid is None:
            return
        t_end = time.time()
        with self.lock:
            ex = self.exchanges.get(xid)
            if ex is None or ex["ended_at"]:
                return
            side = self.side[xid]
            if raw_body is not None:
                body = decode_body(raw_body, ex["response_headers"])
                ex["response_body"] = body
                if isinstance(body, dict) and body.get("type") == "message":  # non-streaming reply
                    ex["assembled"] = {k: body.get(k) for k in
                                       ("id", "model", "content", "stop_reason", "stop_sequence", "usage")}
                    ex["assembled"]["content"] = ex["assembled"]["content"] or []
            self._close(ex, t_end)
            ex["error_info"] = error_info(ex["status"], ex["response_headers"], ex["response_body"], side["stream_error"])
            ex["error"] = failure_message(ex, side["stream_error"], streamed, side["final"])
            if ex["error"] and ex["error_info"] is None:
                ex["error_info"] = {"status": ex["status"], "type": None, "message": ex["error"],
                                    "retry_after": None, "ratelimit": {}}
            self._track_failure(ex)
            if ex["error"]:
                self.publish("exchange_error", dict(self._ending(ex), message=ex["error"]))
                return
            self.publish("exchange_done", self._ending(ex))
            for b in ex["assembled"].get("content") or []:
                if b.get("type") == "tool_use" and b.get("id") and b["id"] not in self.tools:
                    entry = {"id": b["id"], "name": b.get("name"), "input": b.get("input"),
                             "call_exchange": xid, "call_ended_ms": round(t_end * 1000),
                             "result_exchange": None, "result_message": None, "result": None,
                             "is_error": False, "result_size": None, "wait_ms": None, "status": "pending"}
                    self.tools[b["id"]] = entry
                    self.publish("tool_updated", {"tool_use_id": b["id"], "entry": entry})

    @guarded
    def error(self, xid, message: str) -> None:
        """The proxy side failed: upstream unreachable or dropped, or the client
        went away. Does nothing if the exchange already ended."""
        if xid is None:
            return
        t_end = time.time()
        with self.lock:
            ex = self.exchanges.get(xid)
            if ex is None or ex["ended_at"]:
                return
            self._close(ex, t_end)
            ex["error"] = message
            self._track_failure(ex)
            self.publish("exchange_error", dict(self._ending(ex), message=message))

    def _track_failure(self, ex: dict) -> None:
        h = self.side.get(ex["id"], {}).get("hash")
        if h and ex["error"]:
            self.failed_by_hash[h] = ex["id"]


STORE = Store(BUFFER_SIZE)


# --------------------------------------------------------------------------
# The proxy (:8041). Same behavior and terminal output as proxy.py, except
# that cookies are redacted too and an upstream that fails before answering
# gets the client a 502.


def out(text: str) -> None:
    if not QUIET:
        sys.stdout.write(text)
        sys.stdout.flush()


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

    def relay(self) -> None:
        t0 = time.time()
        xid = None
        try:
            length = int(self.headers.get("content-length") or 0)
            body = self.rfile.read(length) if length else b""
            xid = STORE.start(self.command, self.path, self.headers.items(), body, t0)
            self.forward(xid, body)
        finally:
            # Every exchange ends exactly once; this catches anything unexpected.
            # (STORE.error does nothing if it already ended.)
            exc = sys.exc_info()[1]
            STORE.error(xid, f"proxy failed: {exc!r}" if exc else "proxy stopped before the reply ended")

    def forward(self, xid, body: bytes) -> None:
        # 1. Print what the agent sent.
        if not QUIET:
            print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
            for name, value in self.headers.items():
                print(f"    {name}: {redact(name, value)}")
            if body:
                try:
                    print(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
                except ValueError:
                    print(body.decode("utf-8", "replace"))

        # 2. Forward it, headers intact.
        target = urlsplit(UPSTREAM)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
        except OSError as e:
            out(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}\n")
            STORE.error(xid, f"upstream unreachable: {e}")
            self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            self.close_connection = True
            return
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        try:
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()
        except (OSError, http.client.HTTPException) as e:
            out(f"\n[{stamp()}] !!! upstream failed: {e}\n")
            STORE.error(xid, f"upstream failed before answering: {e}")
            self.send_error(502, f"proxy: upstream failed: {e}")
            self.close_connection = True
            return

        # 3. Stream the reply back as it arrives, and publish it on the way.
        out(f"\n[{stamp()}] <<< {reply.status} {reply.reason}\n")
        STORE.response(xid, reply.status, reply.reason, reply.getheaders())
        self.send_response(reply.status, reply.reason)
        for name, value in reply.getheaders():
            if name.lower() not in SKIP and name.lower() != "content-encoding":
                self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command == "HEAD":
            STORE.finish(xid, None, streamed=False)
            return

        is_sse = "text/event-stream" in (reply.getheader("content-type") or "")
        parser = SSEParser() if is_sse else None
        raw = bytearray()
        while True:
            try:
                chunk = reply.read1(4096)  # pass on each chunk as it lands
            except (OSError, http.client.HTTPException) as e:
                STORE.error(xid, f"upstream stream broke: {e}")
                return
            if not chunk:
                break
            try:
                self.wfile.write(chunk)
                self.wfile.flush()
            except OSError:
                STORE.error(xid, "client disconnected")
                return
            out(chunk.decode("utf-8", "replace"))
            self.observe(xid, parser, raw, chunk)
        if parser:
            self.observe(xid, parser, raw, None)
        STORE.finish(xid, None if parser else bytes(raw), streamed=bool(parser))
        out(f"\n[{stamp()}] --- done\n")

    @staticmethod
    def observe(xid, parser, raw: bytearray, chunk) -> None:
        """Parse a chunk for the dashboard. Never raises."""
        try:
            if parser is None:
                raw.extend(chunk)
                return
            events = parser.feed(chunk) if chunk is not None else parser.flush()
            for name, data in events:
                STORE.delta(xid, name, data)
        except Exception as e:  # noqa: BLE001
            warn(f"could not parse upstream stream: {e}")


# --------------------------------------------------------------------------
# The dashboard (:8042)


class UI(BaseHTTPRequestHandler):
    ui_port = 8042

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def local_only(self, check_origin: bool = False) -> bool:
        """Reject other sites: a non-localhost Host (DNS rebinding), and a
        non-localhost Origin on POST (a form posted from another page)."""
        host = (self.headers.get("host") or "").split(":")[0]
        origin = self.headers.get("origin")
        ok_host = host in ("localhost", "127.0.0.1", "[::1]")
        ok_origin = not check_origin or origin is None or re.match(r"^http://(localhost|127\.0\.0\.1)(:\d+)?$", origin)
        if ok_host and ok_origin:
            return True
        self.send_error(403, "dashboard is local only")
        return False

    def send_json(self, obj, status=200, extra=None) -> None:
        data = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if not self.local_only():
            return
        path = urlsplit(self.path).path
        try:
            if path in ("/", "/index.html"):
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)
            elif path == "/events":
                self.events()
            elif re.match(r"^/api/exchanges/\d+/request$", path):
                raw = STORE.request_raw(int(path.split("/")[3]))
                if raw is None:
                    self.send_json({"error": "not found"}, 404)
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.send_header("Content-Length", str(len(raw)))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(raw)
            elif re.match(r"^/api/exchanges/\d+$", path):
                ex = STORE.get(int(path.rsplit("/", 1)[1]))
                if ex is None:
                    self.send_json({"error": "not found"}, 404)
                else:
                    self.send_json(ex)
            elif path == "/api/export":
                name = f"traffic-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
                self.send_json(STORE.export(), extra={"Content-Disposition": f'attachment; filename="{name}"'})
            elif path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
            else:
                self.send_json({"error": "not found"}, 404)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self) -> None:
        if not self.local_only(check_origin=True):
            return
        if urlsplit(self.path).path == "/api/clear":
            STORE.clear()
            self.send_json({"ok": True})
        else:
            self.send_json({"error": "not found"}, 404)

    def events(self) -> None:
        sub, state = STORE.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(b"retry: 1000\n\n")
            while True:
                if state is not None:
                    snapshot = json.dumps(state, ensure_ascii=False, separators=(",", ":"), default=str)
                    state = None
                    self.wfile.write(b"event: snapshot\ndata: " + snapshot.encode("utf-8") + b"\n\n")
                    self.wfile.flush()
                if sub.overflow:
                    state = STORE.resync(sub)  # it lost events: start it over
                    continue
                try:
                    msg = sub.q.get(timeout=15)
                except queue.Empty:
                    msg = b": ping\n\n"
                self.wfile.write(msg)
                self.wfile.flush()
        except OSError:
            pass  # the browser went away
        finally:
            STORE.unsubscribe(sub)
            self.close_connection = True


class Server(ThreadingHTTPServer):
    daemon_threads = True


def main() -> None:
    global UPSTREAM, QUIET
    parser = argparse.ArgumentParser(description="Forward an agent's API traffic and show it live in a browser.")
    parser.add_argument("--port", type=int, default=8041, help="proxy port (default 8041)")
    parser.add_argument("--ui-port", type=int, default=8042, help="dashboard port (default 8042)")
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--quiet", action="store_true", help="don't print traffic (startup lines still print)")
    parser.add_argument("--buffer", type=int, default=BUFFER_SIZE, help="exchanges kept in memory (default 200)")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    QUIET = args.quiet
    STORE.size = max(1, args.buffer)
    UI.ui_port = args.ui_port

    proxy = Server(("127.0.0.1", args.port), Tap)
    ui = Server(("127.0.0.1", args.ui_port), UI)
    threading.Thread(target=ui.serve_forever, daemon=True).start()
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"dashboard on       http://localhost:{args.ui_port}")
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude", flush=True)
    try:
        proxy.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
