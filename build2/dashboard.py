#!/usr/bin/env python3
"""dashboard.py: the agent-traffic proxy plus a live browser dashboard (Lab 4).

One process, two ports, standard library only:

    :8041  the proxy. Forwards traffic unchanged, exactly like proxy.py.
    :8042  the dashboard. Serves dashboard.html and a Server-Sent Events stream.

    python3 dashboard.py                                  # then open http://localhost:8042
    ANTHROPIC_BASE_URL=http://localhost:8041 claude

    python3 dashboard.py --port 9000 --ui-port 9001 --quiet
    python3 dashboard.py --upstream https://chatgpt.com/backend-api/codex   # for Codex

The dashboard never delays forwarding: every chunk is written to the agent
first, and only then parsed and published. Publishing never blocks: each
browser has a bounded queue, and when it is full that browser's events are
dropped (the browser is then re-synced with a fresh snapshot).
"""

from __future__ import annotations

import argparse
import codecs
import copy
import hashlib
import http.client
import json
import os
import queue
import re
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))
HTML_PATH = os.path.join(HERE, "dashboard.html")
PRICING_PATH = os.path.join(HERE, "pricing.json")

# Same hop-by-hop list as proxy.py. `accept-encoding` is dropped so the API
# answers in plain text.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}
# Headers whose values are secrets. Stored/printed as the first 16 chars + "...".
REDACT = {"authorization", "x-api-key", "cookie", "set-cookie"}

UPSTREAM = "https://api.anthropic.com"
QUIET = False
MAX_EXCHANGES = 200
MAX_TOOLS = 2000
SUBSCRIBER_QUEUE = 5000
MAX_PLAIN_BODY = 8 * 1024 * 1024  # non-SSE response bodies kept up to this size


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def say(text: str) -> None:
    if not QUIET:
        print(text, flush=True)


def redact(value: str) -> str:
    return value[:16] + "..."


def header_dict(items) -> dict:
    """Header list -> dict, secrets redacted, repeated names joined with ', '."""
    out: dict = {}
    for name, value in items:
        if name.lower() in REDACT:
            value = redact(value)
        out[name] = f"{out[name]}, {value}" if name in out else value
    return out


def safe(fn, *args, **kwargs):
    """Run a dashboard hook; a bug in the dashboard must never break forwarding."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001
        sys.stderr.write(f"[dashboard] internal error in {getattr(fn, '__name__', fn)}: {e!r}\n")
        return None


# --------------------------------------------------------------------------- #
# Event bus
# --------------------------------------------------------------------------- #

class Subscriber:
    def __init__(self) -> None:
        self.q: queue.Queue = queue.Queue(SUBSCRIBER_QUEUE)
        self.lagged = False


class EventBus:
    """Thread-safe fan-out. publish() never blocks."""

    def __init__(self) -> None:
        self._subs: set = set()
        self._lock = threading.Lock()
        self.dropped = 0

    def add(self, sub: Subscriber) -> None:
        with self._lock:
            self._subs.add(sub)

    def remove(self, sub: Subscriber) -> None:
        with self._lock:
            self._subs.discard(sub)

    def publish(self, event: str, payload) -> None:
        if not self._subs:  # nobody watching: skip serialisation entirely
            return
        with self._lock:
            subs = list(self._subs)
        if not subs:
            return
        msg = sse_message(event, payload)
        for sub in subs:
            try:
                sub.q.put_nowait(msg)
            except queue.Full:
                sub.lagged = True
                self.dropped += 1


def sse_message(event: str, payload) -> bytes:
    data = json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":"))
    return f"event: {event}\ndata: {data}\n\n".encode("utf-8")


# --------------------------------------------------------------------------- #
# Upstream stream parsing
# --------------------------------------------------------------------------- #

class SSEParser:
    """Incremental text/event-stream parser. Never raises on bad data."""

    def __init__(self) -> None:
        self.buf = ""
        self.decoder = codecs.getincrementaldecoder("utf-8")("replace")

    def feed(self, chunk: bytes) -> list:
        self.buf += self.decoder.decode(chunk)
        if "\r" in self.buf:
            self.buf = self.buf.replace("\r\n", "\n")
        events = []
        while "\n\n" in self.buf:
            block, self.buf = self.buf.split("\n\n", 1)
            ev = self._parse(block)
            if ev is not None:
                events.append(ev)
        return events

    def flush(self) -> list:
        rest = self.buf + self.decoder.decode(b"", final=True)
        self.buf = ""
        ev = self._parse(rest.replace("\r\n", "\n")) if rest.strip() else None
        return [ev] if ev is not None else []

    @staticmethod
    def _parse(block: str):
        name, data_lines = None, []
        for line in block.split("\n"):
            if not line or line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "event":
                name = value
            elif field == "data":
                data_lines.append(value)
        if name is None and not data_lines:
            return None
        raw = "\n".join(data_lines)
        try:
            data = json.loads(raw) if raw else None
            return {"event": name or "message", "data": data}
        except ValueError:
            # Unparseable: keep it as a raw string, never interrupt forwarding.
            return {"event": name or "message", "data": raw, "parse_error": True}


def parse_json_maybe(s):
    if not isinstance(s, str):
        return s
    try:
        return json.loads(s) if s.strip() else {}
    except ValueError:
        return s


class Assembler:
    """Builds `assembled` (model, content, stop_reason, usage) from SSE events.

    Handles the Anthropic Messages stream, and (best effort) the OpenAI
    Responses stream that the Codex upstream sends.
    """

    def __init__(self) -> None:
        self.a: dict = {"model": None, "content": [], "stop_reason": None, "usage": {}}
        self.partial: dict = {}  # content index -> partial tool input JSON
        self.item_index: dict = {}  # codex item_id -> content index

    def _block(self, idx: int) -> dict:
        content = self.a["content"]
        while len(content) <= idx:
            content.append({"type": "text", "text": ""})
        return content[idx]

    def apply(self, name: str, data) -> bool:
        """Apply one event. Returns True if it carried model output (for TTFT)."""
        if not isinstance(data, dict):
            return False
        t = data.get("type") or name
        a = self.a
        if t == "message_start":
            m = data.get("message") or {}
            a["model"] = m.get("model") or a["model"]
            a["id"] = m.get("id")
            a["usage"].update({k: v for k, v in (m.get("usage") or {}).items() if v is not None})
        elif t == "content_block_start":
            idx = int(data.get("index", len(a["content"])))
            block = dict(data.get("content_block") or {})
            if block.get("type") in ("tool_use", "server_tool_use", "mcp_tool_use"):
                self.partial[idx] = ""
                block["input"] = {}
            self._block(idx)
            a["content"][idx] = block
        elif t == "content_block_delta":
            idx = int(data.get("index", 0))
            block = self._block(idx)
            d = data.get("delta") or {}
            dt = d.get("type")
            if dt == "text_delta":
                block["text"] = (block.get("text") or "") + d.get("text", "")
            elif dt == "thinking_delta":
                block["thinking"] = (block.get("thinking") or "") + d.get("thinking", "")
            elif dt == "signature_delta":
                block["signature"] = d.get("signature")
            elif dt == "input_json_delta":
                self.partial[idx] = self.partial.get(idx, "") + d.get("partial_json", "")
            elif dt == "citations_delta":
                block.setdefault("citations", []).append(d.get("citation"))
            return True
        elif t == "content_block_stop":
            self._close(int(data.get("index", 0)))
        elif t == "message_delta":
            d = data.get("delta") or {}
            if d.get("stop_reason") is not None:
                a["stop_reason"] = d["stop_reason"]
            if d.get("stop_sequence") is not None:
                a["stop_sequence"] = d["stop_sequence"]
            a["usage"].update({k: v for k, v in (data.get("usage") or {}).items() if v is not None})
        elif t == "error":
            a["error"] = data.get("error") or data
        # ---- OpenAI Responses API (Codex upstream), best effort ----
        elif t in ("response.created", "response.in_progress"):
            r = data.get("response") or {}
            a["model"] = r.get("model") or a["model"]
        elif t == "response.output_text.delta":
            key = data.get("item_id") or "_text"
            if key not in self.item_index:
                self.item_index[key] = len(a["content"])
                a["content"].append({"type": "text", "text": ""})
            blk = a["content"][self.item_index[key]]
            blk["text"] = blk.get("text", "") + (data.get("delta") or "")
            return True
        elif t == "response.output_item.done":
            item = data.get("item") or {}
            it = item.get("type")
            if it == "function_call":
                a["content"].append({"type": "tool_use", "id": item.get("call_id") or item.get("id"),
                                     "name": item.get("name"), "input": parse_json_maybe(item.get("arguments"))})
            elif it == "reasoning":
                summ = "\n\n".join(s.get("text", "") for s in item.get("summary") or [] if isinstance(s, dict))
                a["content"].append({"type": "thinking", "thinking": summ})
            elif it == "message" and item.get("id") not in self.item_index:
                text = "".join(c.get("text", "") for c in item.get("content") or [] if isinstance(c, dict))
                a["content"].append({"type": "text", "text": text})
        elif t in ("response.completed", "response.incomplete", "response.failed"):
            r = data.get("response") or {}
            a["model"] = r.get("model") or a["model"]
            u = r.get("usage") or {}
            if u:
                cached = (u.get("input_tokens_details") or {}).get("cached_tokens") or 0
                a["usage"] = {"input_tokens": (u.get("input_tokens") or 0) - cached,
                              "output_tokens": u.get("output_tokens") or 0,
                              "cache_read_input_tokens": cached,
                              "cache_creation_input_tokens": 0}
                a["usage_raw"] = u
            has_tool = any(b.get("type") == "tool_use" for b in a["content"])
            a["stop_reason"] = "tool_use" if has_tool else (r.get("status") or t)
            if r.get("error"):
                a["error"] = r["error"]
        return False

    def _close(self, idx: int) -> None:
        if idx in self.partial:
            raw = self.partial.pop(idx)
            block = self._block(idx)
            try:
                block["input"] = json.loads(raw) if raw.strip() else {}
            except ValueError:
                block["input"] = {}
                block["input_raw"] = raw

    def finish(self) -> None:
        for idx in list(self.partial):
            self._close(idx)


# --------------------------------------------------------------------------- #
# Pricing
# --------------------------------------------------------------------------- #

class Pricing:
    """pricing.json, reloaded whenever the file changes. USD per million tokens."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.mtime = None
        self.models: dict = {}

    def _reload(self) -> None:
        try:
            m = os.path.getmtime(self.path)
        except OSError:
            self.models = {}
            return
        if m != self.mtime:
            self.mtime = m
            try:
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                self.models = data.get("models", data) if isinstance(data, dict) else {}
            except (OSError, ValueError) as e:
                sys.stderr.write(f"[dashboard] cannot read pricing.json: {e}\n")
                self.models = {}

    def lookup(self, model):
        if not model:
            return None
        self._reload()
        model = re.sub(r"\[.*\]$", "", model)  # e.g. "claude-...[1m]"
        for key in (model, re.sub(r"-\d{8}$", "", model)):
            p = self.models.get(key)
            if isinstance(p, dict):
                return p
        return None

    def cost(self, model, usage):
        p = self.lookup(model)
        if p is None or not usage:
            return None
        u = lambda k: usage.get(k) or 0  # noqa: E731
        total = (u("input_tokens") * p.get("input", 0) + u("output_tokens") * p.get("output", 0)
                 + u("cache_read_input_tokens") * p.get("cache_read", 0)
                 + u("cache_creation_input_tokens") * p.get("cache_write", 0))
        return round(total / 1_000_000, 6)


# --------------------------------------------------------------------------- #
# Request analysis helpers
# --------------------------------------------------------------------------- #

def request_parts(body):
    """(system, messages) for Anthropic bodies, or (instructions, input) for Codex."""
    if not isinstance(body, dict):
        return None, None
    system = body.get("system", body.get("instructions"))
    msgs = body.get("messages")
    if msgs is None and isinstance(body.get("input"), list):
        msgs = body["input"]
    return system, (msgs if isinstance(msgs, list) else None)


def iter_blocks(msgs):
    """Yield (message index, role, block) with Codex items mapped to Anthropic shapes."""
    for i, m in enumerate(msgs or []):
        if not isinstance(m, dict):
            continue
        t = m.get("type")
        if t == "function_call":
            yield i, "assistant", {"type": "tool_use", "id": m.get("call_id"), "name": m.get("name"),
                                   "input": parse_json_maybe(m.get("arguments"))}
        elif t == "function_call_output":
            yield i, "user", {"type": "tool_result", "tool_use_id": m.get("call_id"), "content": m.get("output")}
        else:
            c = m.get("content")
            if isinstance(c, list):
                for b in c:
                    if isinstance(b, dict):
                        yield i, m.get("role"), b


def result_size(content) -> int:
    if content is None:
        return 0
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        n = 0
        for b in content:
            if isinstance(b, dict) and b.get("type") == "text":
                n += len(b.get("text") or "")
            else:
                n += len(json.dumps(b, ensure_ascii=False))
        return n
    return len(json.dumps(content, ensure_ascii=False))


def strip_cache_control(o):
    if isinstance(o, dict):
        return {k: strip_cache_control(v) for k, v in o.items() if k != "cache_control"}
    if isinstance(o, list):
        return [strip_cache_control(v) for v in o]
    return o


def fingerprint(o) -> str:
    s = json.dumps(strip_cache_control(o), sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def parse_ms(iso: str) -> float:
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000
    except (ValueError, AttributeError):
        return 0.0


# --------------------------------------------------------------------------- #
# Store: ring buffer of exchanges + tool-pair index
# --------------------------------------------------------------------------- #

class Store:
    def __init__(self, bus: EventBus, pricing: Pricing, max_exchanges: int = MAX_EXCHANGES) -> None:
        self.bus = bus
        self.pricing = pricing
        self.max = max_exchanges
        self.lock = threading.RLock()
        self.ex: OrderedDict = OrderedDict()
        self.meta: dict = {}
        self.tools: OrderedDict = OrderedDict()
        self.failed_by_key: dict = {}
        self.next_id = 1
        self.tool_seq = 0

    # -- helpers (call with lock held) --
    def _new_tool(self, tid, name, inp, call_exchange, call_ended_at=None) -> dict:
        self.tool_seq += 1
        e = {"tool_use_id": tid, "name": name, "input": inp, "call_exchange": call_exchange,
             "call_ended_at": call_ended_at, "result_exchange": None, "result": None, "is_error": False,
             "status": "pending", "result_size": None, "latency_ms": None, "seq": self.tool_seq}
        self.tools[tid] = e
        while len(self.tools) > MAX_TOOLS:
            self.tools.popitem(last=False)
        return e

    def _evict(self) -> None:
        while len(self.ex) > self.max:
            old_id, _ = self.ex.popitem(last=False)
            self.meta.pop(old_id, None)
            for k in [k for k, v in self.failed_by_key.items() if v == old_id]:
                del self.failed_by_key[k]

    # -- proxy hooks --
    def begin(self, method, path, header_items, body: bytes, started_at: str, mono0: float):
        if body:
            try:
                parsed = json.loads(body)
            except ValueError:
                parsed = body.decode("utf-8", "replace")
        else:
            parsed = None
        system, msgs = request_parts(parsed)
        sys_fp = fingerprint(system) if msgs is not None else None
        msg_fps = [fingerprint(m) for m in msgs] if msgs is not None else None
        retry_key = hashlib.sha1(method.encode() + b" " + path.encode() + b"\n" + body).hexdigest() if body else None

        out = []
        with self.lock:
            xid = self.next_id
            self.next_id += 1
            ex = {
                "id": xid, "started_at": started_at, "ended_at": None, "method": method, "path": path,
                "status": None, "request_headers": header_dict(header_items), "request_body": parsed,
                "response_headers": None, "response_events": [],
                "assembled": None,
                "timing": {"ttfb_ms": None, "ttft_ms": None, "total_ms": None},
                "cost_usd": None, "error": None,
                # additions beyond the spec's data model:
                "kind": "api" if body else "check",
                "reason": None, "response_body": None, "error_info": None,
                "retry_of": None, "retried_by": None, "conversation": None,
            }
            asm = Assembler()
            ex["assembled"] = asm.a
            # Retry linking: identical body to an earlier failed request.
            if retry_key and retry_key in self.failed_by_key:
                prev = self.ex.get(self.failed_by_key.pop(retry_key))
                if prev is not None:
                    ex["retry_of"] = prev["id"]
                    prev["retried_by"] = xid
                    out.append(("exchange_retried", {"id": prev["id"], "retried_by": xid}))
            # Conversation: newest earlier request with the same system prompt whose
            # messages are a prefix of ours (cache_control markers ignored).
            if msg_fps is not None:
                best, best_len = None, 0
                for oid in reversed(self.ex):
                    m = self.meta.get(oid) or {}
                    if m.get("sys") != sys_fp or not m.get("msgs"):
                        continue
                    theirs = m["msgs"]
                    n = 0
                    for a, b in zip(theirs, msg_fps):
                        if a != b:
                            break
                        n += 1
                    if n == len(theirs) and n > 0:
                        best, best_len = oid, n
                        break
                    if n > best_len:
                        best, best_len = oid, n
                prev_count = len(self.meta[best]["msgs"]) if best is not None else 0
                ex["conversation"] = {"prev_id": best, "common_prefix": best_len, "prev_count": prev_count,
                                      "message_count": len(msg_fps)}
            self.ex[xid] = ex
            self.meta[xid] = {"mono0": mono0, "sys": sys_fp, "msgs": msg_fps, "key": retry_key,
                              "asm": asm, "body": bytearray(), "sse": False}
            self._evict()
            started_payload = dict(ex)
            started_payload["assembled"] = copy.deepcopy(ex["assembled"])
            out.insert(0, ("exchange_started", started_payload))

            # Tool pairing.
            if msgs:
                uses, results = {}, {}
                for _i, _role, b in iter_blocks(msgs):
                    if b.get("type") == "tool_use" and b.get("id"):
                        uses[b["id"]] = b
                    elif b.get("type") == "tool_result" and b.get("tool_use_id"):
                        results[b["tool_use_id"]] = b
                for tid, rb in results.items():
                    e = self.tools.get(tid)
                    if e is None:
                        ub = uses.get(tid) or {}
                        e = self._new_tool(tid, ub.get("name"), ub.get("input"), None)
                    if e["status"] == "paired":
                        continue
                    e.update(result_exchange=xid, result=rb.get("content"), is_error=bool(rb.get("is_error")),
                             status="paired", result_size=result_size(rb.get("content")))
                    if e.get("call_ended_at"):
                        e["latency_ms"] = max(0, round(parse_ms(started_at) - parse_ms(e["call_ended_at"])))
                    out.append(("tool_paired", {"tool_use_id": tid, "call_exchange": e["call_exchange"],
                                                "result_exchange": xid, "result": e["result"],
                                                "is_error": e["is_error"], "entry": dict(e)}))
                for tid in uses:
                    if tid in results:
                        continue
                    e = self.tools.get(tid)
                    if e is not None and e["status"] == "pending":
                        e["status"] = "no_result"
                        out.append(("tool_update", {"tool_use_id": tid, "entry": dict(e)}))
        for ev, payload in out:
            self.bus.publish(ev, payload)
        return xid

    def response(self, xid, status, reason, header_items, ttfb_ms, content_type: str) -> None:
        if xid is None:
            return
        with self.lock:
            ex = self.ex.get(xid)
            if ex is None:
                return
            ex["status"] = status
            ex["reason"] = reason
            ex["response_headers"] = header_dict(header_items)
            ex["timing"]["ttfb_ms"] = ttfb_ms
            self.meta[xid]["sse"] = "text/event-stream" in (content_type or "")
            payload = {"id": xid, "status": status, "reason": reason, "headers": ex["response_headers"],
                       "ttfb_ms": ttfb_ms}
        self.bus.publish("exchange_response", payload)

    def feed(self, xid, chunk: bytes, parser) -> None:
        if xid is None:
            return
        events = parser.feed(chunk) if parser is not None else None
        self._apply(xid, chunk, events)

    def _apply(self, xid, chunk, events) -> None:
        out = []
        with self.lock:
            ex, meta = self.ex.get(xid), self.meta.get(xid)
            if ex is None:
                return
            now = time.monotonic()
            if events is None:
                body = meta["body"]
                if len(body) < MAX_PLAIN_BODY:
                    body.extend(chunk[: MAX_PLAIN_BODY - len(body)])
                return
            for ev in events:
                ev["t"] = round((now - meta["mono0"]) * 1000)
                n = len(ex["response_events"])
                ex["response_events"].append(ev)
                if meta["asm"].apply(ev["event"], ev["data"]) and ex["timing"]["ttft_ms"] is None:
                    ex["timing"]["ttft_ms"] = ev["t"]
                out.append({"id": xid, "n": n, "event": ev, "ttft_ms": ex["timing"]["ttft_ms"]})
        for p in out:
            self.bus.publish("exchange_delta", p)

    def finish(self, xid, parser=None, error: str = None, status: int = None, client_drop: bool = False) -> None:
        if xid is None:
            return
        if parser is not None:
            rest = parser.flush()
            if rest:
                self._apply(xid, b"", rest)
        out = []
        with self.lock:
            ex, meta = self.ex.get(xid), self.meta.get(xid)
            if ex is None:
                return
            meta["asm"].finish()
            ex["ended_at"] = iso_now()
            ex["timing"]["total_ms"] = round((time.monotonic() - meta["mono0"]) * 1000)
            if status is not None and ex["status"] is None:
                ex["status"] = status
            if error:
                ex["error"] = error
            a = ex["assembled"]
            body = bytes(meta.get("body") or b"")
            if body:
                text = body.decode("utf-8", "replace")
                try:
                    rb = json.loads(text)
                except ValueError:
                    rb = text
                ex["response_body"] = rb
                if isinstance(rb, dict) and rb.get("type") == "message":
                    a.update(model=rb.get("model"), content=rb.get("content") or [],
                             stop_reason=rb.get("stop_reason"), usage=rb.get("usage") or {}, id=rb.get("id"))
            meta["body"] = None
            if not a.get("model") and isinstance(ex["request_body"], dict):
                a["model"] = ex["request_body"].get("model")
            ex["error_info"] = self._error_info(ex)
            if a.get("usage"):
                ex["cost_usd"] = self.pricing.cost(a.get("model"), a["usage"])
            failed = (ex["status"] or 0) >= 400 or (error and not client_drop)
            if failed and meta.get("key"):
                self.failed_by_key[meta["key"]] = xid
            if not error:
                for b in a.get("content") or []:
                    if isinstance(b, dict) and b.get("type") == "tool_use" and b.get("id") and b["id"] not in self.tools:
                        e = self._new_tool(b["id"], b.get("name"), b.get("input"), xid, ex["ended_at"])
                        out.append(("tool_update", {"tool_use_id": b["id"], "entry": dict(e)}))
            payload = {"id": xid, "ended_at": ex["ended_at"], "assembled": copy.deepcopy(a), "status": ex["status"],
                       "timing": dict(ex["timing"]), "cost_usd": ex["cost_usd"], "error_info": ex["error_info"],
                       "response_body": ex["response_body"], "event_count": len(ex["response_events"])}
            if error:
                payload["message"] = error
                payload["error"] = error
                out.insert(0, ("exchange_error", payload))
            else:
                out.insert(0, ("exchange_done", payload))
        for ev, p in out:
            self.bus.publish(ev, p)

    @staticmethod
    def _error_info(ex):
        status = ex.get("status") or 0
        info = None
        if status >= 400:
            info = {"status": status, "type": None, "message": None}
            rb = ex.get("response_body")
            err = rb.get("error") if isinstance(rb, dict) else None
            if isinstance(err, dict):
                info["type"] = err.get("type")
                info["message"] = err.get("message")
            elif isinstance(err, str):
                info["message"] = err
            elif isinstance(rb, dict):
                info["message"] = rb.get("message") or rb.get("detail")
            elif isinstance(rb, str) and rb.strip():
                info["message"] = rb.strip()[:500]
            if not info["type"]:
                info["type"] = ex.get("reason")
        se = (ex.get("assembled") or {}).get("error")
        if isinstance(se, dict):
            info = info or {"status": status}
            info["type"] = se.get("type") or info.get("type")
            info["message"] = se.get("message") or info.get("message")
            info["in_stream"] = True
        if info is not None:
            hdrs = {k.lower(): v for k, v in (ex.get("response_headers") or {}).items()}
            info["retry_after"] = hdrs.get("retry-after")
            info["ratelimit"] = {k: v for k, v in hdrs.items() if k.startswith("anthropic-ratelimit-")}
        return info

    # -- dashboard side --
    def _light(self, ex) -> dict:
        d = {k: v for k, v in ex.items() if k != "response_events"}
        d["event_count"] = len(ex["response_events"])
        d["events_omitted"] = True
        if ex["ended_at"] is None:
            d["assembled"] = copy.deepcopy(ex["assembled"])
            d["timing"] = dict(ex["timing"])
        return d

    def snapshot_and_subscribe(self, sub: Subscriber) -> bytes:
        with self.lock:
            exchanges = [self._light(ex) for ex in self.ex.values()]
            tools = {k: dict(v) for k, v in self.tools.items()}
            while True:  # drain anything stale (lag resync)
                try:
                    sub.q.get_nowait()
                except queue.Empty:
                    break
            sub.lagged = False
            self.bus.add(sub)
        return sse_message("snapshot", {"exchanges": exchanges, "tools": tools, "max_exchanges": self.max,
                                        "upstream": UPSTREAM, "server_time": iso_now()})

    def get(self, xid):
        with self.lock:
            ex = self.ex.get(xid)
            if ex is None:
                return None
            return json.dumps(ex, ensure_ascii=False, default=str).encode("utf-8")

    def export(self) -> bytes:
        with self.lock:
            data = {"exported_at": iso_now(), "upstream": UPSTREAM, "exchanges": list(self.ex.values()),
                    "tools": dict(self.tools)}
            return json.dumps(data, ensure_ascii=False, default=str, indent=1).encode("utf-8")

    def clear(self) -> None:
        with self.lock:
            # in-flight exchanges keep streaming into their (now detached) dicts
            self.ex.clear()
            self.meta.clear()
            self.tools.clear()
            self.failed_by_key.clear()
        self.bus.publish("cleared", {"at": iso_now()})


BUS = EventBus()
STORE = Store(BUS, Pricing(PRICING_PATH))


# --------------------------------------------------------------------------- #
# The proxy (:8041). Same behaviour and terminal output as proxy.py.
# --------------------------------------------------------------------------- #

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
        mono0 = time.monotonic()
        started_at = iso_now()
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""

        # 1. Print what the agent sent (unless --quiet).
        if not QUIET:
            print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
            for name, value in self.headers.items():
                if name.lower() in REDACT:
                    value = redact(value)
                print(f"    {name}: {value}")
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
            say(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}")
            xid = safe(STORE.begin, self.command, self.path, self.headers.items(), body, started_at, mono0)
            self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            self.close_connection = True
            safe(STORE.finish, xid, None, f"upstream unreachable: {e}", 502)
            return
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        xid = None
        try:
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            # Record the exchange while the upstream is thinking: costs the agent nothing.
            xid = safe(STORE.begin, self.command, self.path, self.headers.items(), body, started_at, mono0)
            reply = upstream.getresponse()
        except (OSError, http.client.HTTPException) as e:
            if xid is None:
                xid = safe(STORE.begin, self.command, self.path, self.headers.items(), body, started_at, mono0)
            say(f"\n[{stamp()}] !!! upstream failed: {e!r}")
            try:
                self.send_error(502, f"proxy: upstream failed: {e!r}")
            except OSError:
                pass
            self.close_connection = True
            safe(STORE.finish, xid, None, f"upstream failed: {e!r}", 502)
            return
        ttfb = round((time.monotonic() - mono0) * 1000)
        ctype = reply.getheader("content-type", "") or ""
        safe(STORE.response, xid, reply.status, reply.reason, reply.getheaders(), ttfb, ctype)

        # 3. Stream the reply back as it arrives, printing it on the way.
        say(f"\n[{stamp()}] <<< {reply.status} {reply.reason}")
        try:
            self.send_response(reply.status, reply.reason)
            for name, value in reply.getheaders():
                if name.lower() not in SKIP and name.lower() != "content-encoding":
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
        except OSError as e:
            self.close_connection = True
            safe(STORE.finish, xid, None, f"client disconnected: {e!r}", None, True)
            return
        if self.command == "HEAD":
            self.close_connection = True
            safe(STORE.finish, xid)
            return
        parser = SSEParser() if "text/event-stream" in ctype else None
        error, client_drop = None, False
        while True:
            try:
                chunk = reply.read1(4096)  # read1: pass on each chunk as it lands
            except (OSError, http.client.HTTPException) as e:
                error = f"upstream stream failed: {e!r}"
                break
            if not chunk:
                break
            try:
                self.wfile.write(chunk)
                self.wfile.flush()
            except OSError as e:
                error, client_drop = f"client disconnected: {e!r}", True
                break
            if not QUIET:
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
            safe(STORE.feed, xid, chunk, parser)
        self.close_connection = True
        upstream.close()
        if error:
            say(f"\n[{stamp()}] !!! {error}")
        else:
            say(f"\n[{stamp()}] --- done")
        safe(STORE.finish, xid, parser, error, None, client_drop)


# --------------------------------------------------------------------------- #
# The dashboard server (:8042)
# --------------------------------------------------------------------------- #

class UI(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def _send(self, code: int, body: bytes, ctype: str, extra: dict = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, code: int, obj) -> None:
        self._send(code, json.dumps(obj).encode(), "application/json; charset=utf-8")

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        try:
            if path in ("/", "/index.html", "/dashboard.html"):
                with open(HTML_PATH, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif path == "/events":
                self._events()
            elif path == "/favicon.ico":
                self._send(204, b"", "image/x-icon")
            elif m := re.fullmatch(r"/api/exchanges/(\d+)", path):
                data = STORE.get(int(m.group(1)))
                if data is None:
                    self._json(404, {"error": "not in buffer"})
                else:
                    self._send(200, data, "application/json; charset=utf-8")
            elif path == "/api/export":
                name = "traffic-" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".json"
                self._send(200, STORE.export(), "application/json; charset=utf-8",
                           {"Content-Disposition": f'attachment; filename="{name}"'})
            else:
                self._json(404, {"error": "not found"})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        if length:
            self.rfile.read(length)
        if urlsplit(self.path).path == "/api/clear":
            STORE.clear()
            self._json(200, {"ok": True})
        else:
            self._json(404, {"error": "not found"})

    def _events(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        self.close_connection = True
        sub = Subscriber()
        try:
            self.wfile.write(b"retry: 2000\n\n" + STORE.snapshot_and_subscribe(sub))
            self.wfile.flush()
            while True:
                if sub.lagged:  # we dropped events for this browser: resync it
                    self.wfile.write(STORE.snapshot_and_subscribe(sub))
                    self.wfile.flush()
                try:
                    msg = sub.q.get(timeout=15)
                except queue.Empty:
                    msg = b": ping\n\n"
                self.wfile.write(msg)
                self.wfile.flush()
        except OSError:
            pass
        finally:
            BUS.remove(sub)


def main() -> None:
    global UPSTREAM, QUIET
    parser = argparse.ArgumentParser(description="Forward an agent's API traffic and show it live in a browser.")
    parser.add_argument("--port", type=int, default=8041, help="proxy port (default 8041)")
    parser.add_argument("--ui-port", type=int, default=8042, help="dashboard port (default 8042)")
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--quiet", action="store_true", help="don't print traffic to the terminal")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    QUIET = args.quiet

    try:
        proxy = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
        ui = ThreadingHTTPServer(("127.0.0.1", args.ui_port), UI)
    except OSError as e:
        print(f"cannot start: {e}", file=sys.stderr)
        sys.exit(1)
    proxy.daemon_threads = True
    ui.daemon_threads = True
    threading.Thread(target=ui.serve_forever, name="dashboard-ui", daemon=True).start()

    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"dashboard on       http://localhost:{args.ui_port}")
    print("run the agent with:")
    print(f"ANTHROPIC_BASE_URL=http://localhost:{args.port} claude", flush=True)
    try:
        proxy.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        proxy.server_close()
        ui.server_close()


if __name__ == "__main__":
    main()
