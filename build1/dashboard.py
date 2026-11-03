#!/usr/bin/env python3
"""dashboard.py: the agent-traffic proxy with a live browser dashboard.

    python3 dashboard.py                    # proxy :8041, dashboard :8042
    ANTHROPIC_BASE_URL=http://localhost:8041 claude
    open http://localhost:8042

Works like proxy.py (forwards everything unchanged and prints it), and also
publishes every exchange to the dashboard over Server-Sent Events.
Standard library only.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import queue
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))

# Hop-by-hop headers, never forwarded. accept-encoding is dropped so the
# upstream answers in plain text.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}
SECRET_HEADERS = {"authorization", "x-api-key", "cookie", "set-cookie"}

UPSTREAM = "https://api.anthropic.com"
QUIET = False
BUFFER_SIZE = 200
SUBSCRIBER_QUEUE = 5000


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def redact(name: str, value: str) -> str:
    if name.lower() in SECRET_HEADERS:
        return value[:16] + "..."
    return value


def out(text: str) -> None:
    if not QUIET:
        sys.stdout.write(text)
        sys.stdout.flush()


# --------------------------------------------------------------------------
# Pricing
# --------------------------------------------------------------------------

def load_pricing() -> dict:
    try:
        with open(os.path.join(HERE, "pricing.json"), encoding="utf-8") as f:
            data = json.load(f)
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except (OSError, ValueError):
        return {}


def compute_cost(model: str | None, usage: dict | None) -> float | None:
    if not model or not usage:
        return None
    pricing = load_pricing()  # re-read each time so edits apply live
    key = model if model in pricing else None
    if key is None:
        matches = [k for k in pricing if model.startswith(k)]
        key = max(matches, key=len) if matches else None
    if key is None:
        return None
    p = pricing[key]
    total = (
        (usage.get("input_tokens") or 0) * p.get("input", 0)
        + (usage.get("output_tokens") or 0) * p.get("output", 0)
        + (usage.get("cache_read_input_tokens") or 0) * p.get("cache_read", 0)
        + (usage.get("cache_creation_input_tokens") or 0) * p.get("cache_write", 0)
    )
    return round(total / 1_000_000, 6)


# --------------------------------------------------------------------------
# Store + event bus (one lock: a mutation and its event are atomic, so a new
# subscriber's snapshot never overlaps with the events it receives after).
# --------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.exchanges: OrderedDict[int, dict] = OrderedDict()
        self.tools: dict[str, dict] = {}
        self.subscribers: list[queue.Queue] = []
        self.next_id = 1

    # -- bus --------------------------------------------------------------
    def _publish(self, event: str, payload) -> None:
        msg = (event, payload)
        for q in self.subscribers:
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass  # slow browser: drop, never block the proxy

    def subscribe(self) -> tuple[queue.Queue, dict]:
        q: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE)
        with self.lock:
            self.subscribers.append(q)
            snap = {"exchanges": list(self.exchanges.values()), "tools": dict(self.tools)}
            # Serialize under the lock so later mutations can't leak into it.
            snap_json = json.dumps(snap, ensure_ascii=False)
        return q, snap_json

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            if q in self.subscribers:
                self.subscribers.remove(q)

    # -- mutations --------------------------------------------------------
    def start(self, method: str, path: str, headers: dict, body) -> dict:
        with self.lock:
            ex = {
                "id": self.next_id,
                "started_at": now_iso(),
                "ended_at": None,
                "method": method,
                "path": path,
                "status": None,
                "request_headers": headers,
                "request_body": body,
                "response_headers": None,
                "response_events": [],
                "response_body": None,
                "assembled": None,
                "timing": {"ttfb_ms": None, "ttft_ms": None, "total_ms": None},
                "cost_usd": None,
                "error": None,
                "_t0": time.monotonic(),
            }
            self.next_id += 1
            self.exchanges[ex["id"]] = ex
            while len(self.exchanges) > BUFFER_SIZE:
                self.exchanges.popitem(last=False)
            self._publish("exchange_started", public(ex))
            self._pair_results(ex)
            return ex

    def response(self, ex: dict, status: int, headers: dict) -> None:
        with self.lock:
            ex["status"] = status
            ex["response_headers"] = headers
            ex["timing"]["ttfb_ms"] = ms_since(ex)
            self._publish("exchange_response", {"id": ex["id"], "status": status, "headers": headers,
                                                "timing": ex["timing"]})

    def delta(self, ex: dict, event: dict) -> None:
        with self.lock:
            ex["response_events"].append(event)
            if ex["timing"]["ttft_ms"] is None and is_token_event(event):
                ex["timing"]["ttft_ms"] = ms_since(ex)
            self._publish("exchange_delta", {"id": ex["id"], "seq": len(ex["response_events"]) - 1,
                                             "event": event, "timing": ex["timing"]})

    def done(self, ex: dict, raw_body: bytes | None) -> None:
        with self.lock:
            if raw_body is not None:
                text = raw_body.decode("utf-8", "replace")
                try:
                    ex["response_body"] = json.loads(text)
                except ValueError:
                    ex["response_body"] = text
            ex["assembled"] = assemble(ex)
            ex["ended_at"] = now_iso()
            ex["timing"]["total_ms"] = ms_since(ex)
            a = ex["assembled"] or {}
            ex["cost_usd"] = compute_cost(a.get("model") or model_of(ex), a.get("usage"))
            self._register_calls(ex)
            self._publish("exchange_done", {
                "id": ex["id"], "ended_at": ex["ended_at"], "assembled": ex["assembled"],
                "timing": ex["timing"], "cost_usd": ex["cost_usd"], "response_body": ex["response_body"],
            })

    def fail(self, ex: dict, message: str) -> None:
        with self.lock:
            ex["error"] = message
            if ex["ended_at"] is None:
                ex["ended_at"] = now_iso()
                ex["timing"]["total_ms"] = ms_since(ex)
            self._publish("exchange_error", {"id": ex["id"], "message": message,
                                             "ended_at": ex["ended_at"], "timing": ex["timing"]})

    def clear(self) -> None:
        with self.lock:
            self.exchanges.clear()
            self.tools.clear()
            self._publish("cleared", {})

    def get(self, ex_id: int) -> dict | None:
        with self.lock:
            ex = self.exchanges.get(ex_id)
            return json.loads(json.dumps(public(ex))) if ex else None

    def export(self) -> str:
        with self.lock:
            return json.dumps({"exported_at": now_iso(),
                               "exchanges": [public(e) for e in self.exchanges.values()],
                               "tools": self.tools}, ensure_ascii=False, indent=2)

    # -- tool pairing -----------------------------------------------------
    def _register_calls(self, ex: dict) -> None:
        for block in (ex.get("assembled") or {}).get("content") or []:
            if block.get("type") == "tool_use" and block.get("id"):
                if block["id"] in self.tools:
                    continue
                entry = {
                    "tool_use_id": block["id"], "name": block.get("name"), "input": block.get("input"),
                    "call_exchange": ex["id"], "call_ended_at": ex["ended_at"],
                    "result_exchange": None, "result_started_at": None,
                    "result": None, "is_error": False, "result_size": None, "status": "pending",
                }
                self.tools[block["id"]] = entry
                self._publish("tool_call", entry)

    def _pair_results(self, ex: dict) -> None:
        messages = normalized_messages(ex.get("request_body"))
        if not messages:
            return
        for i, msg in enumerate(messages):
            for b in msg["blocks"]:
                if b["type"] != "tool_result":
                    continue
                tid = b["tool_use_id"]
                entry = self.tools.get(tid)
                if entry is None:
                    # Call happened before we were watching: recover it from history.
                    call = find_call(messages, tid)
                    entry = {
                        "tool_use_id": tid, "name": call.get("name") if call else None,
                        "input": call.get("input") if call else None,
                        "call_exchange": None, "call_ended_at": None,
                        "result_exchange": None, "result_started_at": None,
                        "result": None, "is_error": False, "result_size": None, "status": "pending",
                    }
                    self.tools[tid] = entry
                if entry["result_exchange"] is not None:
                    continue  # already paired; history is resent every turn
                entry.update({
                    "result_exchange": ex["id"], "result_started_at": ex["started_at"],
                    "result": b["content"], "is_error": bool(b.get("is_error")),
                    "result_size": len(json.dumps(b["content"], ensure_ascii=False)),
                    "status": "paired", "message_index": i,
                })
                self._publish("tool_paired", entry)
        # A call whose following message carries no result: the conversation moved on.
        for i, msg in enumerate(messages[:-1]):
            if msg["role"] != "assistant":
                continue
            next_ids = {b["tool_use_id"] for b in messages[i + 1]["blocks"] if b["type"] == "tool_result"}
            for b in msg["blocks"]:
                if b["type"] == "tool_use":
                    entry = self.tools.get(b["id"])
                    if entry and entry["status"] == "pending" and b["id"] not in next_ids:
                        entry["status"] = "no_result"
                        self._publish("tool_paired", entry)


def ms_since(ex: dict) -> int:
    return int((time.monotonic() - ex["_t0"]) * 1000)


def public(ex: dict) -> dict:
    return {k: v for k, v in ex.items() if not k.startswith("_")}


def model_of(ex: dict) -> str | None:
    body = ex.get("request_body")
    return body.get("model") if isinstance(body, dict) else None


def is_token_event(ev: dict) -> bool:
    name = ev.get("event") or ""
    data = ev.get("data")
    if name == "content_block_delta":
        return True
    if isinstance(data, dict) and data.get("type") == "content_block_delta":
        return True
    return name.endswith(".delta") and ("output_text" in name or "arguments" in name or "reasoning" in name)


# --------------------------------------------------------------------------
# Message normalization (Anthropic Messages API and OpenAI Responses / Codex)
# --------------------------------------------------------------------------

def normalized_messages(body) -> list[dict]:
    if not isinstance(body, dict):
        return []
    result = []
    if isinstance(body.get("messages"), list):
        for m in body["messages"]:
            content = m.get("content")
            if isinstance(content, str):
                content = [{"type": "text", "text": content}]
            blocks = []
            for b in content or []:
                t = b.get("type")
                if t == "tool_result":
                    blocks.append({"type": "tool_result", "tool_use_id": b.get("tool_use_id"),
                                   "content": b.get("content"), "is_error": b.get("is_error", False)})
                elif t == "tool_use":
                    blocks.append({"type": "tool_use", "id": b.get("id"), "name": b.get("name"),
                                   "input": b.get("input")})
                else:
                    blocks.append({"type": t})
            result.append({"role": m.get("role"), "blocks": blocks})
    elif isinstance(body.get("input"), list):  # OpenAI Responses (Codex)
        for item in body["input"]:
            t = item.get("type")
            if t == "function_call":
                try:
                    args = json.loads(item.get("arguments") or "{}")
                except ValueError:
                    args = item.get("arguments")
                result.append({"role": "assistant", "blocks": [
                    {"type": "tool_use", "id": item.get("call_id"), "name": item.get("name"), "input": args}]})
            elif t == "function_call_output":
                result.append({"role": "user", "blocks": [
                    {"type": "tool_result", "tool_use_id": item.get("call_id"),
                     "content": item.get("output"), "is_error": False}]})
            else:
                result.append({"role": item.get("role", t), "blocks": [{"type": t or "message"}]})
    return result


def find_call(messages: list[dict], tid: str) -> dict | None:
    for m in messages:
        for b in m["blocks"]:
            if b["type"] == "tool_use" and b.get("id") == tid:
                return b
    return None


# --------------------------------------------------------------------------
# Assembling a reply from its stream
# --------------------------------------------------------------------------

def assemble(ex: dict) -> dict | None:
    events = ex["response_events"]
    if events:
        if any((e.get("event") or "").startswith("response.") for e in events):
            return assemble_openai(events)
        return assemble_anthropic(events)
    body = ex.get("response_body")
    if isinstance(body, dict) and body.get("type") == "message":
        return {"model": body.get("model"), "content": body.get("content", []),
                "stop_reason": body.get("stop_reason"), "usage": body.get("usage", {})}
    if isinstance(body, dict) and body.get("type") == "error":
        return {"model": model_of(ex), "content": [], "stop_reason": None, "usage": {},
                "error": body.get("error")}
    return None


def assemble_anthropic(events: list[dict]) -> dict:
    a = {"model": None, "content": [], "stop_reason": None, "usage": {}}
    partial: dict[int, str] = {}
    for ev in events:
        d = ev.get("data")
        if not isinstance(d, dict):
            continue
        t = d.get("type")
        if t == "message_start":
            msg = d.get("message") or {}
            a["model"] = msg.get("model")
            a["usage"].update(msg.get("usage") or {})
        elif t == "content_block_start":
            idx = d.get("index", len(a["content"]))
            block = dict(d.get("content_block") or {})
            while len(a["content"]) <= idx:
                a["content"].append({})
            a["content"][idx] = block
        elif t == "content_block_delta":
            idx = d.get("index", 0)
            if idx >= len(a["content"]):
                continue
            block = a["content"][idx]
            delta = d.get("delta") or {}
            dt = delta.get("type")
            if dt == "text_delta":
                block["text"] = block.get("text", "") + delta.get("text", "")
            elif dt == "input_json_delta":
                partial[idx] = partial.get(idx, "") + delta.get("partial_json", "")
            elif dt == "thinking_delta":
                block["thinking"] = block.get("thinking", "") + delta.get("thinking", "")
            elif dt == "signature_delta":
                block["signature"] = delta.get("signature")
        elif t == "content_block_stop":
            idx = d.get("index", 0)
            if idx in partial and idx < len(a["content"]):
                try:
                    a["content"][idx]["input"] = json.loads(partial[idx] or "{}")
                except ValueError:
                    a["content"][idx]["input"] = partial[idx]
                del partial[idx]
        elif t == "message_delta":
            a["stop_reason"] = (d.get("delta") or {}).get("stop_reason") or a["stop_reason"]
            for k, v in (d.get("usage") or {}).items():
                if v is not None:
                    a["usage"][k] = v
        elif t == "error":
            a["error"] = d.get("error")
    for idx, raw in partial.items():  # stream cut before content_block_stop
        if idx < len(a["content"]):
            a["content"][idx]["input"] = raw
    return a


def assemble_openai(events: list[dict]) -> dict:
    a = {"model": None, "content": [], "stop_reason": None, "usage": {}}
    final = None
    items = []
    for ev in events:
        d = ev.get("data")
        if not isinstance(d, dict):
            continue
        if d.get("type") == "response.output_item.done":
            items.append(d.get("item") or {})
        if d.get("type") in ("response.completed", "response.failed", "response.incomplete"):
            final = d.get("response") or {}
        if d.get("type") == "response.created":
            a["model"] = (d.get("response") or {}).get("model")
    if final:
        a["model"] = final.get("model") or a["model"]
        items = final.get("output") or items
        a["stop_reason"] = final.get("status")
        u = final.get("usage") or {}
        cached = (u.get("input_tokens_details") or {}).get("cached_tokens") or 0
        a["usage"] = {"input_tokens": (u.get("input_tokens") or 0) - cached,
                      "output_tokens": u.get("output_tokens") or 0,
                      "cache_read_input_tokens": cached, "cache_creation_input_tokens": 0}
    for it in items:
        t = it.get("type")
        if t == "message":
            text = "".join(c.get("text", "") for c in it.get("content") or [] if isinstance(c, dict))
            a["content"].append({"type": "text", "text": text})
        elif t == "function_call":
            try:
                args = json.loads(it.get("arguments") or "{}")
            except ValueError:
                args = it.get("arguments")
            a["content"].append({"type": "tool_use", "id": it.get("call_id"), "name": it.get("name"),
                                 "input": args})
            if a["stop_reason"] in (None, "completed"):
                a["stop_reason"] = "tool_use"
        elif t == "reasoning":
            summary = "\n".join(s.get("text", "") for s in it.get("summary") or [] if isinstance(s, dict))
            a["content"].append({"type": "thinking", "thinking": summary})
    return a


# --------------------------------------------------------------------------
# SSE parsing of the upstream stream
# --------------------------------------------------------------------------

class SSEParser:
    def __init__(self) -> None:
        self.buf = ""

    def feed(self, text: str) -> list[dict]:
        self.buf += text.replace("\r\n", "\n")
        events = []
        while "\n\n" in self.buf:
            raw, self.buf = self.buf.split("\n\n", 1)
            ev = parse_event(raw)
            if ev:
                events.append(ev)
        return events

    def flush(self) -> list[dict]:
        rest, self.buf = self.buf.strip(), ""
        ev = parse_event(rest) if rest else None
        return [ev] if ev else []


def parse_event(raw: str) -> dict | None:
    name, data_lines = None, []
    for line in raw.split("\n"):
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" ") if line[5:6] == " " else line[5:])
    if name is None and not data_lines:
        return None
    data_text = "\n".join(data_lines)
    try:
        data = json.loads(data_text)
    except ValueError:
        data = data_text  # kept as a raw string; never interrupts forwarding
    if name is None and isinstance(data, dict):
        name = data.get("type")
    return {"event": name, "data": data}


# --------------------------------------------------------------------------
# The proxy (:8041)
# --------------------------------------------------------------------------

STORE = Store()


class Proxy(BaseHTTPRequestHandler):
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
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""

        # 1. Print and record what the agent sent.
        safe_headers = {name: redact(name, value) for name, value in self.headers.items()}
        lines = [f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}\n"]
        lines += [f"    {k}: {v}\n" for k, v in safe_headers.items()]
        parsed = None
        if body:
            try:
                parsed = json.loads(body)
                lines.append(json.dumps(parsed, indent=2, ensure_ascii=False) + "\n")
            except ValueError:
                parsed = body.decode("utf-8", "replace")
                lines.append(parsed + "\n")
        out("".join(lines))
        ex = STORE.start(self.command, self.path, safe_headers, parsed)

        # 2. Forward it, headers intact.
        target = urlsplit(UPSTREAM)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
            upstream.sock.settimeout(600)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()
        except OSError as e:
            out(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}\n")
            STORE.fail(ex, f"upstream unreachable: {e}")
            try:
                self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            except OSError:
                pass
            self.close_connection = True
            return

        # 3. Stream the reply back as it arrives.
        out(f"\n[{stamp()}] <<< {reply.status} {reply.reason}\n")
        resp_headers = {name: redact(name, value) for name, value in reply.getheaders()}
        STORE.response(ex, reply.status, resp_headers)
        self.send_response(reply.status, reply.reason)
        for name, value in reply.getheaders():
            if name.lower() not in SKIP and name.lower() != "content-encoding":
                self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command == "HEAD":
            STORE.done(ex, None)
            return

        is_sse = "text/event-stream" in (reply.getheader("content-type") or "")
        parser = SSEParser() if is_sse else None
        plain = bytearray()
        client_gone = False
        try:
            while chunk := reply.read1(4096):
                if not client_gone:
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except OSError:
                        client_gone = True  # keep reading so the record is complete
                text = chunk.decode("utf-8", "replace")
                out(text)
                if parser:
                    try:
                        for ev in parser.feed(text):
                            STORE.delta(ex, ev)
                    except Exception as e:  # noqa: BLE001 - parsing must never break forwarding
                        STORE.delta(ex, {"event": "parse_error", "data": f"{e}: {text}"})
                else:
                    plain.extend(chunk)
            if parser:
                for ev in parser.flush():
                    STORE.delta(ex, ev)
        except OSError as e:
            STORE.fail(ex, f"upstream stream broke: {e}")
        STORE.done(ex, None if parser else bytes(plain))
        if client_gone:
            STORE.fail(ex, "agent disconnected before the reply finished")
        out(f"\n[{stamp()}] --- done\n")


# --------------------------------------------------------------------------
# The dashboard server (:8042)
# --------------------------------------------------------------------------

class Dashboard(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def send_bytes(self, code: int, ctype: str, data: bytes, extra: dict | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path in ("/", "/index.html", "/dashboard.html"):
            try:
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    self.send_bytes(200, "text/html; charset=utf-8", f.read())
            except OSError:
                self.send_bytes(500, "text/plain", b"dashboard.html not found next to dashboard.py")
        elif path == "/events":
            self.stream_events()
        elif path.startswith("/api/exchanges/"):
            try:
                ex = STORE.get(int(path.rsplit("/", 1)[1]))
            except ValueError:
                ex = None
            if ex is None:
                self.send_bytes(404, "application/json", b'{"error":"not found"}')
            else:
                self.send_bytes(200, "application/json", json.dumps(ex, ensure_ascii=False).encode())
        elif path == "/api/export":
            name = f"traffic-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
            self.send_bytes(200, "application/json", STORE.export().encode(),
                            {"Content-Disposition": f'attachment; filename="{name}"'})
        elif path == "/api/pricing":
            self.send_bytes(200, "application/json", json.dumps(load_pricing()).encode())
        else:
            self.send_bytes(404, "text/plain", b"not found")

    def do_POST(self) -> None:
        if urlsplit(self.path).path == "/api/clear":
            STORE.clear()
            self.send_bytes(200, "application/json", b'{"ok":true}')
        else:
            self.send_bytes(404, "text/plain", b"not found")

    def stream_events(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        q, snap_json = STORE.subscribe()
        try:
            self.wfile.write(f"event: snapshot\ndata: {snap_json}\n\n".encode())
            self.wfile.flush()
            while True:
                try:
                    event, payload = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                data = json.dumps(payload, ensure_ascii=False)
                self.wfile.write(f"event: {event}\ndata: {data}\n\n".encode())
                self.wfile.flush()
        except OSError:
            pass  # browser closed or reloaded; the proxy is unaffected
        finally:
            STORE.unsubscribe(q)


class Server(ThreadingHTTPServer):
    daemon_threads = True


def main() -> None:
    global UPSTREAM, QUIET
    parser = argparse.ArgumentParser(description="Forward an agent's API traffic and show it live in a browser.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--ui-port", type=int, default=8042)
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--quiet", action="store_true", help="don't print traffic to the terminal")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    QUIET = args.quiet

    proxy = Server(("127.0.0.1", args.port), Proxy)
    ui = Server(("127.0.0.1", args.ui_port), Dashboard)
    threading.Thread(target=ui.serve_forever, daemon=True).start()
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"dashboard on      http://localhost:{args.ui_port}")
    print(f"ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    sys.stdout.flush()
    try:
        proxy.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
