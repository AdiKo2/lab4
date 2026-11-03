#!/usr/bin/env python3
"""A fake Anthropic Messages API for testing dashboard.py without real traffic.

    python3 tests/fake_upstream.py [port]      # default 8049

Behaviour (decided by the last user message):
  - contains "RATE_LIMIT_ME": the first time a given body is seen -> 429 JSON error
    with retry-after and anthropic-ratelimit-* headers; an identical retry -> normal reply.
  - contains "use tool": streams thinking + text + a Bash tool_use (input in pieces), stop_reason tool_use.
  - is a tool_result: streams a text reply, end_turn.
  - contains "SLOW": streams 40 deltas 100 ms apart (for client-drop tests).
  - "stream": false in the body -> a plain JSON message.
  - /v1/messages/count_tokens -> {"input_tokens": N}; HEAD/GET anything else -> 200 empty.
"""
import hashlib
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEEN = set()


def last_user_text(body):
    msgs = body.get("messages") or []
    if not msgs:
        return "", False
    c = msgs[-1].get("content")
    if isinstance(c, str):
        return c, False
    texts, tr = [], False
    for b in c or []:
        if b.get("type") == "text":
            texts.append(b.get("text", ""))
        if b.get("type") == "tool_result":
            tr = True
    return "\n".join(texts), tr


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self.do_HEAD()

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        body = json.loads(raw or b"{}")
        if self.path.startswith("/v1/messages/count_tokens"):
            return self.json(200, {"input_tokens": len(raw) // 4})
        text, is_tool_result = last_user_text(body)
        if "RATE_LIMIT_ME" in text:
            h = hashlib.sha1(raw).hexdigest()
            if h not in SEEN:
                SEEN.add(h)
                return self.json(429, {"type": "error", "error": {
                    "type": "rate_limit_error",
                    "message": "Number of request tokens has exceeded your per-minute rate limit"}},
                    {"retry-after": "6", "anthropic-ratelimit-requests-limit": "50",
                     "anthropic-ratelimit-requests-remaining": "0",
                     "anthropic-ratelimit-requests-reset": "2026-11-03T10:00:06Z"})
        model = body.get("model", "claude-sonnet-4-5-20250929")
        usage = {"input_tokens": 12,
                 "cache_read_input_tokens": 18000 if len(body.get("messages", [])) > 1 else 0,
                 "cache_creation_input_tokens": 500, "output_tokens": 1}
        tid = "toolu_" + hashlib.sha1(raw).hexdigest()[:10]
        if is_tool_result:
            blocks = [("text", "The command finished. The output contains a <script>alert('x')</script> tag, shown as text.")]
            stop = "end_turn"
        elif "use tool" in text:
            blocks = [("thinking", "The user wants me to run a tool. I'll list files."),
                      ("text", "Let me run a command."),
                      ("tool_use", {"command": "ls -la /tmp", "description": "List files"})]
            stop = "tool_use"
        else:
            blocks = [("text", "Hello! " + "This is a streamed reply. " * 8 + "\nSecond line.")]
            stop = "end_turn"
        if body.get("stream") is False:
            content = []
            for kind, val in blocks:
                if kind == "text":
                    content.append({"type": "text", "text": val})
                elif kind == "tool_use":
                    content.append({"type": "tool_use", "id": tid, "name": "Bash", "input": val})
            usage["output_tokens"] = 42
            return self.json(200, {"id": "msg_x", "type": "message", "role": "assistant", "model": model,
                                   "content": content, "stop_reason": stop, "usage": usage})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("request-id", "req_fake")
        self.send_header("Connection", "close")
        self.end_headers()
        slow = "SLOW" in text
        delay = 0.1 if slow else 0.02

        def ev(name, data):
            self.wfile.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())
            self.wfile.flush()
            time.sleep(delay)

        ev("message_start", {"type": "message_start", "message": {
            "id": "msg_fake", "type": "message", "role": "assistant", "model": model, "content": [],
            "stop_reason": None, "usage": usage}})
        self.wfile.write(b": keepalive comment\n\n")
        out = 0
        for i, (kind, val) in enumerate(blocks):
            if kind == "text":
                ev("content_block_start", {"type": "content_block_start", "index": i, "content_block": {"type": "text", "text": ""}})
                words = ["tick"] * 40 if slow else val.split(" ")
                for w in words:
                    ev("content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "text_delta", "text": w + " "}})
                    out += 1
            elif kind == "thinking":
                ev("content_block_start", {"type": "content_block_start", "index": i, "content_block": {"type": "thinking", "thinking": ""}})
                ev("content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "thinking_delta", "thinking": val}})
                ev("content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "signature_delta", "signature": "sig123"}})
            else:
                ev("content_block_start", {"type": "content_block_start", "index": i, "content_block": {"type": "tool_use", "id": tid, "name": "Bash", "input": {}}})
                s = json.dumps(val)
                for k in range(0, len(s), 7):
                    ev("content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "input_json_delta", "partial_json": s[k:k + 7]}})
            ev("content_block_stop", {"type": "content_block_stop", "index": i})
        ev("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": None}, "usage": {"output_tokens": out + 20}})
        ev("message_stop", {"type": "message_stop"})
        self.close_connection = True

    def json(self, code, obj, extra=None):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8049
    print(f"fake upstream on http://127.0.0.1:{port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
