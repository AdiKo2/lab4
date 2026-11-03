#!/usr/bin/env python3
"""A fake Anthropic Messages API for tests: spends no tokens.

    python3 tests/fake_upstream.py 18050
    python3 dashboard.py --upstream http://127.0.0.1:18050

The last user text picks the behavior:
    TOOL       reply with text + Bash and Read tool calls (until a tool_result comes back)
    THINK      start with a thinking block
    RATELIMIT  answer 429 the first time a body is seen, then normally
    NONSTREAM  answer with a plain JSON message instead of a stream
    NOFINAL    stop the stream before message_stop
    CACHE1H    report usage.cache_creation split into 5m and 1h parts
Anything else gets a short streamed answer that includes a <script> tag.
"""
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

seen_429 = set()
lock = threading.Lock()


def sse(w, name, data):
    w.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())
    w.flush()


class Fake(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def send_json(self, status, obj, headers=()):
        data = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["content-length"]))
        body = json.loads(raw)
        msgs = body["messages"]
        last = msgs[-1]["content"]
        content = last if isinstance(last, list) else [{"type": "text", "text": last}]
        text = " ".join(b.get("text", "") for b in content if b.get("type") == "text")
        has_result = any(b.get("type") == "tool_result" for b in content)

        if "RATELIMIT" in text:
            with lock:
                first = raw not in seen_429
                seen_429.add(raw)
            if first:
                return self.send_json(429, {"type": "error", "error": {
                    "type": "rate_limit_error", "message": "Number of request tokens has exceeded your per-minute rate limit"}},
                    [("retry-after", "6"), ("anthropic-ratelimit-input-tokens-limit", "40000"),
                     ("anthropic-ratelimit-input-tokens-remaining", "0")])
        if "NONSTREAM" in text:
            return self.send_json(200, {"id": "msg_ns", "type": "message", "role": "assistant", "model": body["model"],
                                        "content": [{"type": "text", "text": "non-streaming answer"}], "stop_reason": "end_turn",
                                        "usage": {"input_tokens": 10, "output_tokens": 3}})

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        w = self.wfile
        usage = {"input_tokens": 12, "cache_read_input_tokens": 18000 if len(msgs) > 1 else 0,
                 "cache_creation_input_tokens": 500, "output_tokens": 1}
        if "CACHE1H" in text:
            usage["cache_creation_input_tokens"] = 3000
            usage["cache_creation"] = {"ephemeral_5m_input_tokens": 1000, "ephemeral_1h_input_tokens": 2000}
        sse(w, "message_start", {"type": "message_start", "message": {
            "id": "msg_1", "type": "message", "role": "assistant", "model": body["model"], "content": [],
            "stop_reason": None, "usage": usage}})
        sse(w, "ping", {"type": "ping"})
        i = 0
        if "THINK" in text:
            sse(w, "content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}})
            sse(w, "content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "Let me think."}})
            sse(w, "content_block_stop", {"type": "content_block_stop", "index": 0})
            i = 1
        tool_turn = "TOOL" in text and not has_result
        words = "I will look at the files now." if tool_turn else "Here is the answer: <script>alert('x')</script> done."
        sse(w, "content_block_start", {"type": "content_block_start", "index": i, "content_block": {"type": "text", "text": ""}})
        sse(w, "content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "text_delta", "text": ""}})
        for word in words.split(" "):
            sse(w, "content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "text_delta", "text": word + " "}})
            time.sleep(0.02)
        sse(w, "content_block_stop", {"type": "content_block_stop", "index": i})
        if "NOFINAL" in text:
            return  # the connection closes without message_delta / message_stop
        if tool_turn:
            for j, (name, inp) in enumerate([("Bash", {"command": "ls -la"}), ("Read", {"file_path": "/tmp/x.py"})]):
                k = i + 1 + j
                sse(w, "content_block_start", {"type": "content_block_start", "index": k, "content_block": {
                    "type": "tool_use", "id": f"toolu_{len(msgs)}_{j}", "name": name, "input": {}}})
                s = json.dumps(inp)
                for p in range(0, len(s), 7):
                    sse(w, "content_block_delta", {"type": "content_block_delta", "index": k, "delta": {"type": "input_json_delta", "partial_json": s[p:p + 7]}})
                sse(w, "content_block_stop", {"type": "content_block_stop", "index": k})
        sse(w, "message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use" if tool_turn else "end_turn"},
                                 "usage": {"output_tokens": 42}})
        sse(w, "message_stop", {"type": "message_stop"})


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Fake).serve_forever()
