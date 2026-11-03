#!/usr/bin/env python3
"""A scripted stand-in for the model API, for tests only. Binds an OS-chosen port.

    python3 fake_upstream.py      # prints "PORT <n>", then serves

It records each request's raw headers and body for GET /__seen, and chooses a
reply from the last user message, so tests can drive one scenario per prompt.
"""

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEEN = []
LOCK = threading.Lock()
MODEL = "claude-opus-5-5"


def sse(name, data):
    return f"event: {name}\ndata: {json.dumps(data)}\n\n".encode()


def message_start(input_tokens=12, read=18000, write=500, model=MODEL):
    return sse("message_start", {"type": "message_start", "message": {
        "id": "msg_1", "type": "message", "role": "assistant", "model": model, "content": [],
        "stop_reason": None,
        "usage": {"input_tokens": input_tokens, "output_tokens": 1,
                  "cache_read_input_tokens": read, "cache_creation_input_tokens": write},
    }})


def text_stream(text, delay=0.0, model=MODEL, read=18000):
    yield message_start(model=model, read=read)
    yield sse("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})
    yield sse("ping", {"type": "ping"})
    for word in text.split(" "):
        yield sse("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": word + " "}})
        if delay:
            time.sleep(delay)
    yield sse("content_block_stop", {"type": "content_block_stop", "index": 0})
    yield sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 340}})
    yield sse("message_stop", {"type": "message_stop"})


def tool_stream(tool_id="toolu_01abc", name="Bash", args='{"command": "ls -la"}'):
    yield message_start()
    yield sse("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}})
    yield sse("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "I should list files."}})
    yield sse("content_block_stop", {"type": "content_block_stop", "index": 0})
    yield sse("content_block_start", {"type": "content_block_start", "index": 1, "content_block": {"type": "text", "text": ""}})
    yield sse("content_block_delta", {"type": "content_block_delta", "index": 1, "delta": {"type": "text_delta", "text": "Listing."}})
    yield sse("content_block_stop", {"type": "content_block_stop", "index": 1})
    yield sse("content_block_start", {"type": "content_block_start", "index": 2, "content_block": {"type": "tool_use", "id": tool_id, "name": name, "input": {}}})
    yield sse("content_block_delta", {"type": "content_block_delta", "index": 2, "delta": {"type": "input_json_delta", "partial_json": ""}})
    for i in range(0, len(args), 5):
        yield sse("content_block_delta", {"type": "content_block_delta", "index": 2, "delta": {"type": "input_json_delta", "partial_json": args[i:i + 5]}})
    yield sse("content_block_stop", {"type": "content_block_stop", "index": 2})
    yield sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 77}})
    yield sse("message_stop", {"type": "message_stop"})


def codex_stream():
    def ev(data):
        return sse(data["type"], data)
    yield ev({"type": "response.created", "response": {"id": "resp_1", "model": "gpt-5-codex", "status": "in_progress"}})
    yield ev({"type": "response.output_item.added", "output_index": 0, "item": {"type": "message", "role": "assistant", "content": []}})
    yield ev({"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": "Hello from "})
    yield ev({"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": "codex"})
    yield ev({"type": "response.output_item.done", "output_index": 0, "item": {"type": "message", "content": [{"type": "output_text", "text": "Hello from codex"}]}})
    yield ev({"type": "response.output_item.added", "output_index": 1, "item": {"type": "function_call", "call_id": "call_9", "name": "shell", "arguments": ""}})
    yield ev({"type": "response.function_call_arguments.delta", "output_index": 1, "delta": '{"cmd":'})
    yield ev({"type": "response.function_call_arguments.delta", "output_index": 1, "delta": '"ls"}'})
    yield ev({"type": "response.output_item.done", "output_index": 1, "item": {"type": "function_call", "call_id": "call_9", "name": "shell", "arguments": '{"cmd":"ls"}'}})
    yield ev({"type": "response.completed", "response": {"id": "resp_1", "model": "gpt-5-codex", "status": "completed",
              "usage": {"input_tokens": 1000, "output_tokens": 50, "input_tokens_details": {"cached_tokens": 900}}}})


def last_text(body):
    if isinstance(body, dict) and isinstance(body.get("input"), list):
        return "codex"
    try:
        content = body["messages"][-1]["content"]
    except (KeyError, IndexError, TypeError):
        return ""
    if isinstance(content, str):
        return content
    parts = []
    for block in content:
        if block.get("type") == "text":
            parts.append(block.get("text", ""))
        elif block.get("type") == "tool_result":
            parts.append("tool-result")
    return " ".join(parts)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True

    def log_message(self, *args):
        pass

    def stream(self, chunks, extra=()):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Transfer-Encoding", "chunked")
        for name, value in extra:
            self.send_header(name, value)
        self.end_headers()
        try:
            for chunk in chunks:
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
            self.wfile.write(b"0\r\n\r\n")
        except OSError:
            pass
        self.close_connection = True

    def plain(self, status, body, content_type="application/json", extra=()):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in extra:
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self):
        self.plain(200, b"")

    def do_GET(self):
        if self.path == "/__seen":
            with LOCK:
                self.plain(200, json.dumps(SEEN).encode())
            return
        with LOCK:
            SEEN.append({"method": "GET", "path": self.path, "headers": self.headers.items(), "body": ""})
        self.plain(200, b'{"data":[{"id":"claude-opus-5-5"}]}')

    def do_POST(self):
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length)
        with LOCK:
            SEEN.append({"method": "POST", "path": self.path, "headers": self.headers.items(), "body": raw.decode("utf-8", "replace")})
        try:
            body = json.loads(raw)
        except ValueError:
            body = None
        prompt = last_text(body)

        if "count_tokens" in self.path:
            self.plain(200, b'{"input_tokens":1234}')
        elif prompt == "codex":
            self.stream(codex_stream())
        elif prompt.startswith("slow"):
            self.stream(text_stream("word " * 40, delay=0.05))
        elif prompt.startswith("tool-error"):
            self.stream(tool_stream("toolu_err", "Read", '{"path": "/nope"}'))
        elif prompt.startswith("tool-parallel"):
            self.stream(tool_stream("toolu_parallel", "Grep", '{"q": "x"}'))
        elif prompt.startswith("tool") and not prompt.startswith("tool-result"):
            self.stream(tool_stream())
        elif prompt.startswith("unpriced"):
            self.stream(text_stream("no price here", model="mystery-model-9"))
        elif prompt.startswith("dated"):
            self.stream(text_stream("dated model", model="claude-opus-5-5-20260401"))
        elif prompt.startswith("xss"):
            self.stream(text_stream("<script>window.__pwned=1</script> <img src=x onerror=window.__pwned=2> done"))
        elif prompt.startswith("longreply"):
            self.stream(text_stream("line\n " * 60, delay=0.03))
        elif prompt.startswith("split"):
            # Events and a multi-byte character split across TCP chunks, plus CRLF endings.
            whole = (
                message_start()
                + sse("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})
                + json.dumps({}).encode()[:0]
                + b'event: content_block_delta\r\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"caf\xc3\xa9 \xf0\x9f\x8c\x8d"}}\r\n\r\n'
                + b"event: broken\ndata: {not json\n\n"
                + b": a comment\n\n"
                + sse("content_block_stop", {"type": "content_block_stop", "index": 0})
                + sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 5, "input_tokens": 0}})
                + sse("message_stop", {"type": "message_stop"})
            )
            def pieces():
                for i in range(0, len(whole), 7):
                    yield whole[i:i + 7]
            self.stream(pieces())
        elif prompt.startswith("cutoff"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            chunk = message_start() + sse("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}) + sse("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "partial"}})
            self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
            self.wfile.flush()
            self.close_connection = True  # no terminating chunk
        elif prompt.startswith("sse-error"):
            def err():
                yield message_start()
                yield sse("error", {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}})
            self.stream(err())
        elif prompt.startswith("http429"):
            self.plain(429, b'{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}', extra=[("Set-Cookie", "session=topsecretcookievalue1234567890; Path=/")])
        elif prompt.startswith("nostream"):
            self.plain(200, json.dumps({"type": "message", "id": "m", "model": MODEL, "role": "assistant", "stop_reason": "end_turn",
                                        "content": [{"type": "text", "text": "plain reply"}],
                                        "usage": {"input_tokens": 5, "output_tokens": 6}}).encode())
        elif prompt.startswith("binary"):
            self.plain(200, bytes(range(256)), "application/octet-stream")
        else:
            self.stream(text_stream("Hello from the fake upstream, streaming a reply."),
                        extra=[("X-Custom-Reply", "kept"), ("Set-Cookie", "a=1"), ("Set-Cookie", "b=2")])


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    print("PORT", server.server_address[1], flush=True)
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
