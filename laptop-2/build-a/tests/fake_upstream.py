#!/usr/bin/env python3
"""A local stand-in for the model API. Never talks to a real service.

The word in the last user message picks the behavior. Prints its port on stdout.
GET /__received (direct, not via the proxy) returns what it was sent.
"""
import json
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RECEIVED = []
LOCK = threading.Lock()
USAGE_START = {"input_tokens": 12, "cache_read_input_tokens": 18000, "cache_creation_input_tokens": 500, "output_tokens": 1}


def last_text(body):
    messages = body.get("messages") or body.get("input") or []
    if isinstance(messages, str):
        return messages
    for message in reversed(messages):
        content = message.get("content", message.get("output", ""))
        if isinstance(content, str):
            return content
        for block in content:
            if block.get("type") == "tool_result":
                return "AFTER_TOOL"
            if "text" in block:
                return block["text"]
        return ""
    return ""


class Fake(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except OSError:
            self.close_connection = True  # the proxy hung up, which some tests do on purpose

    def sse(self, name, data, delay=0.0):
        self.wfile.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())
        self.wfile.flush()
        if delay:
            time.sleep(delay)

    def start_sse(self):
        self.send_response(200, "OK")
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Connection", "close")
        self.send_header("request-id", "req_fake")
        self.send_header("Set-Cookie", "session=SETCOOKIESECRETVALUE1234567890; Path=/")
        self.send_header("Set-Cookie", "other=2")
        self.end_headers()
        self.close_connection = True

    def message_start(self, model):
        self.sse("message_start", {"type": "message_start", "message": {
            "id": "msg_1", "type": "message", "role": "assistant", "model": model, "content": [],
            "stop_reason": None, "usage": USAGE_START}})

    def text_block(self, i, parts, delay=0.0):
        self.sse("content_block_start", {"type": "content_block_start", "index": i, "content_block": {"type": "text", "text": ""}})
        for part in parts:
            self.sse("content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "text_delta", "text": part}}, delay)
        self.sse("content_block_stop", {"type": "content_block_stop", "index": i})

    def end_message(self, stop="end_turn", output=340):
        self.sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": None}, "usage": {"output_tokens": output}})
        self.sse("message_stop", {"type": "message_stop"})

    def tool_block(self, i, tool_id, name, input_parts, delay=0.0):
        self.sse("content_block_start", {"type": "content_block_start", "index": i, "content_block": {"type": "tool_use", "id": tool_id, "name": name, "input": {}}})
        for part in input_parts:
            self.sse("content_block_delta", {"type": "content_block_delta", "index": i, "delta": {"type": "input_json_delta", "partial_json": part}}, delay)
        self.sse("content_block_stop", {"type": "content_block_stop", "index": i})

    def finish(self):
        try:
            super().finish()
        except OSError:
            pass

    def do_HEAD(self):
        self.record(b"")
        self.send_response(200, "OK")
        self.send_header("Content-Length", "5")
        self.send_header("X-Fake", "head")
        self.end_headers()

    def do_GET(self):
        if self.path == "/__received":
            with LOCK:
                body = json.dumps(RECEIVED).encode()
            return self.plain(200, body)
        self.record(b"")
        self.plain(200, b'{"data":[{"id":"claude-opus-5-5"}]}')

    def plain(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Fake", "plain")
        self.end_headers()
        self.wfile.write(body)

    def record(self, raw):
        with LOCK:
            RECEIVED.append({"method": self.command, "path": self.path, "headers": list(self.headers.items()), "body": raw.decode("utf-8", "replace")})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        self.record(raw)
        try:
            body = json.loads(raw)
        except ValueError:
            return self.plain(200, b"not json back", "text/plain")
        word = last_text(body)
        model = body.get("model", "claude-opus-5-5")
        if "responses" in self.path:
            return self.codex(word, model)
        if "NONSTREAM" in word:
            return self.plain(200, json.dumps({"id": "msg_2", "type": "message", "role": "assistant", "model": model, "stop_reason": "tool_use",
                "content": [{"type": "text", "text": "plain reply"}, {"type": "tool_use", "id": "toolu_nonstream", "name": "Read", "input": {"p": 1}}],
                "usage": {"input_tokens": 5, "output_tokens": 7}}).encode())
        if "ERR429" in word:
            return self.plain(429, b'{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}')
        self.start_sse()
        self.message_start(model)
        if "SLOW" in word:
            self.text_block(0, [f"word{i} " for i in range(12)], 0.25)
            self.end_message()
        elif "TOOLS2" in word:
            self.text_block(0, ["Running two."])
            self.tool_block(1, "toolu_ok", "Bash", ['{"comm', 'and": "ls ', '-la"}'])
            self.tool_block(2, "toolu_bad", "Read", ['{"path": "/nope"}'])
            self.end_message("tool_use", 50)
        elif "SLOWTOOL" in word or "TOOL" in word and "AFTER" not in word:
            tool_id = "toolu_" + word.split()[-1]
            self.sse("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}})
            self.sse("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "I should run it."}})
            self.sse("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "signature_delta", "signature": "sig"}})
            self.sse("content_block_stop", {"type": "content_block_stop", "index": 0})
            self.text_block(1, ["Let me ", "check."])
            self.tool_block(2, tool_id, "Bash", ['{"comm', 'and": "ls ', '-la", "opts": {"a": {"b": {"c": {"d": 1}}}}}'])
            self.end_message("tool_use", 50)
        elif "BADSSE" in word:
            self.wfile.write(b"event: content_block_delta\ndata: {not json at all\n\n")
            self.wfile.flush()
            self.text_block(0, ["survived"])
            self.end_message()
        elif "UTF8SPLIT" in word:
            payload = f'event: content_block_start\ndata: {json.dumps({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})}\n\n'.encode()
            payload += ('event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"héllo 😀"}}\n\n').encode("utf-8")
            for i in range(len(payload)):  # one byte at a time splits every multi-byte character
                self.wfile.write(payload[i:i + 1])
                self.wfile.flush()
            self.end_message()
        elif "SSEERROR" in word:
            self.text_block(0, ["partial"])
            self.sse("error", {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}})
        elif "KILL" in word:
            self.text_block(0, ["partial"])
            self.connection.shutdown(socket.SHUT_RDWR)  # close() alone is deferred by rfile/wfile
        elif "LONGREPLY" in word:
            self.text_block(0, [f"line {i} of a long reply\n" for i in range(100)], 0.02)
            self.end_message()
        else:
            self.text_block(0, ["Hello ", "from ", "the fake."])
            self.end_message()

    def codex(self, word, model):
        self.start_sse()
        self.sse("response.created", {"type": "response.created", "response": {"id": "r1", "model": model, "status": "in_progress"}})
        output = 0
        if "TOOL" in word and "AFTER" not in word:
            self.sse("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": {"type": "function_call", "call_id": "call_cx", "name": "shell", "arguments": ""}})
            for part in ('{"cmd"', ': "ls"}'):
                self.sse("response.function_call_arguments.delta", {"type": "response.function_call_arguments.delta", "output_index": 0, "delta": part})
            self.sse("response.output_item.done", {"type": "response.output_item.done", "output_index": 0, "item": {"type": "function_call", "call_id": "call_cx", "name": "shell", "arguments": '{"cmd": "ls"}'}})
        else:
            self.sse("response.output_item.added", {"type": "response.output_item.added", "output_index": output, "item": {"type": "message", "role": "assistant", "content": []}})
            for part in ("Codex ", "says hi"):
                self.sse("response.output_text.delta", {"type": "response.output_text.delta", "output_index": output, "delta": part})
            self.sse("response.output_item.done", {"type": "response.output_item.done", "output_index": output, "item": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Codex says hi"}]}})
        self.sse("response.completed", {"type": "response.completed", "response": {"model": model, "status": "completed",
            "usage": {"input_tokens": 100, "input_tokens_details": {"cached_tokens": 80}, "output_tokens": 9}}})


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    print(server.server_address[1], flush=True)
    server.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
