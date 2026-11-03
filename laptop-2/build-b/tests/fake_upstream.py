#!/usr/bin/env python3
"""A scripted stand-in for the model APIs, for tests only. No real API is called.

The newest user text picks the scenario. GET /__last returns what the last
request looked like on arrival, so tests can compare it with what the agent sent.
"""
import argparse
import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LAST = {}
LOCK = threading.Lock()
LONG = "\n".join(f"line {i}: the quick brown fox" for i in range(1, 343))


def sse(name, data):
    return f"event: {name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")


def anthropic_events(prompt, model):
    usage = {"input_tokens": 12, "output_tokens": 1, "cache_read_input_tokens": 18000, "cache_creation_input_tokens": 500}
    yield sse("message_start", {"type": "message_start", "message": {
        "id": "msg_1", "type": "message", "role": "assistant", "model": model, "content": [], "stop_reason": None, "usage": usage}})
    yield b": comment line\n\n"
    yield sse("ping", {"type": "ping"})
    index = 0
    tool_call = prompt.startswith("tool")
    if tool_call:
        yield sse("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}})
        yield sse("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "I should run a command."}})
        yield sse("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "signature_delta", "signature": "c2ln"}})
        yield sse("content_block_stop", {"type": "content_block_stop", "index": 0})
        index = 1
    yield sse("content_block_start", {"type": "content_block_start", "index": index, "content_block": {"type": "text", "text": ""}})
    text = LONG if prompt.startswith("longreply") else "Héllo wörld ✓ from the fake upstream."
    for start in range(0, len(text), 9):
        yield sse("content_block_delta", {"type": "content_block_delta", "index": index, "delta": {"type": "text_delta", "text": text[start:start + 9]}})
    yield sse("content_block_stop", {"type": "content_block_stop", "index": index})
    if prompt == "die":
        return
    if tool_call:
        call_id = "toolu_" + prompt.split()[1] if " " in prompt else "toolu_01"
        block = index + 1
        yield sse("content_block_start", {"type": "content_block_start", "index": block, "content_block": {"type": "tool_use", "id": call_id, "name": "Bash", "input": {}}})
        for piece in ['{"comm', 'and": "ls', ' -la", "nes', 'ted": {"a": {"b": {"c": {"d": 1}}}}}']:
            yield sse("content_block_delta", {"type": "content_block_delta", "index": block, "delta": {"type": "input_json_delta", "partial_json": piece}})
        yield sse("content_block_stop", {"type": "content_block_stop", "index": block})
    yield sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use" if tool_call else "end_turn"}, "usage": {"output_tokens": 340}})
    yield sse("message_stop", {"type": "message_stop"})


def codex_events():
    yield sse("response.created", {"type": "response.created", "response": {"id": "resp_1", "model": "gpt-5-codex"}})
    yield sse("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": {"type": "message", "role": "assistant"}})
    yield sse("response.output_text.delta", {"type": "response.output_text.delta", "output_index": 0, "delta": "Codex "})
    yield sse("response.output_text.delta", {"type": "response.output_text.delta", "output_index": 0, "delta": "says hi"})
    yield sse("response.output_item.done", {"type": "response.output_item.done", "output_index": 0, "item": {"type": "message"}})
    yield sse("response.output_item.added", {"type": "response.output_item.added", "output_index": 1, "item": {"type": "function_call", "call_id": "call_9", "name": "shell", "arguments": ""}})
    yield sse("response.function_call_arguments.delta", {"type": "response.function_call_arguments.delta", "output_index": 1, "delta": '{"cmd":'})
    yield sse("response.function_call_arguments.delta", {"type": "response.function_call_arguments.delta", "output_index": 1, "delta": '"ls"}'})
    yield sse("response.output_item.done", {"type": "response.output_item.done", "output_index": 1, "item": {"type": "function_call", "call_id": "call_9", "name": "shell", "arguments": '{"cmd":"ls"}'}})
    yield sse("response.completed", {"type": "response.completed", "response": {"model": "gpt-5-codex", "status": "completed", "usage": {
        "input_tokens": 100, "output_tokens": 20, "input_tokens_details": {"cached_tokens": 80}}}})


def newest_prompt(body):
    if not isinstance(body, dict):
        return ""
    for message in reversed(body.get("messages") or []):
        if message.get("role") != "user":
            continue
        content = message.get("content")
        if isinstance(content, str):
            return content
        for block in reversed(content or []):
            if block.get("type") == "text":
                return block["text"]
        return "after-tool"
    return ""


class Fake(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", "5")
        self.end_headers()

    def do_GET(self):
        if self.path == "/__last":
            with LOCK:
                payload = json.dumps(LAST).encode()
        else:
            payload = json.dumps({"data": [{"id": "m"}], "path": self.path}).encode()
        self.plain(200, payload)

    def plain(self, status, payload, extra=()):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for name, value in extra:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        with LOCK:
            LAST.clear()
            LAST.update(path=self.path, headers=list(self.headers.items()), body=base64.b64encode(raw).decode())
        try:
            body = json.loads(raw)
        except ValueError:
            body = None
        if not isinstance(body, dict):
            return self.plain(200, b'{"echo": "not json"}')
        prompt = newest_prompt(body)
        model = body.get("model", "m") if isinstance(body, dict) else "m"
        if "count_tokens" in self.path:
            return self.plain(200, b'{"input_tokens": 42}')
        if "responses" in self.path:
            return self.stream(codex_events(), 0.001)
        if prompt == "rate limit":
            return self.plain(429, b'{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}', [("Set-Cookie", "session=SECRETCOOKIEVALUE-0123456789")])
        if prompt == "nonstream" or not body.get("stream", True):
            return self.plain(200, json.dumps({"id": "msg_2", "type": "message", "role": "assistant", "model": model,
                "content": [{"type": "text", "text": "plain reply"}, {"type": "tool_use", "id": "toolu_ns", "name": "Read", "input": {"p": 1}}],
                "stop_reason": "tool_use", "usage": {"input_tokens": 5, "output_tokens": 6}}).encode())
        if prompt == "badsse":
            return self.stream([b"event: content_block_delta\ndata: {not json\n\n", sse("message_stop", {"type": "message_stop"})], 0)
        if prompt == "streamerror":
            return self.stream([sse("error", {"type": "error", "error": {"type": "overloaded_error", "message": "busy"}})], 0)
        if prompt.startswith("crlf"):
            events = [e.replace(b"\n", b"\r\n") for e in anthropic_events("x", model)]
            return self.stream(events, 0)
        delay = 0.25 if prompt.startswith("slow") else 0.001
        self.stream(anthropic_events(prompt, model), delay, abort=prompt == "die")

    def stream(self, events, delay, abort=False):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("X-Fake", "1")
        self.end_headers()
        try:
            for event in events:
                # Two pieces, so chunks can end inside an event or a character.
                for piece in (event[:len(event) // 2], event[len(event) // 2:]):
                    if piece:
                        self.wfile.write(f"{len(piece):x}\r\n".encode() + piece + b"\r\n")
                        self.wfile.flush()
                if delay:
                    time.sleep(delay)
            if not abort:
                self.wfile.write(b"0\r\n\r\n")
        except OSError:
            pass
        self.close_connection = True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8063)
    args = parser.parse_args()
    ThreadingHTTPServer(("127.0.0.1", args.port), Fake).serve_forever()
