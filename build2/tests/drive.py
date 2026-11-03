#!/usr/bin/env python3
"""Send Claude-Code-like synthetic traffic through dashboard.py and check the results.

    python3 tests/fake_upstream.py 8049 &
    python3 dashboard.py --upstream http://127.0.0.1:8049 --quiet &
    python3 tests/drive.py [PROXY_PORT UI_PORT UPSTREAM_PORT]   # default 8041 8042 8049

Checks: byte-identical forwarding, /events stream contents, tool pairing, retry
linking, redaction, usage numbers, conversation diff, client drop, export.
"""
import http.client
import json
import socket
import sys
import threading
import time

PROXY, UI, UP = (int(x) for x in (sys.argv[1:4] if len(sys.argv) > 3 else (8041, 8042, 8049)))
SECRET = "sk-ant-oat01-SUPERSECRETTOKEN-abcdefghijklmnopqrstuvwxyz0123456789"
FAILS = []


def check(cond, what):
    print(("PASS " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def send(port, method, path, body=None, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    data = json.dumps(body).encode() if body is not None else None
    h = {"content-type": "application/json", "authorization": "Bearer " + SECRET,
         "anthropic-version": "2023-06-01", "anthropic-beta": "oauth-2025-04-20", "cookie": "session=" + SECRET}
    h.update(headers or {})
    c.request(method, path, body=data, headers=h)
    r = c.getresponse()
    out = r.read()
    c.close()
    return r.status, dict(r.getheaders()), out


class Events(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.events = []

    def run(self):
        c = http.client.HTTPConnection("127.0.0.1", UI, timeout=60)
        c.request("GET", "/events")
        r = c.getresponse()
        buf = b""
        while True:
            chunk = r.read1(65536)
            if not chunk:
                return
            buf += chunk
            while b"\n\n" in buf:
                block, buf = buf.split(b"\n\n", 1)
                name, data = None, None
                for line in block.decode().split("\n"):
                    if line.startswith("event: "):
                        name = line[7:]
                    elif line.startswith("data: "):
                        data = json.loads(line[6:])
                if name:
                    self.events.append((name, data, time.time()))


SYSTEM = [
    {"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."},
    {"type": "text", "text": "\n".join(f"Instruction line {i}: always be careful with tool calls and explain what you do." for i in range(80)),
     "cache_control": {"type": "ephemeral"}},
]
TOOLS = [
    {"name": "Bash", "description": "Executes a given bash command.\n" + "\n".join(f"- usage note {i}" for i in range(30)),
     "input_schema": {"type": "object", "properties": {"command": {"type": "string", "description": "The command"},
                                                       "timeout": {"type": "number"},
                                                       "options": {"type": "object", "properties": {"deep": {"type": "object", "properties": {"x": {"type": "string"}}}}}},
                      "required": ["command"]}},
    {"name": "Read", "description": "Reads a file.", "input_schema": {"type": "object", "properties": {"file_path": {"type": "string"}}}},
    {"name": "Edit", "description": "Edits a file.", "input_schema": {"type": "object", "properties": {"old": {"type": "string"}, "new": {"type": "string"}}}},
]
MODEL = "claude-sonnet-4-5-20250929"


def req(messages, **kw):
    b = {"model": MODEL, "max_tokens": 32000, "stream": True, "system": SYSTEM, "tools": TOOLS,
         "messages": messages, "metadata": {"user_id": "user_abc"}, "temperature": 1}
    b.update(kw)
    return b


def assistant_from(raw_sse):
    """Rebuild the assistant message content from a raw SSE body (like a client would)."""
    content = {}
    partial = {}
    for block in raw_sse.decode().split("\n\n"):
        for line in block.split("\n"):
            if line.startswith("data: "):
                d = json.loads(line[6:])
                if d["type"] == "content_block_start":
                    content[d["index"]] = dict(d["content_block"])
                    if d["content_block"]["type"] == "tool_use":
                        partial[d["index"]] = ""
                elif d["type"] == "content_block_delta":
                    dl = d["delta"]
                    if dl["type"] == "text_delta":
                        content[d["index"]]["text"] += dl["text"]
                    elif dl["type"] == "thinking_delta":
                        content[d["index"]]["thinking"] += dl["thinking"]
                    elif dl["type"] == "signature_delta":
                        content[d["index"]]["signature"] = dl["signature"]
                    elif dl["type"] == "input_json_delta":
                        partial[d["index"]] += dl["partial_json"]
                elif d["type"] == "content_block_stop" and d["index"] in partial:
                    content[d["index"]]["input"] = json.loads(partial.pop(d["index"]))
    return [content[i] for i in sorted(content)]


def main():
    ev = Events()
    ev.start()
    time.sleep(0.3)

    # 0. connection check
    st, _, _ = send(PROXY, "HEAD", "/api/hello")
    check(st == 200, "HEAD /api/hello forwarded")

    # 1. first prompt; also compare with a direct upstream call (byte identical)
    m1 = [{"role": "user", "content": [{"type": "text", "text": "hello there", "cache_control": {"type": "ephemeral"}}]}]
    t0 = time.time()
    st, hdr, body1 = send(PROXY, "POST", "/v1/messages?beta=true", req(m1))
    st_d, _, body1_direct = send(UP, "POST", "/v1/messages?beta=true", req(m1))
    check(st == 200 and body1 == body1_direct, "proxy reply is byte-identical to a direct upstream reply")
    started = [e for e in ev.events if e[0] == "exchange_started"]
    check(len(started) >= 2, "exchange_started events arrive")
    if len(started) >= 2:
        check(started[1][2] - t0 < 0.2, f"row event within 200 ms of sending (took {int((started[1][2]-t0)*1000)} ms)")

    # 2. a turn that uses a tool
    a1 = {"role": "assistant", "content": assistant_from(body1)}
    m2 = [{"role": "user", "content": [{"type": "text", "text": "hello there"}]}, a1,
          {"role": "user", "content": [{"type": "text", "text": "please use tool to list files", "cache_control": {"type": "ephemeral"}}]}]
    st, _, body2 = send(PROXY, "POST", "/v1/messages?beta=true", req(m2))
    a2 = {"role": "assistant", "content": assistant_from(body2)}
    tool_use = [b for b in a2["content"] if b["type"] == "tool_use"][0]
    check(tool_use["input"] == {"command": "ls -la /tmp", "description": "List files"}, "tool_use input assembled from input_json_delta")
    time.sleep(0.2)
    upd = [e for e in ev.events if e[0] == "tool_update" and e[1]["tool_use_id"] == tool_use["id"]]
    check(upd and upd[-1][1]["entry"]["status"] == "pending", "tool call registered as pending after the reply")

    # 3. tool result (contains <script> and a long JSON-ish output)
    long_out = json.dumps({"files": [{"name": f"file{i}.txt", "size": i * 100} for i in range(60)]}, indent=2)
    m3 = [m2[0], a1, {"role": "user", "content": [{"type": "text", "text": "please use tool to list files"}]}, a2,
          {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tool_use["id"],
                                        "content": "<script>window.__pwned=1;alert('xss')</script>\n" + long_out,
                                        "is_error": False, "cache_control": {"type": "ephemeral"}}]}]
    st, _, body3 = send(PROXY, "POST", "/v1/messages?beta=true", req(m3))
    time.sleep(0.2)
    paired = [e for e in ev.events if e[0] == "tool_paired" and e[1]["tool_use_id"] == tool_use["id"]]
    check(bool(paired), "tool_paired event emitted when the result arrives")
    if paired:
        p = paired[-1][1]
        check(p["call_exchange"] == 3 and p["result_exchange"] == 4, f"pair links reply #3 -> request #4 (got {p['call_exchange']} -> {p['result_exchange']})")
        check(p["entry"]["latency_ms"] is not None, "call->result latency measured")

    # conversation diff
    ex4 = json.loads(send(UI, "GET", "/api/exchanges/4")[2])
    c = ex4["conversation"]
    check(c["prev_id"] == 3 and c["common_prefix"] == 3 and c["message_count"] == 5,
          f"diff: request #4 extends #3, 2 new messages (got {c})")
    u = ex4["assembled"]["usage"]
    check(u == {"input_tokens": 12, "cache_read_input_tokens": 18000, "cache_creation_input_tokens": 500, "output_tokens": u["output_tokens"]}
          and u["output_tokens"] > 1, f"usage merges message_start + message_delta ({u})")
    check(ex4["cost_usd"] is not None, f"cost computed for priced model ({ex4['cost_usd']})")
    check(ex4["timing"]["ttft_ms"] is not None and ex4["timing"]["ttfb_ms"] is not None, "ttfb and ttft recorded")
    check(ex4["request_headers"].get("authorization", "").endswith("...") and SECRET not in json.dumps(ex4),
          "authorization/cookie redacted in stored exchange")

    # 4. rate limit then identical retry
    m4 = [{"role": "user", "content": "RATE_LIMIT_ME please"}]
    st1, h1, b1 = send(PROXY, "POST", "/v1/messages?beta=true", req(m4))
    st2, _, _ = send(PROXY, "POST", "/v1/messages?beta=true", req(m4))
    check(st1 == 429 and h1.get("retry-after") == "6", "429 forwarded with retry-after")
    check(st2 == 200, "identical retry succeeds")
    ex5 = json.loads(send(UI, "GET", "/api/exchanges/5")[2])
    ex6 = json.loads(send(UI, "GET", "/api/exchanges/6")[2])
    ei = ex5["error_info"] or {}
    check(ei.get("type") == "rate_limit_error" and ei.get("retry_after") == "6" and "requests-limit" in json.dumps(ei.get("ratelimit")),
          f"429 error_info has type, message, retry-after and ratelimit headers ({ei})")
    check(ex6["retry_of"] == 5 and ex5["retried_by"] == 6, "retry linked: #6 retry of #5")

    # 5. non-streaming and count_tokens
    st, _, b = send(PROXY, "POST", "/v1/messages", req(m1, stream=False))
    check(st == 200 and json.loads(b)["type"] == "message", "non-streaming JSON reply forwarded")
    st, _, b = send(PROXY, "POST", "/v1/messages/count_tokens?beta=true", {"model": MODEL, "messages": m1, "system": SYSTEM})
    check(st == 200 and "input_tokens" in json.loads(b), "count_tokens forwarded")
    ex7 = json.loads(send(UI, "GET", "/api/exchanges/7")[2])
    check(ex7["assembled"]["usage"]["output_tokens"] == 42, "non-streaming usage taken from JSON body")

    # 6. unknown model -> no price
    st, _, _ = send(PROXY, "POST", "/v1/messages", req([{"role": "user", "content": "hi"}], model="claude-imaginary-9"))
    ex9 = json.loads(send(UI, "GET", "/api/exchanges/9")[2])
    check(ex9["cost_usd"] is None, "unpriced model -> cost null ('—' in UI)")

    # 7. client drops mid-stream
    s = socket.create_connection(("127.0.0.1", PROXY))
    data = json.dumps(req([{"role": "user", "content": "SLOW please"}])).encode()
    s.sendall(b"POST /v1/messages HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n" % len(data) + data)
    time.sleep(0.6)
    s.close()
    time.sleep(1.5)
    st, _, _ = send(PROXY, "HEAD", "/api/hello")
    check(st == 200, "proxy still serves after a client drop")
    errs = [e for e in ev.events if e[0] == "exchange_error" and e[1]["id"] == 10]
    check(bool(errs) and "client disconnected" in errs[-1][1]["message"], "client drop reported as exchange_error")

    # 8. export + no secrets anywhere
    st, h, exp = send(UI, "GET", "/api/export")
    check(st == 200 and "traffic-" in h.get("Content-Disposition", ""), "export downloads traffic-<timestamp>.json")
    check(SECRET.encode() not in exp, "no full token in export")
    snap_c = http.client.HTTPConnection("127.0.0.1", UI, timeout=5)
    snap_c.request("GET", "/events")
    r = snap_c.getresponse()
    first = b""
    while b"\n\n" not in first.split(b"event: snapshot", 1)[-1] or b"event: snapshot" not in first:
        first += r.read1(65536)
    snap_c.close()
    check(SECRET.encode() not in first, "no full token in the snapshot")
    snap = json.loads(first.split(b"event: snapshot\ndata: ", 1)[1].split(b"\n\n", 1)[0])
    check(len(snap["exchanges"]) == 11 and tool_use["id"] in snap["tools"], "snapshot restores exchanges and tool pairs")
    names = {e[0] for e in ev.events}
    check({"snapshot", "exchange_started", "exchange_response", "exchange_delta", "exchange_done", "exchange_error", "tool_paired"} <= names,
          f"all spec event types seen ({sorted(names)})")

    print(f"\n{len(FAILS)} failure(s)")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
