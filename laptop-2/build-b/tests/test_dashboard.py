#!/usr/bin/env python3
"""End-to-end and unit checks against tests/fake_upstream.py. Starts and stops
its own servers on 8061 (proxy), 8062 (UI), 8063 (fake upstream), 8064 (proxy.py).

    python3 tests/test_dashboard.py
"""
import base64
import http.client
import importlib.util
import json
import queue
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROXY, UI, FAKE, REF = 8061, 8062, 8063, 8064
TOKEN = "sk-ant-oat01-THIS-IS-THE-FULL-SECRET-TOKEN-abcdefghijklmnop"
HEADERS = {"content-type": "application/json", "x-api-key": TOKEN, "authorization": "Bearer " + TOKEN,
           "cookie": "sid=SECRET-COOKIE-VALUE-0123456789", "anthropic-beta": "oauth-2025-04-20",
           "anthropic-version": "2023-06-01", "user-agent": "test/1"}
FAILS, PASSES = [], 0


def check(name, ok, detail=""):
    global PASSES
    if ok:
        PASSES += 1
        print(f"  ok   {name}")
    else:
        FAILS.append(name)
        print(f"  FAIL {name} {detail}")


def wait_port(port):
    for _ in range(100):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise SystemExit(f"nothing on {port}")


def body(prompt, system="You are a test.", model="claude-opus-5-5", history=None, **extra):
    messages = list(history or []) + ([{"role": "user", "content": prompt}] if prompt is not None else [])
    return {"model": model, "max_tokens": 100, "stream": True, "system": system, "messages": messages, **extra}


def call(payload, port=PROXY, path="/v1/messages?beta=true", method="POST", headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    data = payload if isinstance(payload, (bytes, type(None))) else json.dumps(payload).encode()
    conn.request(method, path, body=data, headers=dict(HEADERS if headers is None else headers))
    reply = conn.getresponse()
    out = reply.read()
    result = (reply.status, reply.getheaders(), out)
    conn.close()
    return result


def api(path, method="GET", headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", UI, timeout=20)
    conn.request(method, path, headers=headers or {})
    reply = conn.getresponse()
    out = reply.read()
    conn.close()
    return reply, out


def get_json(path):
    return json.loads(api(path)[1])


def latest():
    settle()
    return get_json("/api/export")[-1]


def settle():
    time.sleep(0.15)


class Events:
    """A browser stand-in: reads /events and timestamps each event."""

    def __init__(self):
        self.q = queue.Queue()
        self.sock = socket.create_connection(("127.0.0.1", UI))
        self.sock.sendall(f"GET /events HTTP/1.1\r\nHost: localhost:{UI}\r\n\r\n".encode())
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        buf = b""
        try:
            while chunk := self.sock.recv(65536):
                buf += chunk
                while b"\n\n" in buf:
                    block, buf = buf.split(b"\n\n", 1)
                    m = re.search(rb"event: (\w+)\ndata: (.*)", block, re.S)
                    if m:
                        self.q.put((time.monotonic(), m.group(1).decode(), json.loads(m.group(2))))
        except OSError:
            pass

    def until(self, name, predicate=lambda d: True, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                t, event, data = self.q.get(timeout=max(0.01, end - time.monotonic()))
            except queue.Empty:
                break
            if event == name and predicate(data):
                return t, data
        return None, None

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def unit_tests():
    print("unit")
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("dashboard", ROOT / "dashboard.py")
    d = importlib.util.module_from_spec(spec)
    sys.modules["dashboard"] = d
    spec.loader.exec_module(d)

    stream = ('event: a\ndata: {"t": "héllo ✓"}\n\n: note\n\nevent: b\r\ndata: {"x":1}\r\n\r\n'
              'data: line1\ndata: line2\n\ndata: [DONE]\n\n').encode()
    whole = d.SSEParser()
    expected = whole.feed(stream) + whole.close()
    same = True
    for cut in range(1, len(stream)):
        p = d.SSEParser()
        got = p.feed(stream[:cut]) + p.feed(stream[cut:]) + p.close()
        same = same and got == expected
    bytewise = d.SSEParser()
    got = [e for i in range(len(stream)) for e in bytewise.feed(stream[i:i + 1])] + bytewise.close()
    check("SSE parser same at every split point and byte by byte", same and got == expected)
    check("SSE parser keeps UTF-8 across splits", expected[0]["data"] == {"t": "héllo ✓"})
    check("SSE multi-line data, raw string, [DONE] not an error",
          expected[2]["data"] == "line1\nline2" and expected[2].get("parse_error") and "parse_error" not in expected[3])
    tail = d.SSEParser()
    check("SSE event with no closing blank line is flushed at the end",
          tail.feed(b'data: {"a":1}') == [] and tail.close()[0]["data"] == {"a": 1})

    check("redact keeps 16 chars", d.redact("x" * 40) == "x" * 16 + "...")
    check("redact hides a short secret entirely", d.redact("short") == "..." and d.redact("x" * 16) == "...")

    store = d.Store(3, d.Pricing(ROOT / "pricing.json"))
    sub, _, _ = store.subscribe()
    sub.queue = queue.Queue(maxsize=2)
    t = time.perf_counter()
    for i in range(10000):
        store.publish("x", {"i": i})
    took = time.perf_counter() - t
    check("publish to a full queue drops without blocking", sub.queue.qsize() == 2 and sub.lost_events and took < 0.5, f"{took:.3f}s")
    snapshot, _ = store.resync(sub)
    check("a subscriber that lost events is resynced", not sub.lost_events and sub.queue.empty() and snapshot == [])

    price = d.Pricing(ROOT / "pricing.json")
    usage = {"input_tokens": 1_000_000, "output_tokens": 1_000_000, "cache_read_input_tokens": 1_000_000, "cache_creation_input_tokens": 1_000_000}
    check("cost formula", price.cost("claude-opus-5-5", usage) == 4 + 20 + 0.2 + 5)
    check("dated model id matches", price.cost("claude-opus-5-5-20260101", usage) == 29.2)
    check("unknown model has no cost", price.cost("gpt-unknown", usage) is None and price.cost("_comment", usage) is None)

    a, b = {"role": "user", "content": "hi"}, {"role": "user", "content": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}]}
    check("message digest ignores cache_control and string/block form", d.message_digest(a) == d.message_digest(b))
    n = d.normalize_codex({"model": "m", "instructions": "SYS", "tools": [{"type": "function", "name": "shell", "description": "d", "parameters": {"type": "object"}}],
                           "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                                     {"type": "function_call", "call_id": "c", "name": "shell", "arguments": '{"a":1}'},
                                     {"type": "function_call_output", "call_id": "c", "output": "ok"}]})
    check("Codex request normalized", n["system"][0]["text"] == "SYS" and n["tools"][0]["input_schema"] == {"type": "object"}
          and n["messages"][1]["content"][0]["input"] == {"a": 1} and n["messages"][2]["content"][0]["tool_use_id"] == "c")


def forwarding_tests():
    print("forwarding is the same as proxy.py")
    cases = [("POST stream", body("hello", ), "/v1/messages?beta=true", "POST"),
             ("POST json", body("nonstream"), "/v1/messages", "POST"),
             ("POST 429", body("rate limit"), "/v1/messages", "POST"),
             ("POST not JSON", b"not json \xff at all", "/v1/other", "POST"),
             ("GET", None, "/v1/models?limit=2", "GET"),
             ("HEAD", None, "/v1/models", "HEAD")]
    for name, payload, path, method in cases:
        a = call(payload, port=PROXY, path=path, method=method)
        upstream_a = call(None, port=FAKE, path="/__last", method="GET")[2]
        b = call(payload, port=REF, path=path, method=method)
        upstream_b = call(None, port=FAKE, path="/__last", method="GET")[2]
        drop = {"date", "server"}
        heads_a = [h for h in a[1] if h[0].lower() not in drop]
        heads_b = [h for h in b[1] if h[0].lower() not in drop]
        check(f"{name}: same status, headers and bytes", a[0] == b[0] and heads_a == heads_b and a[2] == b[2], f"{a[1]} vs {b[1]}")
        if method == "POST":
            check(f"{name}: upstream saw the same request", json.loads(upstream_a) == json.loads(upstream_b))
    seen = json.loads(call(None, port=FAKE, path="/__last", method="GET")[2])
    call(body("hello"), port=PROXY)
    seen = json.loads(call(None, port=FAKE, path="/__last", method="GET")[2])
    names = {k.lower(): v for k, v in seen["headers"]}
    check("secrets reach the upstream unchanged", names["x-api-key"] == TOKEN and names["authorization"] == "Bearer " + TOKEN
          and names["cookie"] == HEADERS["cookie"])
    call(body("hello"), headers={**HEADERS, "accept-encoding": "gzip, br"})
    seen = json.loads(call(None, port=FAKE, path="/__last", method="GET")[2])
    names = {k.lower(): v for k, v in seen["headers"]}
    check("the agent's accept-encoding is dropped", names.get("accept-encoding") == "identity", names)
    raw_sent = json.dumps(body("exact bytes ✓"), indent=3).encode()
    call(raw_sent)
    seen = json.loads(call(None, port=FAKE, path="/__last", method="GET")[2])
    check("request bytes unchanged", base64.b64decode(seen["body"]) == raw_sent)
    ex = latest()
    check("Raw keeps the exact request text", ex["request_raw"].encode() == raw_sent)


def live_tests():
    print("live events")
    events = Events()
    t, snap = events.until("snapshot")
    check("snapshot comes first, and is a list", isinstance(snap, list))
    events.until("tool_index")
    result = {}
    thread = threading.Thread(target=lambda: result.update(r=call(body("slow one"))))
    sent = time.monotonic()
    thread.start()
    t, started = events.until("exchange_started", lambda d: "slow one" in json.dumps(d["body"]))
    check("exchange_started under 200 ms", t and t - sent < 0.2, t and t - sent)
    check("started payload has id, time, method, path, headers, body",
          all(k in started for k in ("id", "time", "method", "path", "headers", "body")))
    the_id = started["id"]
    t2, resp = events.until("exchange_response", lambda d: d["id"] == the_id)
    check("exchange_response has status and headers", resp["status"] == 200 and resp["headers"].get("X-Fake") == "1")
    t3, first = events.until("exchange_delta", lambda d: d["id"] == the_id and d["event"]["event"] == "content_block_delta")
    check("delta has the raw event", "raw" in first["event"] and first["event"]["data"]["type"] == "content_block_delta")
    alive = thread.is_alive()
    check("replies appear while still streaming", alive)
    live = get_json(f"/api/exchanges/{the_id}")
    check("an open exchange has ended_at null and partial text", live["ended_at"] is None and live["assembled"]["content"])
    late = Events()
    _, snap2 = late.until("snapshot")
    in_flight = [e for e in snap2 if e["id"] == the_id]
    check("a reload mid-stream gets the partial exchange in its snapshot", in_flight and in_flight[0]["ended_at"] is None)
    _, done_late = late.until("exchange_done", lambda d: d["id"] == the_id)
    check("and continues with live events", done_late is not None)
    late.close()
    t4, done = events.until("exchange_done", lambda d: d["id"] == the_id)
    thread.join()
    a = done["assembled"]
    check("assembled text", a["content"][0]["text"] == "Héllo wörld ✓ from the fake upstream.", a)
    check("usage is message_start with message_delta's output", a["usage"] == {
        "input_tokens": 12, "output_tokens": 340, "cache_read_input_tokens": 18000, "cache_creation_input_tokens": 500}, a["usage"])
    check("stop_reason, model", a["stop_reason"] == "end_turn" and a["model"] == "claude-opus-5-5")
    t_ = done["timing"]
    check("timing ttfb <= ttft <= total", 0 <= t_["ttfb_ms"] <= t_["ttft_ms"] <= t_["total_ms"] and t_["total_ms"] > 500, t_)
    want = (12 * 4 + 340 * 20 + 18000 * 0.2 + 500 * 5) / 1e6
    check("cost matches the formula", abs(done["cost_usd"] - want) < 1e-7, done["cost_usd"])
    full = get_json(f"/api/exchanges/{the_id}")
    keys = {"id", "started_at", "ended_at", "method", "path", "status", "request_headers", "request_body", "response_headers",
            "response_events", "assembled", "timing", "cost_usd", "error"}
    check("exchange has every field in §4", keys <= set(full), keys - set(full))
    check("raw lines of every event are kept", all("raw" in e for e in full["response_events"]) and len(full["response_events"]) > 5)
    check("the proxied client got it all", b"message_stop" in result["r"][2])
    events.close()


def tool_tests():
    print("tool pairing")
    events = Events()
    events.until("tool_index")
    system = "Tool test system."
    first = body("tool a1", system=system)
    call(first)
    _, update = events.until("tool_update", lambda d: d["tool_use_id"] == "toolu_a1")
    check("call indexed as pending", update and update["status"] == "pending" and update["name"] == "Bash")
    call_ex = latest()
    tool = next(b for b in call_ex["assembled"]["content"] if b["type"] == "tool_use")
    check("input assembled from pieces", tool["input"] == {"command": "ls -la", "nested": {"a": {"b": {"c": {"d": 1}}}}}, tool)
    check("thinking kept, in order", [b["type"] for b in call_ex["assembled"]["content"]] == ["thinking", "text", "tool_use"])
    check("no partial buffer left behind", "partial_chars" not in tool)

    other = body("hello", system="Another conversation, a subagent.")
    call(other)
    time.sleep(0.1)
    check("a parallel conversation does not move the call on", get_json("/api/tools")["toolu_a1"]["status"] == "pending")
    counted = dict(first)
    counted["messages"] = first["messages"] + [{"role": "assistant", "content": call_ex["assembled"]["content"]},
                                               {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_a1", "content": "from count"}]}]
    call(counted, path="/v1/messages/count_tokens")
    settle()
    check("count_tokens does not pair", get_json("/api/tools")["toolu_a1"]["status"] == "pending")

    result_block = {"type": "tool_result", "tool_use_id": "toolu_a1", "content": "<script>window.__pwned=1</script>", "is_error": True}
    history = first["messages"] + [{"role": "assistant", "content": call_ex["assembled"]["content"]},
                                   {"role": "user", "content": [dict(result_block, cache_control={"type": "ephemeral"})]}]
    time.sleep(0.05)
    call(dict(first, messages=history))
    _, paired = events.until("tool_paired", lambda d: d["tool_use_id"] == "toolu_a1")
    check("tool_paired with both exchange ids and result", paired and paired["call_exchange"] == call_ex["id"]
          and paired["result_exchange"] > call_ex["id"] and paired["result"] == result_block["content"])
    check("is_error, size, status, time", paired["is_error"] and paired["result_size"] == 33 and paired["status"] == "paired"
          and paired["call_to_result_ms"] >= 40, paired)
    result_ex = paired["result_exchange"]
    call(dict(first, messages=history + [{"role": "assistant", "content": "ok"}, {"role": "user", "content": "more"}]))
    settle()
    entry = get_json("/api/tools")["toolu_a1"]
    check("history resent later does not re-pair", entry["result_exchange"] == result_ex)
    keys = {"name", "input", "call_exchange", "result_exchange", "result", "is_error", "status"}
    check("index entry has every field in §4", keys <= set(entry))

    # Moves on with the call in history but no result for it.
    a2 = body("tool a2", system=system + " 2")
    call(a2)
    ex2 = latest()
    call(dict(a2, messages=a2["messages"] + [{"role": "assistant", "content": ex2["assembled"]["content"]}, {"role": "user", "content": "never mind"}]))
    settle()
    check("no_result when the history holds the call without a result", get_json("/api/tools")["toolu_a2"]["status"] == "no_result")

    # Moves on after dropping the reply altogether; a plain retry does not count.
    a3 = body("tool a3", system=system + " 3")
    call(a3)
    settle()
    call(a3)
    settle()
    check("a retry of the same request stays pending", get_json("/api/tools")["toolu_a3"]["status"] == "pending")
    call(dict(a3, messages=a3["messages"] + [{"role": "assistant", "content": "other"}, {"role": "user", "content": "go on"}]))
    settle()
    check("no_result when the conversation continues without the reply", get_json("/api/tools")["toolu_a3"]["status"] == "no_result")

    # A result for a call the index never saw.
    orphan = [{"role": "user", "content": "x"}, {"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_old", "name": "Read", "input": {}}]},
              {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_old", "content": "y"}]}]
    call(body(None, history=orphan))
    settle()
    check("a result with no known call is not indexed", "toolu_old" not in get_json("/api/tools"))

    # Non-streaming reply with a tool call.
    call(body("nonstream"))
    settle()
    ex = latest()
    check("non-streaming reply assembled and indexed", ex["assembled"]["content"][0]["text"] == "plain reply" and
          get_json("/api/tools")["toolu_ns"]["status"] == "pending" and ex["response_body"]["id"] == "msg_2")
    events.close()


def failure_tests():
    print("failures")
    status, _, out = call(body("badsse"))
    check("a bad SSE event does not stop forwarding", status == 200 and b"{not json" in out and b"message_stop" in out)
    ex = latest()
    bad = ex["response_events"][0]
    check("and is logged as a raw string", bad["data"] == "{not json" and bad["parse_error"])

    call(body("streamerror"))
    ex = latest()
    check("SSE error event recorded", "overloaded_error" in (ex["error"] or "") and ex["ended_at"], ex["error"])

    call(body("rate limit"))
    ex = latest()
    check("429 recorded with its body", ex["status"] == 429 and ex["response_body"]["error"]["type"] == "rate_limit_error")

    events = Events()
    events.until("tool_index")
    try:
        call(body("die"))
    except http.client.HTTPException:
        pass
    _, err = events.until("exchange_error", lambda d: "mid-stream" in d["message"] or "before its final" in d["message"])
    check("upstream dying mid-stream gives exchange_error", err is not None)
    check("and keeps the partial text", err and err["assembled"]["content"][0]["text"].startswith("Héllo"))

    # The client drops mid-stream.
    sock = socket.create_connection(("127.0.0.1", PROXY))
    payload = json.dumps(body("slow drop")).encode()
    sock.sendall(f"POST /v1/messages HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\n\r\n".encode() + payload)
    sock.recv(1000)
    sock.close()
    _, err = events.until("exchange_error", lambda d: d["message"] == "client disconnected", timeout=15)
    check("client dropping gives exchange_error", err is not None)
    events.close()

    # A browser closing mid-stream leaves the agent untouched.
    watcher = Events()
    watcher.until("tool_index")
    result = {}
    thread = threading.Thread(target=lambda: result.update(r=call(body("slow browser drop"))))
    thread.start()
    time.sleep(0.5)
    watcher.close()
    thread.join()
    check("closing the browser mid-stream does not affect the agent", result["r"][0] == 200 and b"message_stop" in result["r"][2])


def parallel_tests():
    print("parallel")
    results = {}

    def one(i):
        results[i] = call(body(f"parallel {i}", system=f"sys {i}"))

    threads = [threading.Thread(target=one, args=(i,)) for i in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    settle()
    check("all 30 parallel requests answered whole", all(r[0] == 200 and b"message_stop" in r[2] for r in results.values()))
    done = [e for e in get_json("/api/export") if str(e["request_body"]).find("parallel ") >= 0]
    check("30 complete, separate exchanges", len(done) == 30 and len({e["id"] for e in done}) == 30 and
          all(e["assembled"]["content"][0]["text"].endswith("upstream.") and e["assembled"]["stop_reason"] == "end_turn" for e in done))
    check("ids increase", [e["id"] for e in get_json("/api/export")] == sorted(e["id"] for e in get_json("/api/export")))

    # A stalled browser that never reads must not slow the agent.
    stalled = socket.create_connection(("127.0.0.1", UI))
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    stalled.sendall(f"GET /events HTTP/1.1\r\nHost: localhost:{UI}\r\n\r\n".encode())
    t = time.monotonic()
    for i in range(40):
        call(body("x" * 200000))
    with_stalled = time.monotonic() - t
    stalled.close()
    check("a stalled browser does not block forwarding", with_stalled < 15, f"{with_stalled:.2f}s")


def api_tests():
    print("endpoints")
    reply, out = api("/")
    check("GET / serves the page, with a CSP", reply.status == 200 and b"<title>" in out and "script-src" in reply.getheader("Content-Security-Policy"))
    reply, out = api("/api/exchanges/999999")
    check("unknown exchange is a 404", reply.status == 404)
    reply, out = api("/api/export")
    check("export is a JSON download named traffic-<timestamp>.json",
          re.search(r'attachment; filename="traffic-[\d-]+\.json"', reply.getheader("Content-Disposition") or "") and isinstance(json.loads(out), list))
    reply, _ = api("/", headers={"Host": "evil.example"})
    check("foreign Host is refused", reply.status == 403)
    reply, _ = api("/api/clear", method="POST", headers={"Origin": "https://evil.example"})
    check("cross-site clear is refused", reply.status == 403 and get_json("/api/export"))
    lsof = subprocess.run(["lsof", "-nP", f"-iTCP:{PROXY}", f"-iTCP:{UI}", "-sTCP:LISTEN"], capture_output=True, text=True).stdout
    check("both servers bind 127.0.0.1 only", lsof.count("127.0.0.1:") >= 2 and "*:" not in lsof, lsof)


def codex_tests():
    print("Codex")
    request = {"model": "gpt-5-codex", "instructions": "Codex system", "stream": True, "tools": [{"type": "function", "name": "shell", "description": "run", "parameters": {"type": "object"}}],
               "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}]}
    status, _, out = call(request, path="/responses")
    ex = latest()
    a = ex["assembled"]
    check("Codex reply assembled", status == 200 and a["content"][0]["text"] == "Codex says hi" and a["content"][1]["input"] == {"cmd": "ls"}, a)
    check("Codex usage normalized, cost is null for an unpriced model", a["usage"]["cache_read_input_tokens"] == 80 and a["usage"]["input_tokens"] == 20 and ex["cost_usd"] is None)
    check("Codex stop reason and ttft", a["stop_reason"] == "tool_use" and ex["timing"]["ttft_ms"] is not None)
    request["input"] = request["input"] + [{"type": "function_call", "call_id": "call_9", "name": "shell", "arguments": '{"cmd":"ls"}'},
                                           {"type": "function_call_output", "call_id": "call_9", "output": "a.txt"}]
    call(request, path="/responses")
    settle()
    entry = get_json("/api/tools")["call_9"]
    check("Codex call paired with its output", entry["status"] == "paired" and entry["result"] == "a.txt")


def secrets_test(logs):
    print("secrets")
    call(body("rate limit"))
    settle()
    export = api("/api/export")[1].decode()
    events = Events()
    _, snap = events.until("snapshot")
    events.close()
    blob = export + json.dumps(snap) + logs()
    check("no full token in exports, events or logs", TOKEN not in blob and "SECRET-COOKIE-VALUE" not in blob and "SECRETCOOKIEVALUE" not in blob)
    ex = next(e for e in get_json("/api/export") if e["status"] == 429)
    check("redaction shows 16 characters then ...", ex["request_headers"]["x-api-key"] == TOKEN[:16] + "..." and
          ex["request_headers"]["authorization"] == ("Bearer " + TOKEN)[:16] + "..." and ex["request_headers"]["cookie"] == HEADERS["cookie"][:16] + "...")
    check("Set-Cookie in replies is redacted too", ex["response_headers"]["Set-Cookie"].endswith("...") and "SECRETCOOKIE" not in ex["response_headers"]["Set-Cookie"])


def clear_and_ring_tests():
    print("clear and ring buffer")
    events = Events()
    events.until("tool_index")
    reply, _ = api("/api/clear", method="POST")
    _, snap = events.until("snapshot")
    check("clear empties the buffer and tells browsers", reply.status == 200 and get_json("/api/export") == [] and snap == [] and get_json("/api/tools") == {})
    thread = threading.Thread(target=lambda: call(body("slow in flight")))
    thread.start()
    time.sleep(0.4)
    api("/api/clear", method="POST")
    thread.join()
    settle()
    check("clearing during a stream is safe, nothing comes back", get_json("/api/export") == [])
    call(body("after clear"))
    ex = latest()
    check("ids keep increasing after clear", ex["id"] > 1)
    first = ex["id"]
    for i in range(205):
        call(body(f"ring {i}"))
    settle()
    export = get_json("/api/export")
    check("ring buffer holds the last 200", len(export) == 200 and export[0]["id"] == first + 6 and export[-1]["id"] == first + 205, (len(export), export[0]["id"], first))
    _, evicted = events.until("exchange_evicted", timeout=2)
    check("eviction is announced", evicted is not None)
    events.close()


def main():
    unit_tests()
    procs = []
    try:
        procs.append(subprocess.Popen([sys.executable, str(ROOT / "tests/fake_upstream.py"), "--port", str(FAKE)]))
        wait_port(FAKE)
        procs.append(subprocess.Popen([sys.executable, "-B", str(ROOT / "proxy.py"), "--port", str(REF), "--upstream", f"http://127.0.0.1:{FAKE}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        wait_port(REF)
        log = open(ROOT / "tests/.dashboard.log", "w+")
        main_proc = subprocess.Popen([sys.executable, "-B", "-u", str(ROOT / "dashboard.py"), "--port", str(PROXY), "--ui-port", str(UI), "--upstream", f"http://127.0.0.1:{FAKE}"],
                                     stdout=log, stderr=subprocess.STDOUT)
        procs.append(main_proc)
        wait_port(PROXY)
        wait_port(UI)
        logs = lambda: Path(log.name).read_text("utf-8", "replace")
        banner = logs()
        print("startup")
        check("prints both URLs and the run line", f"http://localhost:{PROXY}" in banner and f"http://localhost:{UI}" in banner
              and f"ANTHROPIC_BASE_URL=http://localhost:{PROXY} claude\n" in banner, banner)
        forwarding_tests()
        check("terminal shows traffic like proxy.py", ">>> POST /v1/messages" in logs() and "<<< 200 OK" in logs() and "--- done" in logs())
        live_tests()
        tool_tests()
        failure_tests()
        parallel_tests()
        api_tests()
        codex_tests()
        secrets_test(logs)
        clear_and_ring_tests()
        check("no recorder errors logged", "dashboard:" not in logs() and "Traceback" not in logs(), re.findall(r"dashboard:.*", logs())[:3])
        main_proc.terminate()
        main_proc.wait()
        log.close()

        # Same machine, --quiet, unreachable upstream.
        log2 = open(ROOT / "tests/.dashboard.log", "w+")
        quiet = subprocess.Popen([sys.executable, "-B", "-u", str(ROOT / "dashboard.py"), "--port", str(PROXY), "--ui-port", str(UI), "--quiet", "--upstream", "http://127.0.0.1:9"], stdout=log2, stderr=subprocess.STDOUT)
        procs.append(quiet)
        wait_port(PROXY)
        wait_port(UI)
        print("quiet, upstream unreachable")
        status, _, out = call(body("hello"))
        ex = latest()
        check("502 when the upstream is unreachable", status == 502 and b"cannot reach" in out)
        check("error recorded, status stays null", ex["status"] is None and "upstream unreachable" in ex["error"] and ex["ended_at"])
        text = Path(log2.name).read_text()
        check("--quiet prints no traffic", ">>>" not in text and "!!!" not in text and "hello" not in text, text)
        check("--quiet still prints the run line", f"ANTHROPIC_BASE_URL=http://localhost:{PROXY} claude" in text)
        log2.close()
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()
        Path(ROOT / "tests/.dashboard.log").unlink(missing_ok=True)
    print(f"\n{PASSES} passed, {len(FAILS)} failed")
    for f in FAILS:
        print("  FAILED:", f)
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
