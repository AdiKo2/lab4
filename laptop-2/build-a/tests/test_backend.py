#!/usr/bin/env python3
"""End-to-end tests of the proxy and dashboard server against the fake upstream.

    python3 tests/test_backend.py
Uses ports 8051 (proxy) and 8052 (UI). Starts and stops everything it needs.
"""
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.dont_write_bytecode = True
PROXY, UI = 8051, 8052
TOKEN = "sk-ant-api03-FULLSECRETTOKENVALUE-abcdefghijklmnopqrstuvwxyz"
COOKIE = "session=COOKIESECRETVALUE-abcdefghijklmnopqrstuvwxyz"
SECRET_TAILS = ["FULLSECRETTOKENVALUE", "COOKIESECRETVALUE", "SETCOOKIESECRETVALUE", "abcdefghijklmnopqrstuvwxyz"]
FAILED, PASSED = [], []


def check(name, ok, detail=""):
    (PASSED if ok else FAILED).append(name)
    print(("ok    " if ok else "FAIL  ") + name + (f"  [{detail}]" if detail and not ok else ""), flush=True)


class Proc:
    def __init__(self, *cmd):
        self.log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
        self.p = subprocess.Popen(cmd, stdout=self.log, stderr=subprocess.STDOUT, cwd=ROOT,
                                  env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"})

    def output(self):
        with open(self.log.name, encoding="utf-8", errors="replace") as f:
            return f.read()

    def stop(self):
        self.p.terminate()
        self.p.wait(5)
        os.unlink(self.log.name)


def wait_port(port):
    for _ in range(100):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"port {port} never opened")


def start_dashboard(upstream, *flags):
    proc = Proc(sys.executable, "dashboard.py", "--port", str(PROXY), "--ui-port", str(UI), "--upstream", upstream, *flags)
    wait_port(PROXY); wait_port(UI)
    return proc


def send(text="hi", path="/v1/messages?beta=true", messages=None, system="You are a test.", model="claude-opus-5-5", port=PROXY, **extra):
    body = {"model": model, "max_tokens": 100, "stream": True, "system": system,
            "tools": [{"name": "Bash", "description": "Run", "input_schema": {"type": "object"}}],
            "messages": messages or [{"role": "user", "content": text}], **extra}
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request("POST", path, json.dumps(body), {"Content-Type": "application/json", "x-api-key": TOKEN, "authorization": "Bearer " + TOKEN, "cookie": COOKIE})
    reply = conn.getresponse()
    data = reply.read()
    conn.close()
    return reply, data


def api(path, method="GET", port=UI, headers=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.getheaders()), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


class SSE(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.events, self.times, self.raw = [], [], []
        self.sock = socket.create_connection(("127.0.0.1", UI))
        self.sock.sendall(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.start()
        self.wait(lambda: any(n == "snapshot" for n, _ in self.events))

    def run(self):
        buf = b""
        try:
            while chunk := self.sock.recv(65536):
                buf += chunk
                while b"\n\n" in buf:
                    block, buf = buf.split(b"\n\n", 1)
                    text = block.decode().split("\r\n\r\n")[-1]
                    self.raw.append(text)
                    m = re.match(r"event: (\w+)\ndata: (.*)", text, re.S)
                    if m:
                        self.times.append(time.time())
                        self.events.append((m.group(1), json.loads(m.group(2))))
        except OSError:
            pass

    def wait(self, cond, timeout=8):
        end = time.time() + timeout
        while time.time() < end:
            if cond():
                return True
            time.sleep(0.01)
        return False

    def named(self, name):
        return [p for n, p in self.events if n == name]

    def close(self):
        self.sock.close()


def exchanges():
    return json.loads(api("/api/export")[2])


def last_exchange():
    return max(exchanges(), key=lambda x: x["id"])


def wait_done(xid=None):
    for _ in range(300):
        items = exchanges()
        if items and all(x["ended_at"] for x in items) and (xid is None or any(x["id"] == xid for x in items)):
            return items
        time.sleep(0.02)
    raise RuntimeError("exchanges did not finish")


def normalize_terminal(text):
    text = re.sub(r"\[\d\d:\d\d:\d\d\]", "[T]", text)
    # Date and Host differ by run; the proxy port was the same in both.
    return "\n".join(l for l in text.splitlines() if not l.startswith(("proxy listening", "run the agent", "dashboard on", "ANTHROPIC_BASE_URL")))


def test_identical_to_proxy(upstream, fake):
    """AC1 and §8: same request through proxy.py and dashboard.py, compare everything."""
    sent = [
        ("POST", "/v1/messages?beta=true", json.dumps({"model": "claude-opus-5-5", "stream": True, "messages": [{"role": "user", "content": "TOOL t1"}]}).encode()),
        ("POST", "/v1/messages", json.dumps({"model": "claude-opus-5-5", "messages": [{"role": "user", "content": "NONSTREAM"}]}).encode()),
        ("POST", "/v1/messages", json.dumps({"model": "claude-opus-5-5", "messages": [{"role": "user", "content": "ERR429"}]}).encode()),
        ("POST", "/not/json", b"plain body"),
        ("GET", "/v1/models", b""),
        ("HEAD", "/v1/models", b""),
    ]

    def run(cmd):
        proc = Proc(*cmd)
        wait_port(PROXY)
        before = len(json.loads(urllib.request.urlopen(f"http://127.0.0.1:{fake}/__received").read()))
        replies = []
        for method, path, body in sent:
            conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=20)
            conn.request(method, path, body or None, {"Content-Type": "application/json", "x-api-key": TOKEN, "accept-encoding": "gzip", "anthropic-beta": "b1"})
            r = conn.getresponse()
            headers = [(k, v) for k, v in r.getheaders() if k.lower() != "date"]
            replies.append((r.status, r.reason, headers, r.read()))
            conn.close()
        seen = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{fake}/__received").read())[before:]
        time.sleep(0.2)
        out = proc.output()
        proc.stop()
        return replies, seen, out

    a = run([sys.executable, "proxy.py", "--port", str(PROXY), "--upstream", upstream])
    b = run([sys.executable, "dashboard.py", "--port", str(PROXY), "--ui-port", str(UI), "--upstream", upstream])
    check("AC1 responses byte-identical to proxy.py (status, reason, headers, body)", a[0] == b[0], f"{a[0]!r} vs {b[0]!r}")
    check("AC1 upstream receives the same request as via proxy.py", a[1] == b[1], f"{a[1]!r} vs {b[1]!r}")
    # A fixed Set-Cookie is echoed only as a status line in the terminal, so the print comparison holds.
    check("§3.2 terminal output same as proxy.py when not quiet", normalize_terminal(a[2]) == normalize_terminal(b[2]))


def test_startup(upstream):
    proc = start_dashboard(upstream, "--quiet")
    time.sleep(0.2)
    out = proc.output()
    check("§3.2 banner has both URLs and the agent line",
          f"http://localhost:{PROXY}" in out and f"http://localhost:{UI}" in out
          and f"ANTHROPIC_BASE_URL=http://localhost:{PROXY} claude" in out.splitlines())
    listing = subprocess.run(["lsof", "-nP", "-a", "-p", str(proc.p.pid), "-iTCP", "-sTCP:LISTEN"], capture_output=True, text=True).stdout
    check("§7 both ports bind to 127.0.0.1 only", f"127.0.0.1:{PROXY}" in listing and f"127.0.0.1:{UI}" in listing and "*:" not in listing, listing)
    send("hello")
    time.sleep(0.2)
    check("§3.2 --quiet prints no traffic", ">>>" not in proc.output() and "message_start" not in proc.output())
    proc.stop()


def test_events_and_data(upstream):
    proc = start_dashboard(upstream)
    sse = SSE()
    check("§5 snapshot arrives first after connecting", sse.events[0][0] == "snapshot" and sse.events[0][1] == [])

    t0 = time.time()
    reply, data = send("TOOL first")
    sse.wait(lambda: sse.named("exchange_done"))
    started = sse.named("exchange_started")[0]
    check("§5 exchange_started payload", {"id", "time", "method", "path", "headers", "body"} <= set(started), str(started.keys()))
    first_at = sse.times[1]
    check("AC2 started event reaches a subscriber within 200ms", first_at - t0 < 0.2, f"{first_at - t0:.3f}s")
    check("§5 exchange_response payload", {"id", "status", "headers"} <= set(sse.named("exchange_response")[0]))
    delta = sse.named("exchange_delta")
    check("§5 one exchange_delta per upstream event", len(delta) == data.count(b"event: ") and all("event" in d for d in delta))
    done = sse.named("exchange_done")[0]
    check("§5 exchange_done payload", {"id", "ended_at", "assembled"} <= set(done))

    x = json.loads(api(f"/api/exchanges/{started['id']}")[2])
    check("§4 exchange has every field", {"id", "started_at", "ended_at", "method", "path", "status", "request_headers", "request_body",
          "response_headers", "response_events", "assembled", "timing", "cost_usd", "error"} <= set(x))
    check("§4 started_at format", re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", x["started_at"]) is not None, x["started_at"])
    blocks = x["assembled"]["content"]
    check("§6.3 blocks kept in order", [b["type"] for b in blocks] == ["thinking", "text", "tool_use"], str(blocks))
    check("§6.3 tool input assembled from fragments", blocks[2]["input"]["command"] == "ls -la" and "partial_json" not in blocks[2])
    check("§6.3 text assembled", blocks[1]["text"] == "Let me check.")
    check("§6.3 stop_reason", x["assembled"]["stop_reason"] == "tool_use")
    u = x["assembled"]["usage"]
    check("AC4 usage matches message_start and message_delta",
          (u["input_tokens"], u["cache_read_input_tokens"], u["cache_creation_input_tokens"], u["output_tokens"]) == (12, 18000, 500, 50), str(u))
    expected = (12 * 4 + 50 * 20 + 18000 * 0.2 + 500 * 5) / 1e6
    check("§6.5 cost formula", abs(x["cost_usd"] - expected) < 1e-9, f"{x['cost_usd']} vs {expected}")
    t = x["timing"]
    check("§4 timing filled and ordered", 0 <= t["ttfb_ms"] <= t["ttft_ms"] <= t["total_ms"], str(t))
    tool = [p for p in sse.named("tool_update") if p["tool_use_id"] == "toolu_first"]
    check("§4 tool starts pending", tool and tool[0]["entry"]["status"] == "pending" and tool[0]["entry"]["call_exchange"] == x["id"])

    # The next turn carries the result.
    assistant = {"role": "assistant", "content": blocks}
    history = [{"role": "user", "content": "TOOL first"}, assistant,
               {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_first", "content": "file list", "is_error": False, "cache_control": {"type": "ephemeral"}}]}]
    send(messages=history)
    sse.wait(lambda: sse.named("tool_paired"))
    paired = sse.named("tool_paired")[0]
    check("§5 tool_paired payload", paired["tool_use_id"] == "toolu_first" and paired["call_exchange"] == x["id"]
          and paired["result_exchange"] == x["id"] + 1 and paired["result"] == "file list")
    entry = paired["entry"]
    check("§4 index entry paired", entry["status"] == "paired" and entry["is_error"] is False and entry["result_size"] == 9
          and entry["result_message_index"] == 2 and entry["wait_ms"] >= 0, str(entry))
    follow = last_exchange()
    check("AC3 diff basis: previous request and only the 2 new messages", follow["conversation"] == {"prev_exchange": x["id"], "new_from": 1}, str(follow["conversation"]))
    check("AC4 repeated prompt gives a high cache hit", follow["assembled"]["usage"]["cache_read_input_tokens"] / (12 + 18000 + 500) > 0.9)

    # A third turn resends the history with cache_control moved around.
    moved = json.loads(json.dumps(history))
    del moved[2]["content"][0]["cache_control"]
    moved[0] = {"role": "user", "content": [{"type": "text", "text": "TOOL first", "cache_control": {"type": "ephemeral"}}]}
    send(messages=moved + [{"role": "assistant", "content": "ok"}, {"role": "user", "content": "more"}])
    third = last_exchange()
    check("§6.7 moved cache_control still matches the same conversation", third["conversation"] == {"prev_exchange": follow["id"], "new_from": 3}, str(third["conversation"]))
    check("§6.4 a resent result does not pair twice", len([p for p in sse.named("tool_paired") if p["tool_use_id"] == "toolu_first"]) == 1)
    send("other", system="A different system prompt")
    check("§6.7 different system prompt is a different conversation", last_exchange()["conversation"]["prev_exchange"] is None)

    # no_result: the conversation moves on without a result; a retry has not moved on.
    send("TOOL lost")
    wait_done()
    lost_reply = last_exchange()
    send("TOOL lost")
    wait_done()
    index = lambda: [p["entry"]["status"] for p in sse.named("tool_update") if p["tool_use_id"] == "toolu_lost"]
    sse.wait(lambda: index())
    check("§6.4 a retry keeps the call pending", index()[-1] == "pending", str(index()))
    send(messages=[{"role": "user", "content": "TOOL lost"}, {"role": "assistant", "content": lost_reply["assembled"]["content"]}, {"role": "user", "content": "never mind"}])
    sse.wait(lambda: "no_result" in index())
    check("§6.4 moving on marks no_result", index()[-1] == "no_result", str(index()))
    send(messages=[{"role": "user", "content": "TOOL lost"}, {"role": "assistant", "content": lost_reply["assembled"]["content"]},
                   {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_lost", "content": [{"type": "text", "text": "late"}], "is_error": True}]}])
    sse.wait(lambda: any(p["tool_use_id"] == "toolu_lost" for p in sse.named("tool_paired")))
    late = [p for p in sse.named("tool_paired") if p["tool_use_id"] == "toolu_lost"]
    check("§6.4 a late result still pairs, with is_error", late and late[0]["entry"]["status"] == "paired" and late[0]["entry"]["is_error"] is True)

    # Unpriced and non-streaming.
    send("hello", model="mystery-model-9")
    x = wait_done()[-1]
    check("§4 unknown model has null cost", x["assembled"]["model"] == "mystery-model-9" and x["cost_usd"] is None)
    send("hello", model="claude-opus-5-5-20260101")
    check("§6.5 dated model id uses its base price", wait_done()[-1]["cost_usd"] is not None)
    send("NONSTREAM")
    x = wait_done()[-1]
    check("non-streaming reply is assembled", x["assembled"]["content"][0]["text"] == "plain reply" and x["response_events"] == [] and x["assembled"]["usage"]["output_tokens"] == 7)
    check("non-streaming tool call registers", any(p["tool_use_id"] == "toolu_nonstream" for p in sse.named("tool_update")))
    send("ERR429")
    x = wait_done()[-1]
    check("HTTP error keeps status and body", x["status"] == 429 and x["response_body"]["error"]["type"] == "rate_limit_error" and x["cost_usd"] is None)

    # Failures of parsing and streaming.
    reply, data = send("BADSSE")
    x = wait_done()[-1]
    bad = x["response_events"][1]
    check("§8 bad SSE kept as a raw string, forwarding continues", bad.get("parse_error") and "not json" in bad["raw"] and b"not json" in data and x["assembled"]["content"][0]["text"] == "survived")
    send("UTF8SPLIT")
    x = wait_done()[-1]
    check("multi-byte text split across chunks survives", x["assembled"]["content"][0]["text"] == "héllo 😀", str(x["assembled"]["content"]))
    send("SSEERROR")
    x = wait_done()[-1]
    check("SSE error event sets error and exchange_error", "overloaded_error" in (x["error"] or "") and any(p["id"] == x["id"] for p in sse.named("exchange_error")))
    try:
        send("KILL")
    except http.client.HTTPException:
        pass
    x = wait_done()[-1]
    check("§5 upstream dropping mid-stream gives exchange_error", x["error"] is not None and x["ended_at"] is not None and x["assembled"]["content"][0]["text"] == "partial", str(x["error"]))

    # The client drops mid-stream.
    sock = socket.create_connection(("127.0.0.1", PROXY))
    body = json.dumps({"model": "claude-opus-5-5", "messages": [{"role": "user", "content": "SLOW"}]}).encode()
    sock.sendall(b"POST /v1/messages HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
    sock.recv(1000)
    sock.close()
    ok = sse.wait(lambda: any("client disconnected" in p["message"] for p in sse.named("exchange_error")))
    check("§5 client dropping gives exchange_error", ok)
    wait_done()

    # Other requests are captured too.
    api("/v1/models", port=PROXY)
    x = wait_done()[-1]
    check("GET is captured", x["method"] == "GET" and x["status"] == 200 and x["response_body"]["data"][0]["id"])
    conn = http.client.HTTPConnection("127.0.0.1", PROXY)
    conn.request("POST", "/v1/messages", b"not json", {"x-api-key": TOKEN})
    conn.getresponse().read()
    conn.close()
    x = wait_done()[-1]
    check("§4 non-JSON body stored as a string", x["request_body"] == "not json")

    # Codex.
    body = {"model": "gpt-x", "instructions": "Codex system", "stream": True, "tools": [{"type": "function", "name": "shell", "description": "run", "parameters": {"type": "object"}}],
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "TOOL go"}]}]}
    conn = http.client.HTTPConnection("127.0.0.1", PROXY)
    conn.request("POST", "/responses", json.dumps(body), {"authorization": "Bearer " + TOKEN})
    conn.getresponse().read()
    conn.close()
    x = wait_done()[-1]
    check("Codex tool call assembled", x["assembled"]["content"][0]["id"] == "call_cx" and x["assembled"]["content"][0]["input"] == {"cmd": "ls"} and x["assembled"]["stop_reason"] == "tool_use", str(x["assembled"]))
    check("Codex usage mapped", x["assembled"]["usage"]["cache_read_input_tokens"] == 80 and x["assembled"]["usage"]["input_tokens"] == 20)
    check("Codex request has a Messages-shaped view", x["view"]["system"] == "Codex system" and x["view"]["tools"][0]["name"] == "shell" and x["view"]["messages"][0]["content"][0]["text"] == "TOOL go")
    check("Codex unpriced model has null cost", x["cost_usd"] is None)
    body["input"] += [{"type": "function_call", "call_id": "call_cx", "name": "shell", "arguments": '{"cmd": "ls"}'},
                      {"type": "function_call_output", "call_id": "call_cx", "output": "a.txt"}]
    conn = http.client.HTTPConnection("127.0.0.1", PROXY)
    conn.request("POST", "/responses", json.dumps(body), {"authorization": "Bearer " + TOKEN})
    conn.getresponse().read()
    conn.close()
    x = wait_done()[-1]
    sse.wait(lambda: any(p["tool_use_id"] == "call_cx" for p in sse.named("tool_paired")))
    check("Codex tool result pairs across exchanges", any(p["tool_use_id"] == "call_cx" and p["result"] == "a.txt" for p in sse.named("tool_paired")))
    check("Codex diff finds the previous request", x["conversation"]["new_from"] == 1 and x["conversation"]["prev_exchange"] == x["id"] - 1, str(x["conversation"]))

    # Parallel requests.
    before = {e["id"] for e in exchanges()}
    results, threads = [], []
    def one(i):
        _, data = send(f"parallel {i}")
        results.append(b"message_stop" in data)
    for i in range(30):
        threads.append(threading.Thread(target=one, args=(i,)))
    [t.start() for t in threads]; [t.join() for t in threads]
    items = wait_done()
    fresh = [e for e in items if e["id"] not in before]
    good = all(e["assembled"]["content"][0]["text"] == "Hello from the fake." and e["request_body"]["messages"][0]["content"].startswith("parallel") for e in fresh)
    check("§8 30 parallel requests all recorded intact", len(results) == 30 and all(results) and len(fresh) == 30 and good and len({e["id"] for e in fresh}) == 30)

    # Secrets.
    terminal = proc.output()
    export = api("/api/export")[2].decode()
    events = "\n".join(sse.raw)
    leaks = [t for t in SECRET_TAILS for where in (terminal, export, events) if t in where]
    check("AC6 no full token in terminal, export or SSE", not leaks, str(leaks))
    x = exchanges()[0]
    check("§7 headers show first 16 chars then ...", x["request_headers"]["x-api-key"] == TOKEN[:16] + "..."
          and x["request_headers"]["authorization"] == ("Bearer " + TOKEN)[:16] + "..." and x["request_headers"]["cookie"] == COOKIE[:16] + "...", str(x["request_headers"]))
    received = json.loads(urllib.request.urlopen(f"{upstream}/__received").read())
    got = dict(received[-1]["headers"])
    check("§7 upstream still gets the full token", got.get("authorization") == "Bearer " + TOKEN, str(got))

    # Endpoints.
    status, headers, body = api("/api/export")
    check("§6.9 export is a timestamped JSON download", status == 200 and re.search(r'attachment; filename="traffic-\d{8}-\d{6}\.json"', headers["Content-Disposition"]) and isinstance(json.loads(body), list))
    check("§5 unknown exchange is 404", api("/api/exchanges/99999")[0] == 404)
    status, headers, page = api("/")
    check("§5 GET / serves the page, no external references", status == 200 and b"<title>" in page and not re.search(rb'(src|href)="https?://', page))
    check("§7 foreign Host and foreign Origin refused", api("/", headers={"Host": "evil.example"})[0] == 403
          and api("/api/clear", "POST", headers={"Origin": "http://evil.example"})[0] == 403 and len(exchanges()) > 0)

    second = SSE()
    snapshot = second.events[0][1]
    check("AC5 a new connection gets all history in the snapshot", len(snapshot) == len(exchanges()) and all(e["ended_at"] for e in snapshot))
    check("AC5 the tool index follows the snapshot", second.events[1][0] == "tool_index" and "toolu_first" in second.events[1][1])
    second.close()
    status, _, _ = api("/api/clear", "POST")
    sse.wait(lambda: sse.named("cleared"))
    check("§5 clear empties the buffer and tells browsers", status == 200 and exchanges() == [] and sse.named("cleared"))
    send("hello")
    x = wait_done()[-1]
    check("ids keep increasing after clear", x["id"] > max(p["id"] for p in sse.named("exchange_started")[:-1]))
    sse.close()
    proc.stop()


def test_never_delays(upstream):
    """§8: chunks arrive as they land, with or without a stalled browser."""
    proc = start_dashboard(upstream, "--quiet")

    def chunk_gaps():
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=30)
        conn.request("POST", "/v1/messages", json.dumps({"model": "claude-opus-5-5", "messages": [{"role": "user", "content": "SLOW"}]}), {"x-api-key": TOKEN})
        r = conn.getresponse()
        stamps = []
        while chunk := r.read1(65536):
            if b"text_delta" in chunk:
                stamps.append(time.time())
        conn.close()
        return [b - a for a, b in zip(stamps, stamps[1:])]

    gaps = chunk_gaps()
    check("§8 streaming: deltas arrive spaced, not in one burst", len(gaps) == 11 and all(0.15 < g < 0.45 for g in gaps), str(gaps))

    # A browser that connects and never reads.
    stalled = socket.socket()
    stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
    stalled.connect(("127.0.0.1", UI))
    stalled.sendall(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
    # Fill its pipes with large exchanges.
    big = "x" * 200_000
    for _ in range(20):
        send("hello", system=big)
    gaps = chunk_gaps()
    check("§8 a stalled browser does not slow the agent", len(gaps) == 11 and all(0.15 < g < 0.45 for g in gaps), str(gaps))

    # AC5: leaving during a stream does not touch the agent.
    watcher = SSE()
    result = {}
    def agent():
        result["reply"], result["data"] = send("SLOW")
    t = threading.Thread(target=agent); t.start()
    time.sleep(0.5)
    watcher.close()
    reload_watcher = SSE()
    inflight = [x for x in reload_watcher.events[0][1] if not x["ended_at"]]
    t.join()
    check("AC5 closing a browser mid-stream does not affect the agent", b"message_stop" in result["data"] and result["data"].count(b"text_delta") == 12)
    check("AC5 reconnecting mid-stream shows the partial reply, then the rest", len(inflight) == 1 and inflight[0]["assembled"]["content"][0]["text"].startswith("word0 "))
    reload_watcher.wait(lambda: any(p["id"] == inflight[0]["id"] for p in reload_watcher.named("exchange_done")))
    done = [p for p in reload_watcher.named("exchange_done") if p["id"] == inflight[0]["id"]]
    check("AC5 the reconnected stream finishes", done and done[0]["assembled"]["content"][0]["text"].count("word") == 12)
    reload_watcher.close()
    stalled.close()
    proc.stop()


def test_unreachable():
    with socket.socket() as s:  # a port nothing listens on
        s.bind(("127.0.0.1", 0))
        dead = s.getsockname()[1]
    proc = start_dashboard(f"http://127.0.0.1:{dead}", "--quiet")
    reply, _ = send("hi")
    x = wait_done()[-1]
    check("§4 unreachable upstream: 502 to the client, error recorded, status null",
          reply.status == 502 and x["error"].startswith("upstream unreachable") and x["status"] is None and x["ended_at"] is not None, str(x["error"]))
    proc.stop()


def test_ring(upstream):
    proc = start_dashboard(upstream, "--quiet", "--buffer", "5")
    sse = SSE()
    send("TOOL gone")
    wait_done()
    for i in range(6):
        send(f"n{i}")
    items = wait_done()
    check("§4 ring buffer keeps the newest", [e["id"] for e in items] == list(range(3, 8)), str([e["id"] for e in items]))
    sse.wait(lambda: len(sse.named("exchange_evicted")) == 2)
    check("eviction tells browsers", [p["id"] for p in sse.named("exchange_evicted")] == [1, 2])
    second = SSE()
    check("evicted tool calls leave the index", "toolu_gone" not in second.events[1][1])
    second.close(); sse.close()
    proc.stop()
    default = subprocess.run([sys.executable, "dashboard.py", "--help"], capture_output=True, text=True, cwd=ROOT, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}).stdout
    check("--help lists the CLI flags", all(f in default for f in ("--port", "--ui-port", "--upstream", "--quiet")))


def test_bus_units():
    """§8: publishing to a full subscriber does not block, and the browser recovers."""
    import dashboard as d
    rec = d.Recorder(10, d.Pricing(os.path.join(ROOT, "pricing.json")))
    sub = d.Subscriber()
    sub.queue = d.queue.Queue(maxsize=3)
    rec.subscribers.append(sub)
    t0 = time.perf_counter()
    for i in range(10000):
        rec.publish("x", {"i": i})
    check("§8 publishing to a full queue does not block", time.perf_counter() - t0 < 2 and sub.queue.qsize() == 3 and sub.dropped)
    rec.on_resync(sub)
    first = sub.queue.get_nowait()
    check("after a drop the browser gets a fresh snapshot", first.startswith(b"event: snapshot") and not sub.dropped)
    p = d.Pricing(os.path.join(ROOT, "pricing.json"))
    check("§6.5 pricing rejects an incomplete entry", p.cost("nope", {"input_tokens": 1}) is None and p.cost("_note", {"input_tokens": 1}) is None)
    check("§7 short secrets are not revealed", d.redact("abcdefgh") == "abcd..." and d.redact("x" * 40) == "x" * 16 + "...")


def main():
    fake = subprocess.Popen([sys.executable, os.path.join(ROOT, "tests", "fake_upstream.py")], stdout=subprocess.PIPE, text=True,
                            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    port = int(fake.stdout.readline())
    upstream = f"http://127.0.0.1:{port}"
    try:
        test_bus_units()
        test_identical_to_proxy(upstream, port)
        test_startup(upstream)
        test_events_and_data(upstream)
        test_never_delays(upstream)
        test_unreachable()
        test_ring(upstream)
    finally:
        fake.terminate()
        fake.wait(5)
    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    for name in FAILED:
        print("  FAILED:", name)
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
