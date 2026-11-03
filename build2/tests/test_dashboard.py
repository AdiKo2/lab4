#!/usr/bin/env python3
"""Backend checks against the fake upstream. Spends no tokens.

    python3 tests/test_dashboard.py

Starts tests/fake_upstream.py and dashboard.py on spare ports, drives a short
conversation through the proxy, and checks the events and endpoints. The
browser checks (AC 2, 7, 10-12, 16) and the live-traffic ones (§9.1) are done
by hand.
"""
import http.client
import json
import os
import socket
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SECRET = "Bearer sk-ant-oat01-SECRETSECRETSECRETSECRET"
results = []


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def check(ok, what):
    results.append((bool(ok), what))
    print(("PASS " if ok else "FAIL ") + what)


def request(port, method, path, body=None, headers=None, raw=False):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    data = json.dumps(body).encode() if body is not None else None
    h = {"content-type": "application/json", "authorization": SECRET, "x-api-key": "short-key"} if body is not None else {}
    h.update(headers or {})
    c.request(method, path, body=data, headers=h)
    r = c.getresponse()
    out = r.read()
    return r.status, (out if raw else out.decode("utf-8", "replace"))


class Events:
    """Reads /events in a thread and keeps every (event, data) pair."""

    def __init__(self, port):
        self.items, self.lock = [], threading.Lock()
        self.conn = http.client.HTTPConnection("127.0.0.1", port)
        self.conn.request("GET", "/events")
        self.resp = self.conn.getresponse()
        threading.Thread(target=self.read, daemon=True).start()

    def read(self):
        name, data = None, []
        for line in self.resp:
            line = line.decode().rstrip("\n")
            if line.startswith("event:"):
                name = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
            elif not line and name:
                with self.lock:
                    self.items.append((name, json.loads("\n".join(data))))
                name, data = None, []

    def of(self, name):
        with self.lock:
            return [d for n, d in self.items if n == name]

    def wait(self, pred, timeout=10):
        end = time.time() + timeout
        while time.time() < end:
            if pred():
                return True
            time.sleep(0.05)
        return False


def body(messages, model="claude-opus-5-5"):
    return {"model": model, "max_tokens": 1000, "stream": True, "system": [{"type": "text", "text": "You are a test."}],
            "tools": [{"name": "Bash", "description": "Run a command.", "input_schema": {"type": "object"}}],
            "messages": messages}


def reply_content(sse_text):
    """Rebuild the reply from the stream the way a client would."""
    blocks = []
    for chunk in sse_text.split("\n\n"):
        for line in chunk.splitlines():
            if not line.startswith("data:"):
                continue
            d = json.loads(line[5:])
            if d["type"] == "content_block_start":
                blocks.append(dict(d["content_block"], _p=""))
            elif d["type"] == "content_block_delta" and d["delta"]["type"] == "text_delta":
                blocks[d["index"]]["text"] += d["delta"]["text"]
            elif d["type"] == "content_block_delta" and d["delta"]["type"] == "input_json_delta":
                blocks[d["index"]]["_p"] += d["delta"]["partial_json"]
    for b in blocks:
        p = b.pop("_p")
        if b["type"] == "tool_use":
            b["input"] = json.loads(p)
    return blocks


def main():
    up, port, ui, port2, ui2 = (free_port() for _ in range(5))
    log_path = os.path.join(HERE, ".dashboard-test.log")
    log = open(log_path, "w")
    procs = [subprocess.Popen([sys.executable, os.path.join(HERE, "fake_upstream.py"), str(up)]),
             subprocess.Popen([sys.executable, os.path.join(ROOT, "dashboard.py"), "--port", str(port), "--ui-port", str(ui),
                               "--upstream", f"http://127.0.0.1:{up}"], stdout=log, stderr=log),
             # a second instance whose upstream is down
             subprocess.Popen([sys.executable, os.path.join(ROOT, "dashboard.py"), "--port", str(port2), "--ui-port", str(ui2),
                               "--upstream", "http://127.0.0.1:9", "--quiet"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)]
    try:
        time.sleep(1.0)
        ev = Events(ui)
        ev.wait(lambda: ev.of("snapshot"))

        # AC1: forwarding is byte for byte.
        b_ns = body([{"role": "user", "content": "NONSTREAM hi"}])
        direct = request(up, "POST", "/v1/messages", b_ns, raw=True)
        proxied = request(port, "POST", "/v1/messages", b_ns, raw=True)
        b_st = body([{"role": "user", "content": "hello"}])
        direct_s = request(up, "POST", "/v1/messages", b_st, raw=True)
        proxied_s = request(port, "POST", "/v1/messages", b_st, raw=True)
        check(direct == proxied and direct_s == proxied_s, "AC1 replies pass through unchanged (plain and streamed)")

        # AC13: a HEAD check is stored as a check.
        request(port, "HEAD", "/api/hello")
        # AC8: a tool turn -> two pending calls.
        msgs = [{"role": "user", "content": "TOOL list files"}]
        _, sse1 = request(port, "POST", "/v1/messages?beta=true", body(msgs))
        calls = [b for b in reply_content(sse1) if b["type"] == "tool_use"]
        ev.wait(lambda: len(ev.of("tool_updated")) >= 2)
        check([e["entry"]["status"] for e in ev.of("tool_updated")][:2] == ["pending", "pending"], "AC8 tool calls start pending")
        # AC18: answer only Bash -> Bash paired, Read no result; then Read's result arrives -> paired.
        msgs += [{"role": "assistant", "content": reply_content(sse1)},
                 {"role": "user", "content": [{"type": "tool_result", "tool_use_id": calls[0]["id"], "content": "a.txt\nb.txt"}]}]
        _, sse2 = request(port, "POST", "/v1/messages?beta=true", body(msgs))
        ev.wait(lambda: ev.of("tool_paired"))
        statuses = lambda: {e["entry"]["name"]: e["entry"]["status"] for e in ev.of("tool_updated") + ev.of("tool_paired")}  # noqa: E731
        check(ev.wait(lambda: statuses() == {"Bash": "paired", "Read": "no_result"}), f"AC18 Bash paired, Read no result {statuses()}")
        msgs += [{"role": "assistant", "content": reply_content(sse2)},
                 {"role": "user", "content": [{"type": "tool_result", "tool_use_id": calls[1]["id"], "content": "late", "is_error": True}]}]
        request(port, "POST", "/v1/messages?beta=true", body(msgs))
        check(ev.wait(lambda: statuses().get("Read") == "paired"), "AC18 a late result still turns 'no result' into paired")

        # AC14: 429 with reason, retry-after, and a linked retry.
        b_rl = body([{"role": "user", "content": "RATELIMIT please"}])
        s1, _ = request(port, "POST", "/v1/messages", b_rl)
        s2, _ = request(port, "POST", "/v1/messages", b_rl)
        errs = ev.of("exchange_error")
        rl = next((e for e in errs if e.get("status") == 429), None)
        check(s1 == 429 and s2 == 200 and rl and rl["error_info"]["type"] == "rate_limit_error"
              and rl["error_info"]["retry_after"] == "6" and rl["error_info"]["ratelimit"], "AC14 429 recorded with type, message, retry-after and limits")
        check(ev.wait(lambda: any(m.get("retried_by") for m in ev.of("exchange_meta"))), "AC14 the retry is linked (retry of / retried by)")

        # AC17: a stream that stops early fails; so does an unreachable upstream.
        request(port, "POST", "/v1/messages", body([{"role": "user", "content": "NOFINAL"}]))
        check(ev.wait(lambda: any("final event" in e["message"] for e in ev.of("exchange_error"))), "AC17 a stream without its final event ends as an error")
        st_down, _ = request(port2, "POST", "/v1/messages", b_st)
        _, snap2 = request(ui2, "GET", "/api/export")
        down = json.loads(snap2)["exchanges"][0]
        check(st_down == 502 and down["error"] and down["ended_at"] and down["status"] is None, "AC17 an unreachable upstream: 502, error recorded, status null")

        # §6.5 cost: 5m/1h cache-write split; unknown model shows no price.
        request(port, "POST", "/v1/messages", body([{"role": "user", "content": "CACHE1H"}]))
        request(port, "POST", "/v1/messages", body([{"role": "user", "content": "NONSTREAM x"}], model="mystery-model-1"))
        time.sleep(0.5)
        _, exp = request(ui, "GET", "/api/export")
        state = json.loads(exp)
        exs = state["exchanges"]
        c1h = next(x for x in exs if "CACHE1H" in json.dumps(x["request_body"]))
        want = (12 * 4 + 42 * 20 + 0 * 0.2 + 1000 * 5 + 2000 * 8) / 1e6
        check(abs(c1h["cost_usd"] - want) < 1e-9, f"§6.5 cost uses the 5m/1h split ({c1h['cost_usd']} vs {want})")
        mystery = next(x for x in exs if (x["request_body"] or {}).get("model") == "mystery-model-1")
        check(mystery["cost_usd"] is None and mystery["timing"]["ttft_ms"] is None, "AC9 unpriced model has no cost; non-streamed reply has no ttft")

        # AC17: exactly one ending per exchange.
        ends = {}
        for name in ("exchange_done", "exchange_error"):
            for d in ev.of(name):
                ends[d["id"]] = ends.get(d["id"], 0) + 1
        started = [d["id"] for d in ev.of("exchange_started")]
        check(started and all(ends.get(i) == 1 for i in started), "AC17 every exchange ends exactly once")

        # AC4: usage equals message_start + message_delta.
        ok = True
        for x in exs:
            evs = x["response_events"]
            if not evs:
                continue
            u = dict(next(e["data"]["message"]["usage"] for e in evs if e["event"] == "message_start"))
            for e in evs:
                if e["event"] == "message_delta":
                    u.update(e["data"]["usage"])
            ok &= u == x["assembled"]["usage"]
        check(ok, "AC4 usage matches the stream's message_start and message_delta")
        check(sum(x["kind"] == "check" for x in exs) == 1 and not next(x for x in exs if x["kind"] == "check")["model_call"],
              "AC13 the HEAD check is stored as a check, not a model call")

        # AC5: a new subscriber gets everything, pairs included.
        ev2 = Events(ui)
        ev2.wait(lambda: ev2.of("snapshot"))
        snap = ev2.of("snapshot")[0]
        check(len(snap["exchanges"]) == len(exs) and sum(t["status"] == "paired" for t in snap["tools"].values()) == 2,
              "AC5 a reconnect's snapshot restores exchanges and tool pairs")

        # AC6 / §7: no full secret anywhere; a short secret shows only "...".
        log.flush()
        logged = open(log_path).read()
        check(SECRET not in exp and SECRET not in logged and "SECRETSECRET" not in logged, "AC6 no full token in export or terminal output")
        check(any(x["request_headers"].get("x-api-key") == "..." for x in exs), "§7 a secret of 16 chars or fewer shows only '...'")

        # §7: other sites are rejected.
        st_host, _ = request(ui, "GET", "/api/export", headers={"Host": "evil.example"})
        st_origin, _ = request(ui, "POST", "/api/clear", headers={"Origin": "http://evil.example"})
        check(st_host == 403 and st_origin == 403, "§7 non-localhost Host and POST Origin are rejected")

        # §6.8: the original request text is kept.
        first_post = next(x for x in exs if x["method"] == "POST")
        _, original = request(ui, "GET", f"/api/exchanges/{first_post['id']}/request")
        check(original == json.dumps(b_ns), "§6.8 the original request text is served unchanged")

        # §4/§5: clear keeps ids rising.
        last_id = max(started)
        request(ui, "POST", "/api/clear")
        request(port, "HEAD", "/api/hello")
        check(ev.wait(lambda: ev.of("cleared") and ev.of("exchange_started")[-1]["id"] == last_id + 1), "§4 ids keep rising after clear")
    finally:
        for p in procs:
            p.terminate()
        log.close()
        os.remove(log_path)
    failed = [w for ok, w in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
