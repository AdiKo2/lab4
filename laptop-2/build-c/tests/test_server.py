#!/usr/bin/env python3
"""Server-side tests. Starts a fake upstream and dashboard.py on ports 8071 and 8072.

    python3 tests/test_server.py
"""

import http.client
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PROXY = 8071
UI = 8072
SECRET = "sk-ant-api03-THISISAFULLSECRETTOKENVALUE0123456789"
COOKIE = "session=THISISAFULLCOOKIEVALUE0123456789"
BEARER = "Bearer THISISAFULLBEARERVALUE0123456789"


def wait_port(port):
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} never opened")


def post(prompt, messages=None, system="SYS", extra=None, headers=None, path="/v1/messages?beta=true"):
    body = {"model": "claude-opus-5-5", "max_tokens": 10, "stream": True, "system": system,
            "tools": [{"name": "Bash", "description": "Run", "input_schema": {"type": "object"}}],
            "messages": messages if messages is not None else [{"role": "user", "content": prompt}]}
    body.update(extra or {})
    raw = json.dumps(body).encode()
    conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=20)
    conn.request("POST", path, body=raw, headers={"Content-Type": "application/json", **(headers or {})})
    reply = conn.getresponse()
    data = reply.read()
    return reply, data


def api(path, method="GET"):
    req = urllib.request.Request(f"http://127.0.0.1:{UI}{path}", method=method)
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read(), r.headers


def exchanges():
    return json.loads(api("/api/export")[0])


def settle(n=None):
    """Waits until every exchange has ended, since recording is asynchronous."""
    for _ in range(200):
        items = exchanges()
        if items and all(e["ended_at"] for e in items) and (n is None or len(items) >= n):
            return items
        time.sleep(0.02)
    raise AssertionError("exchanges never ended")


class SSEClient(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.events = []
        self.conn = http.client.HTTPConnection("127.0.0.1", UI, timeout=30)
        self.conn.request("GET", "/events")
        self.resp = self.conn.getresponse()
        self.start()

    def run(self):
        name = None
        try:
            while True:
                line = self.resp.fp.readline()
                if not line:
                    return
                line = line.decode().rstrip("\n")
                if line.startswith("event: "):
                    name = line[7:]
                elif line.startswith("data: "):
                    self.events.append((name, json.loads(line[6:]), time.monotonic()))
        except (OSError, ValueError, AttributeError):
            pass

    def names(self):
        return [e[0] for e in self.events]

    def of(self, name):
        return [e[1] for e in self.events if e[0] == name]

    def wait_for(self, name, count=1):
        for _ in range(300):
            if len(self.of(name)) >= count:
                return self.of(name)
            time.sleep(0.01)
        raise AssertionError(f"never saw {count} {name}; saw {self.names()}")

    def close(self):
        try:
            self.conn.sock.close()
        except Exception:
            pass


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fake = subprocess.Popen([sys.executable, os.path.join(HERE, "fake_upstream.py")], stdout=subprocess.PIPE, text=True)
        cls.fake_port = int(cls.fake.stdout.readline().split()[1])
        cls.log_path = os.environ.get("TEST_LOG", os.path.join(os.environ.get("TMPDIR", "/tmp"), "dash_test.log"))
        cls.log = open(cls.log_path, "w")
        cls.proc = subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "dashboard.py"), "--port", str(PROXY), "--ui-port", str(UI),
             "--upstream", f"http://127.0.0.1:{cls.fake_port}"] + cls.flags(),
            stdout=cls.log, stderr=subprocess.STDOUT)
        wait_port(PROXY)
        wait_port(UI)

    @classmethod
    def flags(cls):
        return []

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait(5)
        cls.fake.terminate()
        cls.fake.wait(5)
        cls.fake.stdout.close()
        cls.log.close()

    def setUp(self):
        api("/api/clear", "POST")

    def seen(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.fake_port}/__seen") as r:
            return json.loads(r.read())


class Forwarding(Base):
    def test_request_reaches_upstream_byte_for_byte(self):
        before = len(self.seen())
        raw = b'{"model":"m",  "a": 1.0,\n "messages":[{"role":"user","content":"hi"}]}'
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("POST", "/v1/messages?beta=true", body=raw, headers={
            "x-api-key": SECRET, "Authorization": BEARER, "Cookie": COOKIE, "anthropic-beta": "oauth-2025",
            "Accept-Encoding": "gzip"})
        conn.getresponse().read()
        got = self.seen()[before]
        self.assertEqual(got["body"].encode(), raw)
        self.assertEqual(got["path"], "/v1/messages?beta=true")
        headers = {k.lower(): v for k, v in got["headers"]}
        self.assertEqual(headers["x-api-key"], SECRET)
        self.assertEqual(headers["authorization"], BEARER)
        self.assertEqual(headers["cookie"], COOKIE)
        self.assertEqual(headers["anthropic-beta"], "oauth-2025")
        self.assertEqual(headers["accept-encoding"], "identity")  # dropped; http.client adds its default, as with proxy.py

    def test_reply_bytes_and_headers_pass_through(self):
        reply, data = post("hello")
        self.assertEqual(reply.status, 200)
        self.assertIn(b"event: message_stop", data)
        self.assertEqual(reply.getheader("X-Custom-Reply"), "kept")
        self.assertEqual(reply.getheader("Connection"), "close")
        cookies = [v for k, v in reply.getheaders() if k.lower() == "set-cookie"]
        self.assertEqual(cookies, ["a=1", "b=2"])

    def test_binary_reply_is_unchanged(self):
        _, data = post("binary")
        self.assertEqual(data, bytes(range(256)))
        self.assertIsNone(settle()[0]["error"])

    def test_streaming_is_not_buffered(self):
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        body = json.dumps({"model": "m", "messages": [{"role": "user", "content": "slow"}]})
        start = time.monotonic()
        conn.request("POST", "/v1/messages", body=body)
        reply = conn.getresponse()
        first = reply.read1(100)
        first_at = time.monotonic() - start
        reply.read()
        total = time.monotonic() - start
        self.assertTrue(first)
        self.assertLess(first_at, 0.5)
        self.assertGreater(total, 1.5)

    def test_get_and_head_and_other_methods(self):
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("GET", "/v1/models")
        self.assertIn(b"claude-opus", conn.getresponse().read())
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("HEAD", "/v1/models")
        r = conn.getresponse()
        self.assertEqual(r.status, 200)
        r.read()
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("PUT", "/x", body=b"")
        self.assertEqual(conn.getresponse().status, 501)  # same as proxy.py

    def test_parallel_requests_stay_separate(self):
        results = {}

        def run(i):
            _, results[i] = post(f"hello {i}")

        threads = [threading.Thread(target=run, args=(i,)) for i in range(20)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        items = settle(20)
        self.assertEqual(len(items), 20)
        self.assertEqual(len({e["id"] for e in items}), 20)
        for e in items:
            self.assertEqual(e["assembled"]["content"][0]["text"].strip(), "Hello from the fake upstream, streaming a reply.")
        self.assertEqual({e["request_body"]["messages"][0]["content"] for e in items}, {f"hello {i}" for i in range(20)})


class Recording(Base):
    def test_exchange_shape_and_usage(self):
        post("hello")
        e = settle(1)[0]
        for key in ("id", "started_at", "ended_at", "method", "path", "status", "request_headers", "request_body",
                    "response_headers", "response_events", "assembled", "timing", "cost_usd", "error"):
            self.assertIn(key, e)
        self.assertRegex(e["started_at"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")
        self.assertEqual(e["path"], "/v1/messages?beta=true")
        self.assertEqual(e["status"], 200)
        self.assertEqual(e["assembled"]["model"], "claude-opus-5-5")
        self.assertEqual(e["assembled"]["stop_reason"], "end_turn")
        # message_start gave input and cache numbers; message_delta gave cumulative output.
        self.assertEqual(e["assembled"]["usage"], {"input_tokens": 12, "output_tokens": 340,
                                                    "cache_read_input_tokens": 18000, "cache_creation_input_tokens": 500})
        self.assertIsNotNone(e["timing"]["ttfb_ms"])
        self.assertIsNotNone(e["timing"]["ttft_ms"])
        self.assertIsNotNone(e["timing"]["total_ms"])
        self.assertIsNotNone(e["timing"]["total_ms"])
        self.assertTrue(e["timing"]["tokens_per_s"] is None or e["timing"]["tokens_per_s"] > 0)
        self.assertLessEqual(e["timing"]["ttfb_ms"], e["timing"]["ttft_ms"])
        # opus 5.5 at 4/20/0.2/5 per million.
        expected = (12 * 4 + 340 * 20 + 18000 * 0.2 + 500 * 5) / 1e6
        self.assertAlmostEqual(e["cost_usd"], expected, places=7)

    def test_unknown_model_has_null_cost_and_dated_model_matches(self):
        post("unpriced")
        post("dated")
        items = settle(2)
        by = {e["assembled"]["model"]: e for e in items}
        self.assertIsNone(by["mystery-model-9"]["cost_usd"])
        self.assertIsNotNone(by["claude-opus-5-5-20260401"]["cost_usd"])

    def test_split_chunks_crlf_multibyte_and_broken_events(self):
        _, data = post("split")
        self.assertIn("café 🌍".encode(), data)
        e = settle(1)[0]
        self.assertEqual(e["assembled"]["content"][0]["text"], "café 🌍")
        broken = [ev for ev in e["response_events"] if ev.get("parse_error")]
        self.assertEqual(len(broken), 1)
        self.assertEqual(broken[0]["data"], "{not json")
        self.assertIsNone(e["error"])
        # An input_tokens of 0 in message_delta does not wipe the real value.
        self.assertEqual(e["assembled"]["usage"]["input_tokens"], 12)
        self.assertEqual(e["assembled"]["usage"]["output_tokens"], 5)

    def test_count_tokens_is_not_treated_as_usage(self):
        post("x", path="/v1/messages/count_tokens")
        e = settle(1)[0]
        self.assertEqual(e["assembled"]["usage"], {})
        self.assertIsNone(e["cost_usd"])
        self.assertEqual(e["response_body"], '{"input_tokens":1234}')

    def test_non_streaming_reply_assembles(self):
        post("nostream")
        e = settle(1)[0]
        self.assertEqual(e["assembled"]["content"][0]["text"], "plain reply")
        self.assertEqual(e["assembled"]["usage"]["output_tokens"], 6)
        self.assertIsNone(e["timing"]["ttft_ms"])

    def test_non_json_body_is_a_string(self):
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("POST", "/v1/other", body=b"not json at all")
        conn.getresponse().read()
        self.assertEqual(settle(1)[0]["request_body"], "not json at all")

    def test_nan_body_stays_valid_json_for_the_browser(self):
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("POST", "/v1/other", body=b'{"a": NaN}')
        conn.getresponse().read()
        self.assertEqual(settle(1)[0]["request_body"], '{"a": NaN}')

    def test_lone_surrogate_does_not_break_anything(self):
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("POST", "/v1/messages", body=b'{"messages":[{"role":"user","content":"x \\ud83d y"}]}')
        conn.getresponse().read()
        self.assertEqual(len(settle(1)), 1)

    def test_ring_buffer_ids_and_get(self):
        first_id = None
        post("hello")
        first_id = settle(1)[0]["id"]
        one = json.loads(api(f"/api/exchanges/{first_id}")[0])
        self.assertEqual(one["id"], first_id)
        self.assertIn("request_raw", one)
        with self.assertRaises(urllib.error.HTTPError) as caught:
            api("/api/exchanges/99999")
        self.assertEqual(caught.exception.code, 404)
        api("/api/clear", "POST")
        self.assertEqual(exchanges(), [])
        post("hello")
        self.assertGreater(settle(1)[0]["id"], first_id)

    def test_export_headers(self):
        post("hello")
        settle(1)
        _, headers = api("/api/export")
        self.assertRegex(headers["Content-Disposition"], r'attachment; filename="traffic-\d{8}-\d{6}\.json"')

    def test_page_and_security_headers(self):
        page, headers = api("/")
        self.assertIn(b"<title>", page)
        self.assertNotIn(b"http://", page)  # no CDN
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        self.assertNotIn(b"innerHTML =", page)


class Failures(Base):
    def test_cutoff_stream(self):
        post_err = None
        try:
            post("cutoff")
        except (http.client.HTTPException, OSError) as e:
            post_err = e
        e = settle(1)[0]
        self.assertIsNotNone(e["error"])
        self.assertEqual(e["assembled"]["content"][0]["text"], "partial")
        self.assertIsNotNone(e["ended_at"])
        del post_err

    def test_sse_error_event(self):
        post("sse-error")
        self.assertEqual(settle(1)[0]["error"], "Overloaded")

    def test_http_error_is_passed_on_and_flagged(self):
        reply, data = post("http429")
        self.assertEqual(reply.status, 429)
        self.assertIn(b"slow down", data)
        e = settle(1)[0]
        self.assertEqual(e["status"], 429)
        self.assertIn("slow down", e["error"])

    def test_client_drop_mid_stream(self):
        sub = SSEClient()
        try:
            body = json.dumps({"model": "m", "messages": [{"role": "user", "content": "slow"}]}).encode()
            raw = socket.create_connection(("127.0.0.1", PROXY))
            raw.sendall(b"POST /v1/messages HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body))
            raw.recv(200)
            raw.close()
            sub.wait_for("exchange_error")
            self.assertIn("client", sub.of("exchange_error")[0]["message"])
        finally:
            sub.close()
        # The proxy keeps working after a drop.
        reply, _ = post("hello")
        self.assertEqual(reply.status, 200)


class Unreachable(unittest.TestCase):
    def test_unreachable_upstream_gives_502_and_error(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            dead = s.getsockname()[1]
        proc = subprocess.Popen([sys.executable, os.path.join(ROOT, "dashboard.py"), "--port", str(PROXY), "--ui-port", str(UI),
                                 "--upstream", f"http://127.0.0.1:{dead}", "--quiet"],
                                stdout=subprocess.PIPE, text=True)
        try:
            wait_port(PROXY)
            wait_port(UI)
            reply, _ = post("hello")
            self.assertEqual(reply.status, 502)
            e = settle(1)[0]
            self.assertIn("upstream unreachable", e["error"])
            self.assertIsNone(e["status"])
        finally:
            proc.terminate()
            proc.wait(5)
            proc.stdout.close()


class Events(Base):
    def test_snapshot_then_lifecycle_order(self):
        post("hello")
        settle(1)
        sub = SSEClient()
        try:
            snap = sub.wait_for("snapshot")[0]
            self.assertEqual(len(snap), 1)
            self.assertEqual(sub.names()[:2], ["snapshot", "tools_snapshot"])
            post("hello again")
            sub.wait_for("exchange_done")
            names = sub.names()
            self.assertLess(names.index("exchange_started"), names.index("exchange_response"))
            self.assertLess(names.index("exchange_response"), names.index("exchange_delta"))
            self.assertLess(names.index("exchange_delta"), names.index("exchange_done"))
            started = sub.of("exchange_started")[0]
            for key in ("id", "time", "method", "path", "headers", "body"):
                self.assertIn(key, started)
            resp = sub.of("exchange_response")[0]
            for key in ("id", "status", "headers"):
                self.assertIn(key, resp)
            delta = sub.of("exchange_delta")[0]
            self.assertIn("id", delta)
            self.assertIn("event", delta)
            done = sub.of("exchange_done")[0]
            for key in ("id", "ended_at", "assembled"):
                self.assertIn(key, done)
        finally:
            sub.close()

    def test_started_event_arrives_within_200ms(self):
        sub = SSEClient()
        try:
            sub.wait_for("snapshot")
            t0 = time.monotonic()
            threading.Thread(target=post, args=("slow",), daemon=True).start()
            sub.wait_for("exchange_started")
            at = [e[2] for e in sub.events if e[0] == "exchange_started"][0]
            self.assertLess(at - t0, 0.2)
            first_delta = sub.wait_for("exchange_delta")
            self.assertTrue(first_delta)
            sub.wait_for("exchange_done")
        finally:
            sub.close()

    def test_two_subscribers_and_a_stalled_one_dont_slow_the_proxy(self):
        # This client connects and never reads.
        stalled = socket.create_connection(("127.0.0.1", UI))
        stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        stalled.sendall(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        sub = SSEClient()
        try:
            sub.wait_for("snapshot")
            t0 = time.monotonic()
            for _ in range(30):
                post("hello " + "x" * 200000)
            elapsed = time.monotonic() - t0
            self.assertLess(elapsed, 6)
            sub.wait_for("exchange_done", 30)
        finally:
            sub.close()
            stalled.close()

    def test_clear_event(self):
        sub = SSEClient()
        try:
            sub.wait_for("snapshot")
            api("/api/clear", "POST")
            sub.wait_for("cleared")
        finally:
            sub.close()

    def test_reconnect_mid_stream_gets_partial_snapshot_then_rest(self):
        t = threading.Thread(target=post, args=("slow",), daemon=True)
        t.start()
        time.sleep(0.5)
        sub = SSEClient()
        try:
            snap = sub.wait_for("snapshot")[0]
            self.assertEqual(len(snap), 1)
            self.assertIsNone(snap[0]["ended_at"])
            partial = snap[0]["assembled"]["content"][0]["text"]
            self.assertTrue(partial)
            done = sub.wait_for("exchange_done")[0]
            total = partial
            for d in sub.of("exchange_delta"):
                for p in d["patches"]:
                    if p["op"] == "append":
                        total += p["text"]
            self.assertEqual(total, done["assembled"]["content"][0]["text"])
        finally:
            sub.close()
        t.join()


class ToolPairing(Base):
    HISTORY = [{"role": "user", "content": "tool please"}]

    def call_message(self, tool_id="toolu_01abc", name="Bash", args=None):
        return {"role": "assistant", "content": [{"type": "tool_use", "id": tool_id, "name": name, "input": args or {"command": "ls -la"}}]}

    def tools_from_snapshot(self):
        sub = SSEClient()
        try:
            return sub.wait_for("tools_snapshot")[0]
        finally:
            sub.close()

    def test_pending_then_paired_across_exchanges(self):
        sub = SSEClient()
        try:
            sub.wait_for("snapshot")
            post("tool please", messages=self.HISTORY)
            pending = sub.wait_for("tool_updated")[0]
            self.assertEqual(pending["status"], "pending")
            self.assertEqual(pending["input"], {"command": "ls -la"})
            self.assertEqual(pending["name"], "Bash")
            first = settle(1)[0]
            self.assertEqual(first["assembled"]["content"][2]["input"], {"command": "ls -la"})
            self.assertEqual([b["type"] for b in first["assembled"]["content"]], ["thinking", "text", "tool_use"])
            time.sleep(0.05)
            messages = self.HISTORY + [self.call_message(), {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_01abc", "content": "file1\nfile2"}]}]
            post("x", messages=messages)
            paired = sub.wait_for("tool_paired")[0]
            for key in ("tool_use_id", "call_exchange", "result_exchange", "result"):
                self.assertIn(key, paired)
            self.assertEqual(paired["tool_use_id"], "toolu_01abc")
            self.assertEqual(paired["call_exchange"], first["id"])
            self.assertEqual(paired["result_exchange"], first["id"] + 1)
            self.assertEqual(paired["result"], "file1\nfile2")
            self.assertEqual(paired["status"], "paired")
            self.assertEqual(paired["result_size"], 11)
            self.assertGreaterEqual(paired["wait_ms"], 40)
            # Later requests resend the same result. It must not pair again.
            post("y", messages=messages + [{"role": "assistant", "content": "ok"}, {"role": "user", "content": "more"}])
            settle(3)
            time.sleep(0.1)
            self.assertEqual(len(sub.of("tool_paired")), 1)
            self.assertEqual(self.tools_from_snapshot()["toolu_01abc"]["result_exchange"], first["id"] + 1)
        finally:
            sub.close()

    def test_is_error_flag(self):
        post("tool-error", messages=[{"role": "user", "content": "tool-error"}])
        settle(1)
        post("x", messages=[{"role": "user", "content": "tool-error"}, self.call_message("toolu_err", "Read"), {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_err", "is_error": True, "content": [{"type": "text", "text": "no such file"}]}]}])
        settle(2)
        entry = self.tools_from_snapshot()["toolu_err"]
        self.assertTrue(entry["is_error"])
        self.assertEqual(entry["result_size"], 12)

    def test_no_result_when_conversation_moves_on(self):
        sub = SSEClient()
        try:
            sub.wait_for("snapshot")
            post("tool please", messages=self.HISTORY)
            settle(1)
            post("x", messages=self.HISTORY + [self.call_message(), {"role": "user", "content": "never mind"}])
            settle(2)
            updates = sub.wait_for("tool_updated", 2)
            self.assertEqual(updates[-1]["status"], "no_result")
        finally:
            sub.close()

    def test_rewound_history_counts_as_moved_on(self):
        post("tool please", messages=self.HISTORY)
        settle(1)
        post("x", messages=self.HISTORY + [{"role": "assistant", "content": "other"}, {"role": "user", "content": "go"}])
        settle(2)
        self.assertEqual(self.tools_from_snapshot()["toolu_01abc"]["status"], "no_result")

    def test_other_conversation_does_not_disturb_a_pending_call(self):
        post("tool please", messages=self.HISTORY)
        settle(1)
        post("hello", messages=[{"role": "user", "content": "unrelated"}, {"role": "assistant", "content": "a"}, {"role": "user", "content": "b"}])
        post("hello", system="A DIFFERENT PROMPT")
        settle(3)
        self.assertEqual(self.tools_from_snapshot()["toolu_01abc"]["status"], "pending")

    def test_tool_result_without_a_captured_call_is_ignored_by_the_index(self):
        post("x", messages=[{"role": "user", "content": "q"}, self.call_message("toolu_old"), {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_old", "content": "r"}]}])
        settle(1)
        self.assertEqual(self.tools_from_snapshot(), {})

    def test_clear_empties_tool_index(self):
        post("tool please", messages=self.HISTORY)
        settle(1)
        api("/api/clear", "POST")
        self.assertEqual(self.tools_from_snapshot(), {})


class Codex(Base):
    def test_codex_stream_and_pairing(self):
        body = {"model": "gpt-5-codex", "instructions": "CODEX SYSTEM", "stream": True,
                "tools": [{"type": "function", "name": "shell", "description": "Run", "parameters": {"type": "object"}}],
                "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "go"}]}]}
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("POST", "/responses", body=json.dumps(body))
        conn.getresponse().read()
        e = settle(1)[0]
        self.assertEqual(e["request_view"]["system"][0]["text"], "CODEX SYSTEM")
        self.assertEqual(e["request_view"]["tools"][0]["name"], "shell")
        content = e["assembled"]["content"]
        self.assertEqual(content[0]["text"], "Hello from codex")
        self.assertEqual(content[1]["input"], {"cmd": "ls"})
        self.assertEqual(e["assembled"]["stop_reason"], "tool_use")
        # 900 of the 1000 input tokens were cached.
        self.assertEqual(e["assembled"]["usage"]["input_tokens"], 100)
        self.assertEqual(e["assembled"]["usage"]["cache_read_input_tokens"], 900)
        self.assertIsNone(e["cost_usd"])

        body["input"] += [{"type": "function_call", "call_id": "call_9", "name": "shell", "arguments": '{"cmd":"ls"}'},
                          {"type": "function_call_output", "call_id": "call_9", "output": "a.txt"}]
        conn = http.client.HTTPConnection("127.0.0.1", PROXY, timeout=10)
        conn.request("POST", "/responses", body=json.dumps(body))
        conn.getresponse().read()
        items = settle(2)
        self.assertEqual(len(items[1]["request_view"]["messages"]), 3)
        sub = SSEClient()
        try:
            entry = sub.wait_for("tools_snapshot")[0]["call_9"]
        finally:
            sub.close()
        self.assertEqual(entry["status"], "paired")
        self.assertEqual(entry["result"], "a.txt")


class Secrets(Base):
    def test_no_full_token_in_store_export_events_or_logs(self):
        sub = SSEClient()
        try:
            sub.wait_for("snapshot")
            post("http429", headers={"x-api-key": SECRET, "Authorization": BEARER, "Cookie": COOKIE, "Proxy-Authorization": "Basic THISISAFULLPROXYSECRET0123456789"})
            post("hello", headers={"x-api-key": "shortkey"})
            items = settle(2)
            sub.wait_for("exchange_done")
            export = api("/api/export")[0].decode()
            one = api(f"/api/exchanges/{items[0]['id']}")[0].decode()
            events = json.dumps([e[1] for e in sub.events])
            time.sleep(0.2)
            self.log.flush()
            with open(self.log_path, encoding="utf-8") as f:
                log = f.read()
            for label, text in (("export", export), ("exchange", one), ("events", events), ("log", log)):
                for secret in (SECRET, COOKIE, BEARER, "THISISAFULLPROXYSECRET", "topsecretcookievalue1234567890", "shortkey"):
                    self.assertNotIn(secret, text, f"{secret} leaked in {label}")
            first = items[0]["request_headers"]
            self.assertEqual(first["x-api-key"], SECRET[:16] + "...")
            self.assertEqual(first["Authorization"], BEARER[:16] + "...")
            self.assertEqual(first["Cookie"], COOKIE[:16] + "...")
            self.assertIn("...", items[0]["response_headers"]["Set-Cookie"])
        finally:
            sub.close()

    def test_binds_loopback_only_and_rejects_foreign_hosts(self):
        out = subprocess.run(["lsof", "-nP", "-a", "-p", str(self.proc.pid), "-iTCP", "-sTCP:LISTEN"], capture_output=True, text=True).stdout
        self.assertIn(f"127.0.0.1:{PROXY}", out)
        self.assertIn(f"127.0.0.1:{UI}", out)
        self.assertNotIn("*:", out)
        conn = http.client.HTTPConnection("127.0.0.1", UI, timeout=5)
        conn.request("GET", "/api/export", headers={"Host": "evil.example"})
        self.assertEqual(conn.getresponse().status, 403)
        conn = http.client.HTTPConnection("127.0.0.1", UI, timeout=5)
        conn.request("POST", "/api/clear", headers={"Origin": "https://evil.example"})
        self.assertEqual(conn.getresponse().status, 403)


class TerminalOutput(Base):
    def test_terminal_prints_like_proxy_py(self):
        post("hello")
        settle(1)
        time.sleep(0.2)
        self.log.flush()
        with open(self.log_path, encoding="utf-8") as f:
            log = f.read()
        self.assertIn(f"proxy listening on http://localhost:{PROXY}", log)
        self.assertIn(f"dashboard on http://localhost:{UI}", log)
        self.assertRegex(log, rf"(?m)^ANTHROPIC_BASE_URL=http://localhost:{PROXY} claude$")
        self.assertRegex(log, r">>> POST /v1/messages\?beta=true")
        self.assertRegex(log, r"<<< 200 OK")
        self.assertIn("event: message_stop", log)
        self.assertRegex(log, r"--- done")
        self.assertIn('"max_tokens": 10', log)


class Quiet(Base):
    @classmethod
    def flags(cls):
        return ["--quiet", "--buffer", "3"]

    def test_quiet_and_ring_buffer(self):
        for i in range(5):
            post(f"hello {i}")
        items = settle(3)
        self.assertEqual(len(items), 3)
        self.assertEqual([e["request_body"]["messages"][0]["content"] for e in items], ["hello 2", "hello 3", "hello 4"])
        time.sleep(0.1)
        self.log.flush()
        with open(self.log_path, encoding="utf-8") as f:
            log = f.read()
        self.assertNotIn(">>>", log)
        self.assertRegex(log, rf"(?m)^ANTHROPIC_BASE_URL=http://localhost:{PROXY} claude$")

    def test_eviction_event_drops_tool_entries(self):
        post("tool please", messages=[{"role": "user", "content": "tool please"}])
        for i in range(3):
            post(f"hello {i}")
        settle(3)
        sub = SSEClient()
        try:
            self.assertEqual(sub.wait_for("tools_snapshot")[0], {})
        finally:
            sub.close()


if __name__ == "__main__":
    unittest.main(verbosity=1)
