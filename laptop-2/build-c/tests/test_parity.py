#!/usr/bin/env python3
"""AC 1 stand-in: replays one request set through proxy.py and dashboard.py and compares.

    python3 tests/test_parity.py

Compares what the client receives, what the upstream receives, and the terminal
text. Only the cookie line may differ in the terminal (see the note in dashboard.py).
"""

import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PORT, UI = 8071, 8072
KEY = "sk-ant-api03-LONGSECRETLONGSECRETLONGSECRET"
AUTH = {"x-api-key": KEY, "Authorization": "Bearer oauthtokenoauthtokenoauthtoken", "anthropic-beta": "oauth-2025-04-20",
        "Cookie": "session=longcookievaluelongcookievalue", "Accept-Encoding": "gzip, br", "User-Agent": "parity"}


def body(prompt):
    return json.dumps({"model": "claude-opus-5-5", "stream": True, "messages": [{"role": "user", "content": prompt}]}, ensure_ascii=False).encode()


CASES = [
    ("POST", "/v1/messages?beta=true", body("hello")),
    ("POST", "/v1/messages", body("tool please")),
    ("POST", "/v1/messages", body("split")),
    ("POST", "/v1/messages", body("nostream")),
    ("POST", "/v1/messages", body("http429")),
    ("POST", "/v1/messages", body("binary")),
    ("POST", "/v1/messages/count_tokens", body("x")),
    ("POST", "/v1/other", b"not json"),
    ("GET", "/v1/models", b""),
    ("HEAD", "/v1/models", b""),
    ("PUT", "/v1/models", b""),
]


def raw_request(method, path, data):
    lines = [f"{method} {path} HTTP/1.1", "Host: localhost"] + [f"{k}: {v}" for k, v in AUTH.items()]
    if data:
        lines.append(f"Content-Length: {len(data)}")
    with socket.create_connection(("127.0.0.1", PORT), timeout=10) as s:
        s.sendall(("\r\n".join(lines) + "\r\n\r\n").encode() + data)
        out = b""
        while chunk := s.recv(65536):
            out += chunk
    return out


def normalise_reply(data):
    head, _, rest = data.partition(b"\r\n\r\n")
    head = re.sub(rb"(?i)date: [^\r]+", b"Date: X", head)
    return head, rest


def run(cmd):
    fake = subprocess.Popen([sys.executable, os.path.join(HERE, "fake_upstream.py")], stdout=subprocess.PIPE, text=True)
    fake_port = int(fake.stdout.readline().split()[1])
    log_path = os.path.join(os.environ.get("TMPDIR", "/tmp"), "parity.log")
    log = open(log_path, "w")
    proc = subprocess.Popen(cmd + ["--upstream", f"http://127.0.0.1:{fake_port}"], stdout=log, stderr=subprocess.STDOUT,
                            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"})
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", PORT), 0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        replies = [normalise_reply(raw_request(*case)) for case in CASES]
        time.sleep(0.3)
        with urllib.request.urlopen(f"http://127.0.0.1:{fake_port}/__seen") as r:
            seen = json.loads(r.read())
        for item in seen:  # the fake upstream's port differs between runs
            item["headers"] = [h for h in item["headers"] if h[0].lower() != "host"]
    finally:
        proc.terminate()
        proc.wait(5)
        fake.terminate()
        fake.wait(5)
        fake.stdout.close()
        log.close()
    with open(log_path, encoding="utf-8") as f:
        terminal = f.read()
    os.remove(log_path)
    return replies, seen, terminal


def normalise_terminal(text):
    text = text[text.index("=" * 78):]  # startup lines differ on purpose
    text = re.sub(r"\[\d\d:\d\d:\d\d\]", "[T]", text)
    text = re.sub(r"(?m)^    Cookie: .*$", "    Cookie: X", text)
    return text


def main():
    a_replies, a_seen, a_term = run([sys.executable, os.path.join(ROOT, "proxy.py"), "--port", str(PORT)])
    b_replies, b_seen, b_term = run([sys.executable, os.path.join(ROOT, "dashboard.py"), "--port", str(PORT), "--ui-port", str(UI)])
    failed = 0
    for case, x, y in zip(CASES, a_replies, b_replies):
        same = x == y
        print(("same " if same else "DIFF ") + f"reply  {case[0]} {case[1]} {case[2][:40]!r}")
        if not same:
            failed += 1
            print("  proxy.py    ", x[0][:300])
            print("  dashboard.py", y[0][:300])
    same = a_seen == b_seen
    print(("same " if same else "DIFF ") + f"upstream saw {len(a_seen)} requests")
    failed += not same
    x, y = normalise_terminal(a_term), normalise_terminal(b_term)
    same = x == y
    print(("same " if same else "DIFF ") + f"terminal text ({len(x)} chars, cookie line normalised)")
    if not same:
        failed += 1
        for i, (m, n) in enumerate(zip(x.splitlines(), y.splitlines())):
            if m != n:
                print(f"  line {i}\n   proxy.py     {m[:200]}\n   dashboard.py {n[:200]}")
                break
    print("cookie hidden in dashboard terminal:", "longcookievaluelongcookievalue" not in b_term)
    print("cookie shown in proxy.py terminal:  ", "longcookievaluelongcookievalue" in a_term)
    sys.exit(1 if failed or "longcookievaluelongcookievalue" in b_term else 0)


if __name__ == "__main__":
    main()
