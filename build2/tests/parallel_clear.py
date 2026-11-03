#!/usr/bin/env python3
"""20 parallel streamed requests through the proxy, then POST /api/clear.

    python3 tests/parallel_clear.py [PROXY_PORT UI_PORT UPSTREAM_PORT]
"""
import http.client
import json
import sys
import threading

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import drive  # noqa: E402

PROXY, UI, UP = (int(x) for x in (sys.argv[1:4] if len(sys.argv) > 3 else (8041, 8042, 8049)))
results = []


def one(i):
    st, _, body = drive.send(PROXY, "POST", "/v1/messages", drive.req([{"role": "user", "content": f"parallel {i}"}]))
    st2, _, direct = drive.send(UP, "POST", "/v1/messages", drive.req([{"role": "user", "content": f"parallel {i}"}]))
    results.append(st == 200 and body == direct)


ts = [threading.Thread(target=one, args=(i,)) for i in range(20)]
[t.start() for t in ts]
[t.join() for t in ts]
print("parallel: %d/20 byte-identical" % sum(results))
exp = json.loads(drive.send(UI, "GET", "/api/export")[2])
done = [e for e in exp["exchanges"] if e["request_body"] and "parallel" in json.dumps(e["request_body"]["messages"])]
print("parallel exchanges recorded complete:", sum(1 for e in done if e["ended_at"] and e["assembled"]["stop_reason"] == "end_turn"))
c = http.client.HTTPConnection("127.0.0.1", UI)
c.request("POST", "/api/clear")
print("clear:", c.getresponse().read())
print("after clear:", len(json.loads(drive.send(UI, "GET", "/api/export")[2])["exchanges"]), "exchanges")
