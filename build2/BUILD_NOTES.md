# Build notes (build 2)

## Files

| File | What |
|---|---|
| `proxy.py` | Unchanged reference proxy. |
| `dashboard.py` | Proxy (:8041), event bus, SSE stream parser/assembler, tool-pair index, dashboard server (:8042). Stdlib only, Python 3.9+. |
| `dashboard.html` | The whole UI: plain JS/CSS, no build step, no CDN. |
| `pricing.json` | USD per million tokens, per model. Re-read whenever the file changes. |
| `tests/fake_upstream.py` | Fake Anthropic Messages API: SSE text, thinking, tool_use with `input_json_delta`, 429-then-OK, non-streaming JSON, count_tokens, slow stream. |
| `tests/drive.py` | Sends Claude-Code-shaped traffic through the proxy and checks forwarding, `/events`, pairing, retries, redaction, diff, usage, client drop, export. |
| `tests/browser_check.py` | Headless Chrome (Playwright) check of the UI. |
| `tests/parallel_clear.py` | 20 parallel streams, then `POST /api/clear`. |

## How to run

```
python3 dashboard.py                          # proxy :8041, dashboard http://localhost:8042
ANTHROPIC_BASE_URL=http://localhost:8041 claude

python3 dashboard.py --port 8041 --ui-port 8042 --upstream URL --quiet
python3 dashboard.py --upstream https://chatgpt.com/backend-api/codex   # Codex (best effort, see below)
```

Tests. I used ports 18041/18042/18049 because something else was already listening on 8041/8042:

```
python3 tests/fake_upstream.py 18049 &
python3 dashboard.py --port 18041 --ui-port 18042 --upstream http://127.0.0.1:18049 --quiet &
python3 tests/drive.py 18041 18042 18049              # run first, on a fresh dashboard + fresh fake upstream
python3 -m venv /tmp/pwvenv && /tmp/pwvenv/bin/pip install playwright
/tmp/pwvenv/bin/python tests/browser_check.py 18041 18042     # uses installed Google Chrome (channel="chrome")
python3 tests/parallel_clear.py 18041 18042 18049
```
`drive.py` expects exact exchange ids (1..11), so it needs a fresh dashboard **and** a fresh fake upstream. The fake upstream remembers which 429 bodies it has already seen.

## Acceptance checks (§9)

All runs used synthetic traffic from the fake upstream. **No real Claude Code or API traffic was used.**

| # | Status | How |
|---|---|---|
| 1 | Verified against the fake upstream, not real Claude | Proxy reply is byte-identical to a direct upstream reply (single requests and 20 in parallel). HEAD, count_tokens and non-streaming replies are forwarded. Headers are forwarded with the same SKIP list as proxy.py. **Not tested with a real `claude` login.** |
| 2 | Verified | `exchange_started` reaches an SSE client about 2 ms after sending. In the browser, the new row appears and the Reply tab streams live (spinner is seen while streaming). |
| 3 | Verified | `tool_use` in reply #3, its `tool_result` in request #4. Diff of #4 says "#3 sent 3, resends all 3, adds 2 new" and shows exactly 2 cards. |
| 4 | Verified | usage = `message_start` usage merged with `message_delta` usage. Second turn shows 97% cache hit (fake data). |
| 5 | Verified | Reloaded the page during a slow stream: the agent got all 40 deltas. After reload the row has a spinner and the final text is complete, not doubled. Pairs come back from the snapshot. |
| 6 | Verified | The token string is absent from the stored exchange, snapshot, export, page HTML and terminal log. `authorization`, `x-api-key`, `cookie` and `set-cookie` are cut to 16 chars + `...`. |
| 7 | Verified | Four panels in order. Expanded system text equals the sent text word for word. All 3 tools are listed. Newest-first order is checked. |
| 8 | Verified | Call shows *Pending*. The next request turns it *paired* in place, with no reload. |
| 9 | Verified | Strip shows tokens, cost (or `—` for `claude-imaginary-9`), total, TTFB, TTFT and tok/s. Tools view shows call counts. |
| 10 | Verified | Summary tile → list of exchanges → exchange → `#/exchange/4/message/4` modal with full blocks + raw JSON + copy. Bash row in tool usage → every Bash pair. `429 × 1` → that exchange. |
| 11 | Verified | Long system prompt and long tool result start collapsed ("Show all (80 lines, 6.4k chars)") and expand fully. JSON is indented, coloured and foldable, with depth ≥ 2 folded. A `<script>` tool result is shown as text and `window.__pwned` stays undefined. |
| 12 | Verified (synthetic traffic) | Clicked every row × Request/Reply/Diff/Raw/Tools in headless Chrome: all had content, with zero `pageerror`/`console.error` for the whole session (the favicon 404 was fixed). |
| 13 | Verified | Checks are hidden by default. Shown, they are one dimmed line (opacity .55) with no token or cost fields. |
| 14 | Verified | Row: `429 rate_limit_error: <message> · retry after 6s` and `retried by #6`. Reply tab: error + rate-limit table. Retry row: `retry of #5`, and clicking it jumps to #5. |
| 15 | Verified | Summary `prompt`/`output` equal the per-exchange sums. The row `prompt` equals the strip `prompt` exactly. |

Also verified: pause buffers events and resume applies them, filter, keyboard j/k and 1–4, Expand all/Collapse all, dark mode, `POST /api/clear`, upstream unreachable → 502 + `exchange_error`, client drop mid-stream → `exchange_error`, and the proxy keeps serving afterwards.

**Not verified:** real Claude Code traffic (actual SSE shapes, `?beta=true` paths, OAuth headers, real cache numbers, server-tool blocks), the Codex upstream (Responses API parsing is untested), the 200 ms target over a real network, behaviour at buffer scale (200 large exchanges, multi-MB snapshot), and clipboard copy in a headed browser. Copy was clicked headless with no error, but the content wasn't checked.

## Decisions the spec did not pin down (inputs for the next revision)

**Server / data**
1. **Extra Exchange fields:** `kind` (`"check"` if no request body, else `"api"`), `reason`, `response_body` (non-SSE replies, kept up to 8 MB), `error_info` `{status,type,message,retry_after,ratelimit{},in_stream?}`, `retry_of`, `retried_by`, `conversation` `{prev_id, common_prefix, prev_count, message_count}`. Each SSE event is stored as `{event, data, t}`, where `t` is ms since the request started. Unparseable events become `{event, data:"raw string", parse_error:true}`.
2. **Extra SSE events:** `exchange_retried` (to label the failed row live), `tool_update` (pending/no_result changes and newly registered calls), `cleared`. `exchange_delta` carries `n` (event index) so the browser can drop duplicates after a snapshot. `exchange_done`/`exchange_error` also carry status, timing, cost, error_info, response_body and event_count.
3. **Snapshot omits `response_events`** (only `event_count`), to keep reloads small. The Raw tab fetches `/api/exchanges/<id>` on demand.
4. **Lagging subscriber:** when a browser's bounded queue (5000) overflows, its events are dropped, as the spec requires, and that browser is then sent a fresh snapshot.
5. **Publishing happens outside the store lock.** Only small copies are made under the lock. Event handling in the browser is idempotent.
6. **Order on the proxy path:** the request is forwarded upstream *before* it is parsed and recorded. JSON parsing and fingerprinting run while the upstream is thinking.
7. **TTFB** = proxy received the request → upstream response headers. **TTFT** = first `content_block_delta` (or Codex `output_text.delta`). **Total** = until the upstream stream ends. **tok/s** = output / (total − TTFT).
8. **Tool latency** = result request `started_at` − call reply `ended_at`. Result size = characters (text blocks) or JSON length (other blocks).
9. **No result:** a pending call becomes `no_result` when a later request contains the `tool_use` in its history without a matching `tool_result`. A late result still pairs.
10. **Pairs for calls the dashboard never saw** (started mid-conversation) are created from the history, with `call_exchange: null` ("earlier reply").
11. **Tool calls are only registered from completed replies.** Aborted or failed replies don't create pending calls.
12. **Retry key** = method + path + exact body bytes. "Failed" means status ≥ 400 or a proxy/upstream error. A client abort does not count. Requests with no body are never linked.
13. **Conversation match for Diff:** same system fingerprint, with `cache_control` keys stripped everywhere (Claude Code moves cache breakpoints every turn). The newest earlier request whose messages are a full prefix wins. Otherwise the longest common prefix of at least 1 is used, and the Diff then says the history was edited.
14. **Pricing match:** exact model id, or the id with a trailing `-YYYYMMDD` (or `[1m]`) removed. **No loose prefix matching**, because it would guess prices for new models. The prices in `pricing.json` come from my own memory, not a checked price list. **Verify them.** Newer model ids (e.g. any 4.6 models) are not listed and show `—`. The spec's single `cache_write` price ignores the 1-hour-TTL rate and long-context (>200k) pricing.
15. **Redaction** also covers `set-cookie` in responses, and `cookie` in the terminal output (proxy.py doesn't redact it).
16. **Upstream errors:** proxy.py lets exceptions in `request`/`getresponse` drop the connection. dashboard.py returns a 502 instead and records `exchange_error`. Only POST/GET/HEAD are proxied, as in proxy.py.
17. **`--quiet`** silences traffic printing. The startup lines are still printed.
18. **Clear** empties the buffer and the tool index. In-flight exchanges vanish from the dashboard but still finish forwarding.
19. **Tool index cap** is 2000 entries (oldest dropped). The ring buffer is 200 (constant `MAX_EXCHANGES`, no CLI flag).
20. **Codex (best effort):** `instructions` is treated as the system prompt and `input` as messages. `function_call`/`function_call_output` are mapped to tool_use/tool_result. `response.completed` usage is mapped to input = input − cached, cache_read = cached.

**UI**
21. **Row format:** line 1 is `#id time model status duration [spinner] [retry labels]`; line 2 is `prompt N · out N · cache N% · stop_reason · $cost`. **Row numbers are exact integers**, not `19k`: abbreviations broke "same label, same number" (acceptance 15). An aborted reply shows "aborted by client" and is not counted as an error.
22. **Session summary covers the whole buffer** and ignores the filter and hide-checks. Requests count excludes checks (shown as "+N checks"). Cost sums only priced exchanges and shows "(+N unpriced)". Avg latency is over successful requests. Wall clock = first start → last end.
23. **Filter text** = path + model + the *new* (last) message + reply content + error info. The full history is excluded: otherwise every later request in a conversation would match.
24. **Tabs:** Request | Reply | Diff | Raw | Tools. Tools is session-wide, also on the top-bar button and the `t` key. Statistics drill-downs render in the right pane (`#/stats/<metric>[/<status>]`). The message drill-down is a modal at `#/exchange/<id>/message/<index|reply>`. Per-request strip numbers link to the Reply tab's usage panel.
25. **Collapse rule:** the collapsed preview shows the first 6 lines (at most 1500 chars) plus "…", capped at 6 lines high with a fade. Thinking blocks are always collapsed by default, even when short. The message modal shows full text with no collapse.
26. **JSON:** "deeper than 2 levels" means the root and its children are open and grandchildren start folded. Short strings are shown JSON-escaped; long strings are shown raw (pre-wrap) under the long-text rule. "Copy" copies `JSON.stringify(value)`, which is not byte-exact for the raw request. The Raw SSE text is rebuilt from the parsed events, not stored verbatim.
27. **Expand all** opens nested folds recursively (up to 12 passes), including JSON schemas. This can be heavy on huge requests.
28. **Pause** also buffers a reconnect snapshot, keeping only the newest one.
29. **Approximate tokens** for the system prompt = chars / 4.
30. **Extras:** dark mode via `prefers-color-scheme`. Keyboard: ↑/↓ or j/k move, 1–4 switch tabs, t opens Tools, Esc closes the modal. The banner can be dismissed for the page session.

## Spec gaps / ambiguities found
- §6.1's example row `429 rate limited · retry after 6s` conflicts with §9.14, which requires the error type *and* message in the row. I followed §9.14; the error line wraps to 3 lines.
- §6.5 requires "one meaning per label" but doesn't say whether abbreviated numbers (`18.5k`) are allowed. They make §9.15 fail visually, so the spec should say "exact integers" or "same formatter everywhere".
- "Conversation identified by matching system prompt and message prefix": it doesn't say whether `cache_control` moves count (they must be ignored) or what to do when history is compacted or edited.
- The `snapshot` payload "every exchange" can be tens of MB. The spec should allow omitting raw events.
- "When the subscriber queue is full, events are dropped" leaves the browser stale. The spec should define a resync.
- "Clicking any statistic shows the exchanges behind it" doesn't say where the list appears, and per-request stats point at only one exchange.
- What should *no result* mean exactly, and how is a client abort different from an error?
- §6.7 doesn't say what the diff shows for the first request of a conversation. I show all messages as new.
- `pricing.json` format and model-id matching rules (date suffixes, 1h cache-write price, long-context price) are unspecified.
- The Codex/Responses API rendering (no `system`/`messages`) is not described at all.
- The §3 ports are fixed defaults. If 8041/8042 are taken, the program exits with an error; that case is not specified.
