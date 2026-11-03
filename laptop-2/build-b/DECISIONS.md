# Decisions

Choices made where SPEC.md is ambiguous, silent or self-contradictory.

| § | Issue | Choice |
|---|---|---|
| 1.1, 6.4, 6.5 | Which requests are "the conversation" | Model call = POST with `messages` (or Codex `input` + `model`), excluding `count_tokens`. Everything else is listed as a row and shown in the four empty panels, plus a body panel, but never pairs tools or joins a diff. |
| 3, 11 | Open question: separate UI port | Kept two ports, as §3 specifies. |
| 3.1 | Extra files | The four listed, plus this file and `tests/`. `proxy.py` untouched. |
| 3.2 | `--quiet` against "on startup it prints" | The startup lines always print; `--quiet` silences traffic only. |
| 3.2 vs 7, 9.6 | "Same output as proxy.py", but proxy.py prints `cookie` in full | Redact in the terminal too. This is the one intended difference in terminal output. |
| 3 vs 5, 8 | "Identical to proxy.py", but proxy.py drops the connection without a reply on a failure after connecting | Sends a 502, as it does for a failed connect, and records the error. Other behavior is the same, including 501 for methods other than GET/POST/HEAD, and no chunked request bodies. |
| 3.2, 4 | "Holds the last 200 by default", with no way to change it | Added an optional `--buffer N`. |
| 4 | Extra fields on an Exchange | Added `request_raw` (exact text, for Raw and exports; left out of SSE to save size), `normalized_request` (Codex only), `is_model_call`, `conversation` (digests for the diff), `response_body` (non-SSE replies). Each `response_events` item is `{event, data, raw}`, plus `parse_error` when `data` is a raw string. |
| 4 | Non-streaming replies, which have no events | Kept in `response_body`, and `assembled` is built from it. Capped at 32 MB, flagged `response_body_truncated`; the client still gets everything. This is the one place stored data can be cut. |
| 4 | `status` when the upstream is unreachable | Stays null, since the upstream never answered, although the client gets a 502. |
| 4, 6.5 | Codex usage: its `input_tokens` includes cached tokens | Normalized to the Messages API meaning, so cost and hit % use one formula. Codex has no cache-write count, so it is 0. |
| 5 | The table has no way to carry pending or no_result states, the tool index after a reload, or evictions | Added `tool_update` (entry changed), `tool_index` (sent right after every `snapshot`) and `exchange_evicted`. `snapshot` itself stays a plain list. Payloads in the table are supersets: `exchange_delta` also has `patches`, so the browser never re-implements stream assembly. Added `GET /api/tools`. |
| 5 | How other browsers learn of a clear | `POST /api/clear` publishes an empty `snapshot`. |
| 5, 6.9 | Export format | A plain JSON list of exchanges, oldest first, without the tool index, which `/api/tools` serves. |
| 5 | Clear while a stream is running | That exchange is removed and its later events are ignored. Ids keep increasing, so old links never point at new content. |
| 5, 8 | Stream ends with no final event, or with an SSE `error` event | Recorded in `error`, with `exchange_error`. Mid-stream failures and client drops send `exchange_error` instead of `exchange_done`, carrying `ended_at` and the partial `assembled` so the spinner stops. |
| 6.1 vs 6.5 | §6.1's row fields against §6.5's "these numbers appear in the list row" | The row has all of them, on three lines. |
| 6.1 | Pause: where events are buffered, and Clear while paused | In the browser. A snapshot replaces what was held before it. Clear acts immediately and discards what was held. |
| 6.1 | What "text" in the filter covers | Request body and reply, case-insensitive; headers excluded. |
| 6.2 | "Four sections" but also "Other parameters" | A fifth panel after the four, and not in the index of four parts. |
| 6.2 | Whether §6.2's "starts as a preview" applies to a short system prompt | No: §6.10.1 wins. Short blocks have no toggle. Each block is shown and collapsed separately. |
| 6.2 | Approximate token count | Characters / 4. |
| 6.2 | A request with no `system`, `tools` or `messages` | All four panels still appear, saying so. |
| 6.3, 9.2 | "Reply tab" against "Response tab" | The same tab, named Reply. |
| 6.3 | Tool input before it is complete | Shows "input arriving… N chars", never partial JSON. |
| 6.4 | Which request pairs a result, when every turn resends history | The first model call carrying it. Later resends do not re-pair. |
| 6.4 | Results whose call was never seen (cleared, evicted, or before startup) | Not indexed, so a clear stays clear. The Request tab uses the call from that request's own history if it is there. |
| 6.4 | "The conversation moves on" | A later request whose history holds the call with no result for it, or which has the call's request as a prefix and then continues differently. A same-length retry or another conversation, such as a parallel subagent, never counts. A later result can still turn no_result into paired. After compaction rewrites history this cannot be detected, so the call stays pending. |
| 6.4 | Server-side tools (`server_tool_use`) | Shown in the reply, not paired, because their results arrive in the same reply. |
| 6.4 | Result size | Characters of text; JSON length for non-text parts. |
| 6.5 | "Output tokens per second" | Output tokens / (total − time to first token). |
| 6.5 | "First token" | The first content delta of any kind: text, thinking or tool input. Signatures do not count. |
| 6.5 | Time origin | When the proxy starts handling the request. ttfb ends when upstream headers arrive. |
| 6.5 | Session scope and wall clock | A session is what is in the buffer, so totals shrink on eviction or clear. Wall clock is first start to last end. Requests counts every exchange; average latency covers model calls only, so telemetry does not skew it. |
| 6.5 | Cache hit ratio | cache_read / (input + cache_read + cache_write). |
| 6.5 | Cache write price, since §6.5 has one price but 5-minute and 1-hour writes differ | One price, seeded at the 5-minute rate, so 1-hour writes are underestimated. |
| 6.5 | Which model prices a request | The reply's model, else the request's. Exact key, then the key with a `-YYYYMMDD` suffix removed. Anything else is "—". A missing or null price field also gives "—". |
| 6.5 | Seed prices | From the Claude API reference; derived cache prices are marked in `pricing.json`. No Codex prices, since none were available to verify, so those show "—". |
| 6.5 | What clicking a list-row statistic does | Opens that exchange. Stats-strip numbers open their source values and message links. Session tiles list the exchanges behind them. |
| 6.6 | Link for the reply, which is not in `messages` | `message/N` with N = `messages.length`: the position it takes in the next request. |
| 6.6 | "Exact raw JSON" | The parsed value re-serialized, compact. The Raw tab's request copy is the exact original text. Codex messages show their original `input` item. |
| 6.6 | Images | Never rendered; media type and decoded size only. |
| 6.7 | Matching system prompt | Compares block text, skipping Claude Code's per-request `x-anthropic-billing-header` block. |
| 6.7 | Matching messages | Ignores `cache_control`, which harnesses move each turn, and treats a string and one text block as equal. Previous = newest earlier request with the same system whose messages are a prefix. If none, the longest shared prefix, with a note that history was edited. |
| 6.7 | Identical repeat of a prompt in a new session | Looks like a resend ("nothing new"); the spec's definition cannot tell them apart. |
| 6.10.1 | "First 6 lines" with one very long line | Six visual lines (CSS height), so a single huge line also collapses. |
| 6.10.1 | "State is kept … when you switch between exchanges" | Keyed by position, not exchange: opening system block 2 or message 5 keeps it open on every exchange. History is resent, so position 5 is the same message within a conversation. |
| 6.10.2 | "Deeper than 2 levels" | The root is level 0, so levels 0, 1 and 2 start open. |
| 6.10.2 | What gets the "format" toggle | Strings that parse to an object or array, not `"123"` or `"true"`. |
| 6.10.2 | Long strings in the tree | Shown with real line breaks. Short strings keep JSON escapes. |
| 7 | Secret of 16 characters or fewer: "first 16" would show all of it | Shows only `...`. |
| 7 | Other secret headers | `set-cookie` and `proxy-authorization` are also redacted. Secrets inside bodies are not looked for. |
| 7 | "Local only", beyond the bind address | The UI refuses foreign `Host` and `Origin` headers and sets a CSP, so another website cannot read or clear it. |
| 8 | Keeping forwarding unblocked by parsing as well as publishing | Proxy threads only copy bytes and enqueue. One recorder thread parses and publishes. |
| 8 | What a browser sees after dropped events | Its queue is replaced by a fresh `snapshot`, so a dropped `exchange_done` cannot leave a row spinning. |
| 11 | Open question: JSONL on disk | Not built (§2). History is lost on restart. |
| 10 | M5 polish | Built: keyboard (j/k, arrows, 1-5, /, p, Esc), dark mode, Codex via `--upstream`. Codex tested against the fake only. |
