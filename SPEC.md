# SPEC: Live Agent-Traffic Dashboard

## 1. Goal

Build a web dashboard that shows, live, every API request an agent harness
(Claude Code, or Codex) makes, and the reply that comes back. It builds on
`proxy.py`. The proxy keeps forwarding traffic unchanged, but instead of only
printing to the terminal, it also publishes each exchange to a browser page.

The dashboard is a learning tool. It should make it easy to see what the
harness actually sends: the system prompt, the tool definitions, the growing
`messages` array, tool calls and results, and the token and cache usage.

### 1.1 Core requirements (must have)

The dashboard must show:

1. **Every request** the harness sends and **every reply**, as they happen.
   Replies appear while they are still streaming. The list is ordered
   **newest first**.
2. **The parts of each request**, each shown separately:
   - **System instructions:** shown in full, with no truncation.
   - **Tool definitions:** every tool's name, description and input schema.
   - **The conversation so far:** all earlier messages that the request
     resends.
   - **The new message:** the last entry in `messages`, either the user's new
     prompt or the tool results being returned. The Diff view (§6.7) shows
     everything added since the previous request.
3. **The reply:** the model's text and the tool calls it asks for (see §6.3).
4. **Tool calls, each beside its result.** A tool call (`tool_use`) appears in
   one reply, and its result (`tool_result`) arrives in a *later* request. The
   dashboard pairs them and shows them side by side (see §6.4).
5. **Statistics:** tokens per request, cost, time, and tool usage (see §6.5).
   Statistics are an addition, not a replacement. **Every message must be
   openable**, so you can drill down into its details (see §6.6).
6. **Readable format** (see §6.10):
   - Long text is collapsible.
   - JSON is pretty-printed and foldable.
   - The parts of a request are clearly separated.

## 2. Non-goals

- Changing, blocking or replaying traffic. The proxy stays read-only.
- Multiple users, logins, or remote access. It runs on localhost only.
- Storing data long-term or searching it (an optional export is enough, see §6.6).
- Support for providers other than the Anthropic Messages API and the Codex
  upstream that `proxy.py` already supports.

## 3. Architecture

```
 claude ──HTTP──▶ proxy (:8041) ──HTTPS──▶ api.anthropic.com
                     │
                     │ in-process event bus
                     ▼
              dashboard server (:8042) ──SSE──▶ browser
```

- **One Python process, two ports.**
  - `:8041`: the proxy. Its behavior is identical to `proxy.py` today, except
    for the differences listed in §3.2 and §8.
  - `:8042`: the dashboard. It serves the HTML page and a Server-Sent Events
    (SSE) stream.
  - The ports are separate so dashboard routes can never collide with API
    paths such as `/v1/messages`.
- **Standard library only** (`http.server`, `http.client`, `json`, `threading`,
  `queue`), the same as `proxy.py`, so there is nothing to install.
- **Frontend:** one static `dashboard.html` file using plain JS and CSS. No
  build step and no CDN dependencies, so it works offline.
- **Live transport:** SSE (`EventSource`), which is one-way from server to
  browser and enough for this. The browser reconnects automatically.

### 3.1 Files

| File             | Purpose                                              |
|------------------|------------------------------------------------------|
| `proxy.py`       | Unchanged. Keep it as the naive reference version.   |
| `dashboard.py`   | Proxy, event bus, dashboard server, and stream parser. |
| `dashboard.html` | The UI, served by `dashboard.py`.                    |
| `pricing.json`   | Editable price per million tokens for each model (§6.5). |
| `tests/`         | A fake upstream and the checks described in §9.1.    |

### 3.2 CLI

```
python3 dashboard.py [--port 8041] [--ui-port 8042] [--upstream URL] [--quiet]
```

- `--quiet`: turns off traffic printing only. The startup lines always print.
  Without it, traffic output matches `proxy.py`, except that cookies are
  redacted (§7).
- On startup it prints both URLs and the line
  `ANTHROPIC_BASE_URL=http://localhost:<port> claude`, using the actual port.
- `--buffer N`: optional size of the ring buffer (§4), default 200.

## 4. Data model

Each request/response pair is one **Exchange**, kept in memory in a ring
buffer that holds the last 200 exchanges by default.

```jsonc
{
  "id": 17,                       // increasing integer
  "started_at": "2026-11-03T08:12:01.123Z",
  "ended_at": null,               // set when the stream finishes
  "method": "POST",
  "path": "/v1/messages?beta=true",
  "status": 200,                  // null until the upstream answers
  "request_headers": { ... },     // secrets redacted, see §7
  "request_body": { ... },        // parsed JSON, or a string if it isn't JSON
  "response_headers": { ... },
  "response_events": [ ... ],     // the SSE events as they were parsed
  "assembled": {                  // built up from the events
    "model": "...",
    "content": [ {"type":"text","text":"..."},
                 {"type":"tool_use","name":"Bash","input":{...}} ],
    "stop_reason": "tool_use",
    "usage": { "input_tokens": 12, "output_tokens": 340,
               "cache_read_input_tokens": 18000,
               "cache_creation_input_tokens": 500 }
  },
  "timing": { "ttfb_ms": 410, "ttft_ms": 620, "total_ms": 5300 },
  "cost_usd": 0.0123,             // null if the model isn't in pricing.json
  "error": null                   // e.g. "upstream unreachable"
}
```

Rules for these fields:

- **`error`** is set for every failed exchange: the upstream is unreachable or
  drops, the reply is HTTP 4xx/5xx, the stream carries an SSE error event, or
  the stream ends without its final event. `status` keeps the real upstream
  status, and stays `null` if the upstream never answered.
- **`response_body`** is an added field that holds a reply that isn't a stream.
- **Nothing stored is cut.** Bodies are kept whole. Only the ring buffer limits
  memory.
- **`ttfb_ms`** runs from receiving the request to the upstream headers.
- **`ttft_ms`** runs to the first non-empty text, thinking or tool-input delta.
  It is `null` for replies that aren't streamed.
- **Ids** keep rising, and never reset on clear.

Tool calls are paired with their results in a separate index, keyed by the
`tool_use` id:

```jsonc
{
  "toolu_01abc": {
    "name": "Bash", "input": {...},
    "call_exchange": 12,           // the reply that asked for the tool
    "result_exchange": 13,         // null until the result arrives
    "result": [...], "is_error": false,
    "status": "paired"             // pending | paired | no_result
  }
}
```

## 5. Event stream (`GET /events` on :8042)

SSE events, all with a JSON payload:

| event                | when                                  | payload                               |
|----------------------|---------------------------------------|---------------------------------------|
| `snapshot`           | right after the browser connects      | `{exchanges: [...], tools: {...}}`: every exchange in the buffer plus the tool-pair index, so a reload restores the pairs too |
| `exchange_started`   | a request reaches the proxy           | id, time, method, path, headers, body |
| `exchange_response`  | upstream status and headers arrive    | id, status, headers                   |
| `exchange_delta`     | each parsed upstream SSE event        | id, raw event                         |
| `exchange_done`      | stream ends                           | id, ended_at, assembled               |
| `exchange_error`     | the exchange failed (§4) or the client drops | id, message, ended_at, partial assembled |
| `tool_paired`        | a request carries a `tool_result` for an earlier `tool_use` | tool_use_id, call exchange id, result exchange id, result |
| `tool_updated`       | a call is added or becomes *no result* (pairing uses `tool_paired`) | the tool index entry |
| `exchange_evicted`   | the ring buffer drops an exchange     | id                                    |
| `cleared`            | `POST /api/clear` empties the buffer  | none                                  |

- **One ending:** each exchange ends with exactly one of `exchange_done` or
  `exchange_error`, and either one stops the row's spinner.
- **Lost events:** a subscriber that lost events (§8) gets a fresh `snapshot`,
  so a dropped ending can't leave a row spinning.
- **Clear during a stream:** an exchange still in flight leaves the buffer and
  its later events are ignored, but its forwarding continues untouched. Clear
  applies immediately even while paused, and drops the held events.

Other endpoints on :8042:
- `GET /`: serves `dashboard.html`.
- `GET /api/exchanges/<id>`: returns the full exchange as JSON.
- `GET /api/export`: returns the whole buffer as a JSON download.
- `POST /api/clear`: empties the buffer.

## 6. UI requirements

### 6.1 Layout
- **Left side:** a list of exchanges, newest at the top, that updates live.
  Each row shows the id, time, model, status, duration, prompt/output tokens
  (as defined in §6.5), cost, cache hit %, and the stop reason. While a reply
  is still streaming, the row shows a spinner.
  - **Selection:** when nothing is selected, the newest exchange is selected
    automatically. Once the user picks a row, new traffic doesn't change the
    selection.
  - **Requests with no body** (for example Claude Code's `HEAD /api/hello`
    connection checks) are shown as a single dimmed line with only the id,
    time, method, path, status and duration. They show no token, cache or
    cost fields. A "hide checks" toggle in the list header hides them; it is
    on by default. This rule wins over the error-row rule, so a failed check
    stays a dimmed line, and it isn't counted in the session's Errors.
  - **Error rows** (a model call or other request with a body, with status
    4xx/5xx or a proxy error) show a short reason
    instead of the token fields, e.g. `429 rate limited · retry after 6s`
    (see §6.11).
- **Right side:** details of the selected exchange, in the tabs described below.
- **Top bar:** connection status (live or reconnecting), a pause/resume
  button (while paused, new events are buffered and not shown), a clear
  button, and a filter box that matches on path, model or text.

### 6.2 Request tab
The request is split into four sections, in this order (see §1.1). Each is
shown as its own visually separate panel, following the formatting rules in
§6.10.

1. **System instructions:** the full text of the `system` field (all of its
   blocks), with its character and approximate token count (characters ÷ 4).
   The long-text rule (§6.10.1) applies to each block separately: a long block
   starts as a preview, and one click shows all of it, never truncated. A
   short block is shown in full.
2. **Tool definitions:** every entry in `tools`, showing its name and full
   description, with its input schema expandable. A total count is shown.
3. **Conversation so far:** every message in `messages` except the last one,
   shown as a chat-style timeline with user and assistant roles.
4. **New message:** the last entry in `messages`, highlighted. This is the
   user's new prompt, or the `tool_result` blocks that answer the previous
   reply's `tool_use`.

In both message sections, `tool_use` and `tool_result` blocks get distinct
styling, and blocks with a `cache_control` marker are flagged.

**Other parameters** is a separate fifth panel after the four parts, outside
their index: `model`, `max_tokens`, `thinking`, `temperature`, `metadata`, and
so on, shown as a small table.

### 6.3 Reply tab
- **The model's text** appears live as it streams. A block stays open while
  it streams. Viewed later, it follows the long-text rule (§6.10.1).
- **Tool calls the model asks for:** each `tool_use` block is shown with its
  tool name, its `id`, and its input, pretty-printed. The input arrives in
  pieces (`input_json_delta`) and is assembled before it is displayed.
  Until then the call shows "receiving input… N chars", never partial JSON.
  - Each tool call links to its result (§6.4). Until the result arrives, the
    call is marked *pending*.
- **Blocks in order:** text, tool-call and thinking blocks are shown in the
  order the model produced them. Thinking blocks are collapsed by default.
- **Stop reason and model:** `stop_reason` (such as `end_turn` or `tool_use`)
  and the model name.
- **Usage panel:** input, output, cache-read and cache-write tokens, plus the
  cache hit ratio.

### 6.4 Tool calls beside their results
- **Pairing:** each `tool_use` block in a reply has an `id`. A later request
  contains a `tool_result` block whose `tool_use_id` matches it. The server
  pairs them by this id, across exchanges. The first request that carries a
  result does the pairing. `server_tool_use` blocks are shown in the reply but
  aren't paired.
- **Side-by-side view:** each pair is shown on one row.
  - Left: the tool call, with its name and input.
  - Right: the result, with its content, an `is_error` flag shown as a red
    marker, and its size (characters of text, plus JSON length for non-text
    blocks).
  - Labels show which exchange each side came from (e.g. "reply #12 →
    request #13"), and clicking either side jumps to that exchange.
- **Where pairs appear:**
  - In the Reply tab, under each tool call.
  - In the Request tab, beside each `tool_result` in the new message.
  - In a **Tools** view that lists every pair in the session, newest first.
- **Call without a result:** if no matching result has arrived yet, the call is
  shown as *pending*. It becomes *no result* when a later model call in the same
  conversation (§6.7) has the call in its history, and the message after the
  call has no `tool_result` with that id.
  - A retry (§6.11) doesn't count.
  - A result that arrives later still turns the call into *paired*.
  - If compaction rewrites the history, this can't be detected, so the call
    stays *pending*.
- **Live update:** when a later request brings the result, the open pair
  updates in place.

### 6.5 Statistics
Statistics are shown alongside the messages. They never replace them.

- **Definitions:**
  - **Model call:** a POST that carries `messages` (or Codex `input` and
    `model`), excluding `count_tokens`.
  - **Session:** what is in the buffer now, since the last clear. Totals can
    shrink when old exchanges are evicted, and the summary says when that has
    happened.
  - **Cache hit:** `cache read / prompt`, weighted by tokens over a session.
  - **Output tokens per second:** `output / (total duration - time to first token)`.
  - **Wall-clock time:** first start to last end.
  - **Average latency:** mean total duration of model calls only, so connection
    checks don't distort it.
- **Per request:** the list row shows the fields in §6.1. A stats strip in the
  detail view shows all of these numbers.
  - Tokens: input, output, cache read, cache write.
  - **One meaning per label, everywhere** (list row, stats strip, session
    summary):
    - `input` = `usage.input_tokens` only (the uncached part, often tiny).
    - `prompt` = input + cache read + cache write (everything the model read).
    - `output` = `usage.output_tokens`.
    - The list row shows `prompt` and `output`. The session summary shows
      `prompt`, `output`, `cache read` and `cache write`, with the same
      labels. A label never means a different sum in two places.
  - Cost: estimated in USD.
  - Time:
    - total duration
    - time to first byte
    - time to first token
    - output tokens per second
- **Per session:** shown in a summary panel at the top.
  - Totals: model calls, errors, tokens, cost, wall-clock time. Requests with
    no body are counted separately as checks, and add nothing to the other
    totals. Failed model calls count, and add only the usage they report.
  - Average latency.
  - Cache hit ratio over the session.
- **Tool usage:**
  - For each tool name: number of calls, number of errors, average result
    size, and average time from call to result. That time is measured from the
    end of the reply to the start of the request that carries the result.
- **Cost calculation:**
  - The formula uses `usage` and a pricing table:
    `input × in_price + output × out_price + cache_read × read_price + cache_write_5m × write_5m_price + cache_write_1h × write_1h_price`.
  - **Cache writes:** when `usage.cache_creation` splits writes into 5-minute
    and 1-hour parts, use that split. Otherwise, price all writes at the
    5-minute rate.
  - **Long-context prices** (such as a higher rate above 100k tokens) aren't
    modeled, so the cost is labelled as an estimate.
  - The pricing table is a small editable file (`pricing.json`, priced per
    million tokens, one entry per model). A model matches exactly, or after
    removing a `-YYYYMMDD` suffix.
  - Models that aren't in the table show "—" instead of a guessed price.
- **Statistics link back to messages:** clicking any statistic shows the
  exchanges behind it. For example, clicking the "Bash" row in tool usage lists
  every Bash call and its result.

### 6.6 Drill-down (every message is openable)
- **Opening a message:** every message, in both the request and the reply, can
  be opened. Long text in an opened message starts expanded.
  - Each one shows its role and position in `messages`.
  - It lists every content block, with that block's type and details:
    - text: the full text
    - `tool_use`: the id, name, and input
    - `tool_result`: the `tool_use_id`, the content, and `is_error`
    - images: the media type and size
    - thinking: the full content
    - `cache_control` markers
  - Its raw JSON can be viewed, with a copy button. This is the parsed value
    re-serialized compactly. The Raw tab (§6.8) keeps the original request
    text.
- **Nothing is truncated:** in a collapsed view, text may be shortened with
  "…". Opening it always shows the full content.
- **Linking:** each exchange and message has a stable link, such as
  `#/exchange/13/message/42`. Message positions start at 0. The reply's link is
  `#/exchange/13/message/reply`.

### 6.7 Diff view (the main learning feature)
- Compares the selected request with the previous request from the same
  conversation. A conversation is identified by matching system prompt and
  message prefix.
- **Matching rules:**
  - Ignore `cache_control`, because Claude Code moves it between requests.
  - Skip the per-request billing-header block in `system`.
  - Treat a plain string and a single text block as equal.
- **Previous request:** the nearest earlier model call whose messages are a
  prefix of this one, skipping failed attempts.
- **Edge cases:**
  - A request with no new messages is labelled "no new messages". The word
    "retry" is used only as §6.11 defines it.
  - If older messages changed, compare with the longest shared prefix and show
    a warning.
- Shows only the messages that are new since that previous request, so it is
  clear that the harness resends the entire history on every turn.

### 6.8 Raw tab
- The request and response headers, and the raw request JSON and raw SSE
  lines, each with a "copy" button.

### 6.9 Export
- A button that downloads the buffer as `traffic-<timestamp>.json`.

### 6.10 Display format (applies everywhere)

#### 6.10.1 Long text is collapsible
- **What counts as long:** text over 12 lines or 1,000 characters. This
  applies to system blocks, message text, tool results, thinking, and JSON
  strings.
- **Collapsed:** long text starts collapsed, showing the first 6 visual lines
  after wrapping and a fade at the bottom, so one huge line also collapses.
  - A toggle shows the size, such as **"Show all (342 lines, 18.2k chars)"**.
  - The toggle becomes **"Collapse"** once the text is open.
- **Expanded:** shows the full text, never truncated, keeping whitespace and
  line breaks (`white-space: pre-wrap`). Long lines wrap, so there is no
  sideways scrolling.
- **Expand all / Collapse all:** each panel and each tab has these controls.
- **State is kept:** what is open or closed stays that way while new events
  stream in and when you switch between exchanges. State is stored per exchange
  and per block, and lasts until the page is reloaded.
- **Short text** is shown in full, with no toggle.

#### 6.10.2 JSON is readable
- **Indentation:** pretty-printed with 2-space indents, never shown as a
  single line.
- **Syntax colors:** keys, strings, numbers, booleans and `null` each get
  their own color.
- **Collapsible tree:** every object and array can fold. A folded node shows a
  summary, such as `{…} 5 keys` or `[…] 12 items`.
  - The root is level 0. Levels 0 to 2 start open, and deeper nodes start
    folded.
- **JSON inside strings:** a string that parses to a JSON object or array gets a "format"
  toggle that shows it parsed. Tool results are often like this.
- **Long string values** follow the long-text rule (§6.10.1).
- **Copy button:** each JSON block has one that copies exact, unformatted
  JSON.
- **Font:** JSON and code use a monospace font. Prose uses a regular font.

#### 6.10.3 The parts of a request are clearly apart
- **Panels:** each of the four parts is its own panel (§6.2).
  - Each panel has a header with its name and a count, for example:
    - **System instructions · 3 blocks · ~14k tokens**
    - **Tools · 18**
    - **Conversation so far · 46 messages**
    - **New message**
  - Each part has its own accent color, used on the panel's left border and
    on its label. Panels are separated by clear gaps.
- **Fixed order:** the order is always system → tools → conversation → new
  message.
- **Navigation:** the panel headers stay pinned while scrolling. A small
  index of the four parts at the top jumps to each one.
- **Messages:** each message is a card labelled with its role and index (e.g.
  `user #45`).
  - User and assistant cards look different.
  - `tool_use`, `tool_result`, `thinking` and `text` blocks each have a
    labelled badge.
- **Reply:** the reply (§6.3) is a separate tab with its own color, so it is
  never confused with the request.
- **Safe rendering:** all content is shown as text, never as HTML. Model
  output and tool results may contain markup, which is displayed, not run.

### 6.11 Errors and retries
- **Reason, not just a code:** for any 4xx/5xx reply, the row and the top of
  the Reply tab show the error `type` and `message` from the response body
  (e.g. `rate_limit_error: …`).
- **Rate limits (429):** also show the `retry-after` header and any
  `anthropic-ratelimit-*` headers (limit, remaining, reset) as a small table
  in the Reply tab.
- **Retries are linked:** a request whose body is identical to an earlier
  failed request is labelled "retry of #N" in its row, and the failed one
  is labelled "retried by #M". Clicking the label jumps to the other one.
  N is the nearest such failed request, so a chain reads #3 retry of #2, and
  #2 retry of #1. A failed request is linked to only one retry.
- **Session summary:** the Errors count is split by status (e.g.
  `429 × 4`), and clicking it lists those exchanges.

## 7. Security and privacy

- **Redaction:** `authorization`, `x-api-key`, `cookie`, `set-cookie` and
  `proxy-authorization` headers are redacted before they are stored or printed,
  showing only the first 16 characters then `...`. A secret of 16 characters or
  fewer shows only `...`. They are still forwarded to the upstream unchanged.
- **Request bodies aren't redacted:** a secret inside a body is shown as sent.
- **Local only:** both servers bind to `127.0.0.1`, never `0.0.0.0`. The
  dashboard server rejects a non-localhost `Host`, and a non-localhost `Origin`
  on POST.
- **Warning in the UI:** a banner says that conversation contents are shown in
  full, so the user should be careful when sharing screenshots or exports.

## 8. Correctness constraints (carried over from `proxy.py`)

- The proxy must not change requests or responses. Headers pass through except
  hop-by-hop headers, and `accept-encoding` is dropped so the reply comes back
  as readable plain text. It keeps `proxy.py`'s behavior, including adding
  `Connection: close`.
- One deliberate difference from `proxy.py`: if the upstream fails after
  connecting but before answering, the client gets a 502 and the error is
  recorded.
- Bytes are streamed to the client as they arrive, using `read1`. The
  dashboard must never delay forwarding:
  - Publishing to the event bus must not block. Each subscriber has a bounded
    queue, and when it is full, events for that subscriber are dropped.
  - If the dashboard is slow or no browser is connected, the agent's speed is
    unaffected.
- If parsing an upstream SSE event fails, the event is logged as a raw string.
  It never interrupts forwarding.
- Requests arriving in parallel are handled correctly (the server uses
  `ThreadingHTTPServer`, and the event bus is thread-safe).

## 9. Acceptance criteria

1. With `python3 dashboard.py` running, `ANTHROPIC_BASE_URL=http://localhost:8041 claude`
   works exactly as it does through `proxy.py`.
2. Opening `http://localhost:8042` and sending a prompt in Claude Code makes a
   new row appear within about 200 ms, and the reply text streams into the
   Reply tab.
3. A turn that uses a tool shows the `tool_use` block in one exchange and the
   matching `tool_result` in the next. The diff view shows only those new
   messages.
4. Usage numbers match the `message_start` and `message_delta` events, and a
   repeated prompt shows a high cache hit %.
5. Closing the browser tab or reloading it during a stream doesn't affect the
   agent. A reload restores history from the `snapshot` event.
6. No full token appears anywhere in the UI, the exports, or the logs.
7. For any request, the Request tab shows the system instructions word for
   word with nothing cut off, all the tool definitions, the earlier
   conversation, and the new message as four separate sections. The list is
   ordered newest first.
8. When Claude Code runs a tool:
   - The call shows as *pending* in the reply.
   - When the next request arrives, the result appears beside the call, with
     no page reload.
9. Each request shows its tokens, cost (or "—" if the model isn't priced),
   duration, and time to first token. The Tools view shows call counts for
   each tool.
10. Starting from any statistic, you can click through to the underlying
    message and open its full content and raw JSON.
11. Formatting:
    - A long system prompt and a long tool result both start collapsed, and
      each can be opened to show all of it.
    - All JSON is shown indented, colored, and foldable.
    - The four request parts are visibly separate panels, each with its own
      header.
    - A tool result that contains `<script>` is shown as plain text and is
      not run.
12. **No crashes in the page:** with live traffic, clicking any row and
    opening each of the Request, Reply, Diff and Raw tabs, and the Tools
    view, shows content, and the browser console shows no errors. This is
    checked in a real browser (not only with `curl`) before a build is
    called done.
13. `HEAD /api/hello` checks are hidden by default, and when shown they
    appear as dimmed one-line rows without token or cost fields.
14. A 429 reply shows `rate_limit_error`, its message and the retry-after
    value in its row and in the Reply tab, and the request that retried it
    is labelled "retry of #N".
15. The same token label shows the same number in the row, the stats strip
    and (summed) the session summary. For example, the summary's `prompt`
    equals the sum of the rows' `prompt`.
16. With live traffic, the Diff view of a later request in a conversation shows
    only the new messages, even when `cache_control` moved between requests.
17. Every exchange ends with exactly one of `exchange_done` or `exchange_error`.
    No row keeps spinning after a failure.
18. A call whose conversation moved on without a result shows *no result*, and
    a result that arrives later turns it into *paired*.
19. A long live session that goes through compaction keeps working. Calls whose
    history was rewritten stay *pending*, and the page shows no errors.

### 9.1 How to test

1. **Fake upstream:** a small fake model API in `tests/`, for fast checks that
   spend no tokens. This alone isn't enough: it didn't catch the row-click crash
   in build 1.
2. **Real browser:** check the page in a real browser, as AC12 requires.
3. **Live traffic:** run one real session by hand before calling a build done.
   ACs 12, 13, 14, 16 and 19 need live traffic.

Keep tests small: about one check per acceptance criterion. In builds A to C,
test code was about as large as the product, and each build took about 25
minutes.

## 10. Milestones

1. **M1:** add the event bus and the `/events` SSE endpoint. A bare HTML page
   lists the exchanges.
2. **M2:** parse the upstream stream and show live response text and usage.
3. **M3:** build the Request tab (system prompt, tools, messages timeline).
4. **M4:** add the diff view, filter, pause and export.
5. **M5 (optional, only after M1 to M4 work on live traffic):** polish:
   keyboard navigation, dark mode, and testing with Codex (`--upstream`).

## 11. Open questions

- **Snapshot size:** every request carries the full system prompt and tools, so
  200 exchanges can be tens of MB. Against a fake upstream, a 41 MB snapshot
  barely affected forwarding (median 1.8 ms against 1.3 ms). If reloads are slow
  on live traffic, send summaries in the snapshot and load details through
  `/api/exchanges/<id>`.

Decided: no disk persistence, and the UI keeps its separate port.

## 12. Findings in the traffic

Seen on live traffic through build 1, unless noted:

- **429 before requests:** each real request first failed with 429 and was sent
  again a few seconds later, with a `HEAD /api/hello` check before each attempt.
  This may depend on the account's limits at the time.
- **Prompt caching:** request #9 wrote about 36k tokens to the cache (0% hit,
  $0.185). Request #11 resent about the same 36k with a 98% hit for $0.022,
  about 8 times cheaper.
- **Small side request:** request #10 (156 tokens) may serve a side purpose such
  as a session title. This hasn't been confirmed.
- **`cache_control` moves between requests:** seen with a real `claude` client
  against a fake upstream, not yet on live traffic.
