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
   - **The new message:** what was added this turn, either the user's new
     prompt or the tool results being returned.
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
  - `:8041`: the proxy. Its behavior is identical to `proxy.py` today.
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

### 3.2 CLI

```
python3 dashboard.py [--port 8041] [--ui-port 8042] [--upstream URL] [--quiet]
```

- `--quiet`: turns off terminal printing. Without it, the program prints the
  same output as `proxy.py`.
- On startup it prints both URLs and the line
  `ANTHROPIC_BASE_URL=http://localhost:8041 claude`.

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
| `snapshot`           | right after the browser connects      | list of all exchanges in the buffer   |
| `exchange_started`   | a request reaches the proxy           | id, time, method, path, headers, body |
| `exchange_response`  | upstream status and headers arrive    | id, status, headers                   |
| `exchange_delta`     | each parsed upstream SSE event        | id, raw event                         |
| `exchange_done`      | stream ends                           | id, ended_at, assembled               |
| `exchange_error`     | upstream fails or the client drops    | id, message                           |
| `tool_paired`        | a request carries a `tool_result` for an earlier `tool_use` | tool_use_id, call exchange id, result exchange id, result |

Other endpoints on :8042:
- `GET /`: serves `dashboard.html`.
- `GET /api/exchanges/<id>`: returns the full exchange as JSON.
- `GET /api/export`: returns the whole buffer as a JSON download.
- `POST /api/clear`: empties the buffer.

## 6. UI requirements

### 6.1 Layout
- **Left side:** a list of exchanges, newest at the top, that updates live.
  Each row shows the id, time, model, status, duration, input/output tokens,
  cache hit %, and the stop reason. While a reply is still streaming, the row
  shows a spinner.
- **Right side:** details of the selected exchange, in the tabs described below.
- **Top bar:** connection status (live or reconnecting), a pause/resume
  button (while paused, new events are buffered and not shown), a clear
  button, and a filter box that matches on path, model or text.

### 6.2 Request tab
The request is split into four sections, in this order (see §1.1). Each is
shown as its own visually separate panel, following the formatting rules in
§6.10.

1. **System instructions:** the full text of the `system` field (all of its
   blocks), with its character and approximate token count. Because it is
   long, it follows the long-text rule (§6.10.1): it starts as a preview, and
   one click shows all of it, never truncated.
2. **Tool definitions:** every entry in `tools`, showing its name and full
   description, with its input schema expandable. A total count is shown.
3. **Conversation so far:** every message in `messages` except the last one,
   shown as a chat-style timeline with user and assistant roles.
4. **New message:** the last entry in `messages`, highlighted. This is the
   user's new prompt, or the `tool_result` blocks that answer the previous
   reply's `tool_use`.

In both message sections, `tool_use` and `tool_result` blocks get distinct
styling, and blocks with a `cache_control` marker are flagged.
- **Other parameters:** `model`, `max_tokens`, `thinking`, `temperature`,
  `metadata`, and so on, shown as a small table.

### 6.3 Reply tab
- **The model's text** appears live as it streams.
- **Tool calls the model asks for:** each `tool_use` block is shown with its
  tool name, its `id`, and its input, pretty-printed. The input arrives in
  pieces (`input_json_delta`) and is assembled before it is displayed.
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
  pairs them by this id, across exchanges.
- **Side-by-side view:** each pair is shown on one row.
  - Left: the tool call, with its name and input.
  - Right: the result, with its content, an `is_error` flag shown as a red
    marker, and its size.
  - Labels show which exchange each side came from (e.g. "reply #12 →
    request #13"), and clicking either side jumps to that exchange.
- **Where pairs appear:**
  - In the Reply tab, under each tool call.
  - In the Request tab, beside each `tool_result` in the new message.
  - In a **Tools** view that lists every pair in the session, newest first.
- **Call without a result:** if no matching result has arrived yet, the call is
  shown as *pending*. If the conversation moves on without one, it is shown as
  *no result*.
- **Live update:** when a later request brings the result, the open pair
  updates in place.

### 6.5 Statistics
Statistics are shown alongside the messages. They never replace them.

- **Per request:** these numbers appear in the list row and in a stats strip in
  the detail view.
  - Tokens: input, output, cache read, cache write.
  - Cost: estimated in USD.
  - Time:
    - total duration
    - time to first byte
    - time to first token
    - output tokens per second
- **Per session:** shown in a summary panel at the top.
  - Totals: request count, tokens, cost, wall-clock time.
  - Average latency.
  - Cache hit ratio over the session.
- **Tool usage:**
  - For each tool name: number of calls, number of errors, average result
    size, and average time from call to result. That time is measured from the
    end of the reply to the start of the request that carries the result.
- **Cost calculation:**
  - The formula uses `usage` and a pricing table:
    `input × in_price + output × out_price + cache_read × read_price + cache_write × write_price`.
  - The pricing table is a small editable file (`pricing.json`, priced per
    million tokens, one entry per model).
  - Models that aren't in the table show "—" instead of a guessed price.
- **Statistics link back to messages:** clicking any statistic shows the
  exchanges behind it. For example, clicking the "Bash" row in tool usage lists
  every Bash call and its result.

### 6.6 Drill-down (every message is openable)
- **Opening a message:** every message, in both the request and the reply, can
  be opened.
  - Each one shows its role and position in `messages`.
  - It lists every content block, with that block's type and details:
    - text: the full text
    - `tool_use`: the id, name, and input
    - `tool_result`: the `tool_use_id`, the content, and `is_error`
    - images: the media type and size
    - thinking: the full content
    - `cache_control` markers
  - Its exact raw JSON can be viewed, with a copy button.
- **Nothing is truncated:** in a collapsed view, text may be shortened with
  "…". Opening it always shows the full content.
- **Linking:** each exchange and message has a stable link, such as
  `#/exchange/13/message/42`.

### 6.7 Diff view (the main learning feature)
- Compares the selected request with the previous request from the same
  conversation. A conversation is identified by matching system prompt and
  message prefix.
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
- **Collapsed:** long text starts collapsed, showing the first 6 lines and a
  fade at the bottom.
  - A toggle shows the size, such as **"Show all (342 lines, 18.2k chars)"**.
  - The toggle becomes **"Collapse"** once the text is open.
- **Expanded:** shows the full text, never truncated, keeping whitespace and
  line breaks (`white-space: pre-wrap`). Long lines wrap, so there is no
  sideways scrolling.
- **Expand all / Collapse all:** each panel and each tab has these controls.
- **State is kept:** what is open or closed stays that way while new events
  stream in and when you switch between exchanges.
- **Short text** is shown in full, with no toggle.

#### 6.10.2 JSON is readable
- **Indentation:** pretty-printed with 2-space indents, never shown as a
  single line.
- **Syntax colors:** keys, strings, numbers, booleans and `null` each get
  their own color.
- **Collapsible tree:** every object and array can fold. A folded node shows a
  summary, such as `{…} 5 keys` or `[…] 12 items`.
  - Nodes deeper than 2 levels start folded.
- **JSON inside strings:** a string that is itself valid JSON gets a "format"
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

## 7. Security and privacy

- **Redaction:** `authorization`, `x-api-key` and `cookie` headers are
  redacted before they are stored, showing only the first 16 characters then
  `...`. They are still forwarded to the upstream unchanged.
- **Local only:** both servers bind to `127.0.0.1`, never `0.0.0.0`.
- **Warning in the UI:** a banner says that conversation contents are shown in
  full, so the user should be careful when sharing screenshots or exports.

## 8. Correctness constraints (carried over from `proxy.py`)

- The proxy must not change requests or responses. Headers pass through except
  hop-by-hop headers, and `accept-encoding` is dropped so the reply comes back
  as readable plain text.
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
   Response tab.
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

## 10. Milestones

1. **M1:** add the event bus and the `/events` SSE endpoint. A bare HTML page
   lists the exchanges.
2. **M2:** parse the upstream stream and show live response text and usage.
3. **M3:** build the Request tab (system prompt, tools, messages timeline).
4. **M4:** add the diff view, filter, pause and export.
5. **M5:** polish: keyboard navigation, dark mode, and testing with Codex
   (`--upstream`).

## 11. Open questions

- Should the in-memory buffer also be written to disk (JSONL) so history
  survives a restart?
- Is a separate UI port acceptable, or should the dashboard sit under a
  reserved path on 8041 (such as `/__dash`) so there is only one port?
