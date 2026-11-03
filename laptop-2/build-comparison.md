# Build comparison: SPEC.md at commit 76c7a4e

Three independent builds of the same spec, on the second laptop. Differences and
decisions come from each agent's own report, not from reading the code.

## Results

| | A | B | C |
|---|---|---|---|
| Build time | 24 min | 25 min | 25 min |
| `dashboard.py` lines | 1095 | 1189 | 1241 |
| `dashboard.html` lines | 1059 | 1200 | 1438 |
| Tests re-run after the builds finished | 76 + 112 pass | 96 + 97 pass | 40 + parity + 117 pass |
| Real Claude Code client | no | yes, against a fake upstream | no |
| Real model traffic | no | no | no |

All tests use each build's own fake upstream and were written by the same agent
that wrote the code. They do not show that a build works on live traffic.

## Where the builds differed (the spec does not pin these)

| Topic | Section | A | B | C |
|---|---|---|---|---|
| Failure mid-stream | §5 | `exchange_error`, then `exchange_done` | `exchange_error` instead of done | exactly one of the two |
| HTTP 4xx/5xx | §4 | keeps status, `error: null` | not stated in report | counted as an error |
| Link to a reply | §6.6 | `message/reply` | `message/N`, N = `messages.length` | `message/N`, N = `messages.length` |
| Open/closed state | §6.10.1 | per exchange | by position, across exchanges | until reload; key not stated |
| Short secret | §7 | at most half its length | `...` only | `...` only |
| Average latency | §6.5 | mean total duration | model calls only | mean total duration |
| Diff "previous request" | §6.7 | longest matching prefix, ties to newest | newest earlier prefix, falls back to longest shared prefix | nearest earlier prefix, skips failed attempts |
| Stored non-streaming reply | §4, §6.6 | not stated in report | capped at 32 MB and flagged | not stated in report |
| Long text in drill-down | §6.6 | not stated in report | not stated in report | starts open |

## Where all three made the same choice without being told

Promote these into the spec so they stay stable:

- `--quiet` silences traffic only; startup lines always print.
- The terminal redacts cookies, unlike `proxy.py`.
- Row fields are the union of §6.1 and §6.5.
- "Other parameters" is a fifth panel, outside the four-part index.
- The tab is called Reply (AC2 says "Response").
- The formula's single cache-write price uses the 5-minute rate, so 1-hour writes are underestimated.
- Cache hit = read / (input + read + write).
- Tokens per second = output / (total - TTFT).
- `cache_control` is ignored when matching conversations.
- Extra events were needed: all three added tool snapshot, tool update and eviction events that §5 lacks.

## Suggested spec changes

1. **§3.2:** "Without `--quiet`, output matches `proxy.py` except that cookies are redacted. `--quiet` silences traffic only."
2. **§5:** add the missing events for tool-index restore, tool status updates, eviction and clear. State that each exchange ends with exactly one of `exchange_done` or `exchange_error`.
3. **§4:** define whether HTTP 4xx/5xx sets `error`.
4. **§6.1/§6.5:** one list of row fields.
5. **§6.2:** "four parts, plus a separate Other parameters panel."
6. **§6.5:** define session scope, average latency and cache hit. Either add a 1-hour cache-write price or state the limitation.
7. **§6.6:** define the reply link format. State any storage limit explicitly, or forbid one.
8. **§6.7:** define "previous request" for retries and failed attempts. Ignore `cache_control` when comparing.
9. **§6.10.1:** define the key for open/closed state.
10. **§7:** "A secret of 16 characters or fewer shows only `...`."
11. **AC2:** rename "Response tab" to "Reply tab".
12. **§9:** add live acceptance checks, for example a real tool turn, a repeated prompt with a real cache hit, and a long session through compaction.
13. **Scope:** each build took about 25 minutes. Consider limiting test-writing or deferring §10 M5 to leave time for more rounds.

## Still unverified

- Any build on live traffic.
- Real cache hit behavior, and long sessions with compaction.
- Codex, unless tested against the real service.
- Prices in `pricing.json`, which come from reference data cached on 2026-10-06.
- A's single 86-second stall on its first test run. It did not recur and was never explained.
