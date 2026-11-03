# Spec history

Written from `git log -p SPEC.md`.

## v1: Initial spec draft (`76c7a4e`, 10:09)

The prepared draft: a proxy on one port and a dashboard on another, connected by SSE. It covers the data model, the event stream, the UI, statistics, tool pairing, formatting rules and 11 acceptance criteria.

## v2: What build 1 showed on live traffic (`f224ac2`, 10:45)

Build 1 was generated from scratch from v1 (`2a02167`) and then run on live traffic. It led to these changes:

| What the build showed | Change to the spec |
|---|---|
| Clicking a row crashed the details area. Testing with `curl` alone could not catch this. | AC12: open every row and tab in a real browser, with no console errors. |
| Claude Code's `HEAD /api/hello` checks appeared as full rows with empty token fields. | §6.1: dimmed one-line rows, hidden by default. AC13. |
| Real requests failed with 429 and were retried, but the dashboard showed only a red code. | §6.1 and new §6.11: error type and message, `retry-after`, rate-limit headers, "retry of #N" links, and error counts by status. AC14. |
| "in" meant different sums in the row and in the top bar. | §6.5: one meaning per label (`input`, `prompt`, `output`). AC15. |
| After a reload, the snapshot had no tool pairs. | §5: the snapshot becomes `{exchanges, tools}`. |

## Parallel builds of v1 on the second laptop

Three more from-scratch builds of v1 took about 25 minutes each. They were tested against a fake upstream, not on live traffic. Their differences are in `laptop-2/findings.md`. They disagreed on how a failed exchange ends, how HTTP errors are recorded, the reply link format, the key for open/closed state, and the meaning of "previous request" in the diff.


## v3: What builds 1 and A-C showed (`13838de`)

Based on the open gaps in `notes.md` and on `laptop-2/findings.md`. No build of v3 exists yet, so these changes haven't been tested.

| What the builds showed | Change to the spec |
|---|---|
| Builds ended failed exchanges differently and treated HTTP errors differently. | §4 and §5: define `error`. Each exchange ends with exactly one ending. AC17. |
| "No result" was undefined (gap 9). | §6.4: a definition, including late results and compaction. AC18 and AC19. |
| Claude Code moves `cache_control`, and "previous request" had three meanings (gap 8). | §6.7: matching rules and one definition. AC16. |
| Cache-write price was ambiguous (gap 7). | §6.5: use the 5-minute and 1-hour split when present. |
| Statistics were undefined, and §6.1 and §6.5 listed different row fields. | §6.1 and §6.5: one row list, plus definitions. |
| Builds differed on reply links, open/closed state and short secrets. | §6.6, §6.10.1 and §7 pin each one. |
| §5 had no way to report tool status changes, eviction or clear. | §5: added three events. |
| The fake upstream missed the row-click crash, and builds took about 25 minutes (gap 10). | §9.1: how to test. M5 becomes optional. |
| The right side was empty with nothing selected (gap 6). | §6.1: select the newest exchange. |
| Interesting traffic was only in the notes. | New §12: findings in the traffic. |

## Why we stopped regenerating

Not yet recorded. Add what you decide: a build takes about 25 minutes, and the remaining gaps are small and local.
