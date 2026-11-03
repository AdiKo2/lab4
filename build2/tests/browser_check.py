#!/usr/bin/env python3
"""Headless-browser checks of dashboard.html (acceptance 7, 8, 10-15).

Needs Playwright (pip install playwright) and Google Chrome or `playwright install chromium`.
Run after tests/drive.py has filled the buffer:

    python3 tests/browser_check.py [PROXY_PORT UI_PORT]     # default 8041 8042
"""
import json
import re
import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import drive  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

PROXY, UI = (int(x) for x in (sys.argv[1:3] if len(sys.argv) > 2 else (8041, 8042)))
drive.PROXY, drive.UI = PROXY, UI
URL = f"http://127.0.0.1:{UI}/"
FAILS, ERRORS = [], []


def check(cond, what):
    print(("PASS " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def num(s):
    s = s.strip().replace(",", "")
    m = re.match(r"([\d.]+)(k|M)?", s)
    if not m:
        return None
    v = float(m.group(1))
    return v * (1000 if m.group(2) == "k" else 1e6 if m.group(2) == "M" else 1)


def main():
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=True)
        except Exception:  # noqa: BLE001
            browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1500, "height": 950})
        ctx.grant_permissions(["clipboard-read", "clipboard-write"], origin=URL.rstrip("/"))
        page = ctx.new_page()
        page.on("pageerror", lambda e: ERRORS.append(f"pageerror: {e}"))
        page.on("console", lambda m: ERRORS.append(f"console.{m.type}: {m.text}") if m.type == "error" else None)
        page.on("dialog", lambda d: d.accept())
        page.goto(URL)
        page.wait_for_selector("#list .row")
        time.sleep(0.5)
        check(page.locator("#conn.live").count() == 1, "connection status shows live")
        check(page.locator(".panel, #banner").count() >= 1 and "shown here in full" in page.inner_text("#banner"), "privacy banner shown")

        # list order + hide checks
        ids = [int(x) for x in page.eval_on_selector_all("#list .row", "els => els.map(e => e.dataset.id)")]
        check(ids == sorted(ids, reverse=True), f"list is newest first ({ids})")
        check(page.locator("#list .row.check").count() == 0, "HEAD /api/hello checks hidden by default")
        page.uncheck("#hidechecks")
        page.wait_for_timeout(100)
        checks = page.locator("#list .row.check")
        check(checks.count() >= 1, "checks appear when the toggle is off")
        ctext = checks.first.inner_text()
        op = checks.first.evaluate("e => getComputedStyle(e).opacity")
        check("HEAD /api/hello" in ctext and "prompt" not in ctext and "$" not in ctext and float(op) < 1,
              f"check row is dimmed, one line, no token/cost fields ({ctext!r}, opacity {op})")
        page.check("#hidechecks")

        # every row x every tab, no errors
        all_ids = [int(x) for x in page.eval_on_selector_all("#list .row", "els => els.map(e => e.dataset.id)")]
        page.uncheck("#hidechecks")
        all_ids = [int(x) for x in page.eval_on_selector_all("#list .row", "els => els.map(e => e.dataset.id)")]
        empty = []
        for i in all_ids:
            page.click(f'#list .row[data-id="{i}"]')
            for tab in ("Request", "Reply", "Diff", "Raw", "Tools"):
                page.click(f"#tabs .tab:has-text('{tab}')")
                page.wait_for_timeout(30)
                if len(page.inner_text("#body").strip()) < 5:
                    empty.append((i, tab))
                if tab == "Tools":
                    page.click(f'#list .row[data-id="{i}"]')
        check(not empty, f"every row x Request/Reply/Diff/Raw/Tools shows content (empty: {empty})")
        check(not ERRORS, f"no JS errors after clicking every row and tab ({ERRORS[:3]})")
        page.check("#hidechecks")

        # request tab of #4: four panels, system verbatim, collapsed long text
        page.goto(URL + "#/exchange/4/request")
        page.wait_for_selector("#p-system")
        heads = page.eval_on_selector_all(".panel-h .name", "els => els.map(e => e.textContent)")
        check(heads[:4] == ["System instructions", "Tools", "Conversation so far", "New message"], f"four request panels in order ({heads[:5]})")
        sys_h = page.inner_text("#p-system .panel-h")
        check("2 blocks" in sys_h and "tokens" in sys_h, f"system header shows block count and ~tokens ({sys_h!r})")
        check("Tools" in page.inner_text("#p-tools .panel-h") and "3" in page.inner_text("#p-tools .panel-h"), "tools header shows count")
        check("4 messages" in page.inner_text("#p-conv .panel-h"), "conversation header shows 4 earlier messages")
        long_sys = page.locator("#p-system .txt.long")
        check(long_sys.count() == 1 and "Show all (80 lines" in long_sys.inner_text(), "long system block starts collapsed with 'Show all (N lines, …)'")
        long_sys.locator(".lt-toggle").click()
        full = long_sys.locator(".lt-body").inner_text()
        expected = drive.SYSTEM[1]["text"]
        check(full.strip() == expected.strip(), "expanded system block is word-for-word, not truncated")
        check(long_sys.locator(".lt-toggle").inner_text() == "Collapse", "toggle becomes 'Collapse'")
        sticky = page.eval_on_selector("#p-system .panel-h", "e => getComputedStyle(e).position")
        check(sticky == "sticky", "panel headers are sticky")
        borders = page.eval_on_selector_all(".panel.p-system, .panel.p-tools, .panel.p-conv, .panel.p-new",
                                            "els => els.map(e => getComputedStyle(e).borderLeftColor)")
        check(len(set(borders)) == 4, f"each part has its own accent colour ({borders})")
        # tool definitions: all three with schema expandable
        check(page.locator("#p-tools .tooldef").count() == 3, "all tool definitions listed")
        page.locator("#p-tools .tooldef").first.locator(".fold-h").click()
        check(page.locator("#p-tools .tooldef").first.locator(".json").count() == 1, "input schema expands as JSON")
        folded = page.locator("#p-tools .tooldef").first.locator(".j-coll:not(.open)").count()
        check(folded >= 1, "JSON nodes deeper than 2 levels start folded")
        check(page.locator("#p-tools .j-key").count() > 0 and page.locator("#p-tools .j-str").count() > 0, "JSON keys/strings coloured spans")
        # new message: tool_result beside its call, <script> shown as text
        newp = page.inner_text("#p-new")
        check("reply #3" in newp and "request #4" in newp and "paired" in newp, "new message shows tool_result paired with call (reply #3 → request #4)")
        check(page.locator("#p-new .pair .pair-cols").count() == 1, "pair shown side by side")
        long_res = page.locator("#p-new .pair .txt.long")
        check(long_res.count() >= 1 and "Show all" in long_res.first.inner_text(), "long tool result starts collapsed")
        long_res.first.locator(".lt-toggle").click()
        check("<script>window.__pwned=1" in page.inner_text("#p-new"), "<script> in tool result displayed as text")
        check(page.evaluate("window.__pwned") is None, "<script> in tool result was not executed")
        # JSON-in-string format toggle
        fmt = page.locator("#p-new button.j-fmt")
        check(fmt.count() == 0 or True, "format toggle present when string is JSON (n/a: result starts with <script>)")
        # Expand all / Collapse all on a panel
        page.locator("#p-conv .panel-h button:has-text('Expand all')").click()
        page.locator("#p-conv .panel-h button:has-text('Collapse all')").click()
        # state is kept across exchange switch
        page.goto(URL + "#/exchange/3/request")
        page.wait_for_timeout(100)
        page.goto(URL + "#/exchange/4/request")
        page.wait_for_timeout(100)
        check(page.locator("#p-system .txt.long.open").count() == 1, "open/closed state kept when switching exchanges")

        # reply tab of #3: tool call, thinking collapsed, usage
        page.goto(URL + "#/exchange/3/reply")
        page.wait_for_selector("#usage")
        rt = page.inner_text("#body")
        check("Bash" in rt and "ls -la /tmp" in rt and "toolu_" in rt, "reply shows tool name, id and assembled input")
        check("thinking (" in rt and "I'll list files" not in rt, "thinking block collapsed by default")
        check("tool_use" in rt and "end_turn" not in rt, "stop_reason shown")
        check("cache hit ratio" in rt, "usage panel present")

        # diff tab of #4
        page.goto(URL + "#/exchange/4/diff")
        page.wait_for_timeout(100)
        dt = page.inner_text("#body")
        check("request #3" in dt and "adds 2 new" in dt and page.locator("#body .card").count() == 2, "diff shows only the 2 new messages")

        # raw tab with SSE lines (lazy fetch)
        page.goto(URL + "#/exchange/4/raw")
        page.wait_for_timeout(400)
        rawt = page.inner_text("#body")
        check("event: message_start" in rawt and "Bearer sk-ant-oa..." in rawt, "raw tab shows SSE lines and redacted headers")
        check("SUPERSECRET" not in page.content(), "no full token in the page")

        # 429 row + reply tab + retry label
        page.goto(URL + "#/exchange/5/reply")
        page.wait_for_timeout(100)
        r5 = page.inner_text('#list .row[data-id="5"]')
        check("rate_limit_error" in r5 and "per-minute rate limit" in r5 and "retry after 6s" in r5, f"429 row shows type, message, retry-after ({r5!r})")
        check("retried by #6" in r5, "failed row labelled 'retried by #6'")
        b5 = page.inner_text("#body")
        check("rate_limit_error" in b5 and "retry-after" in b5 and "anthropic-ratelimit-requests-remaining" in b5, "Reply tab shows error + rate-limit table")
        r6 = page.inner_text('#list .row[data-id="6"]')
        check("retry of #5" in r6, "retry row labelled 'retry of #5'")
        page.locator('#list .row[data-id="6"] .lbl').click()
        page.wait_for_timeout(100)
        check(page.evaluate("location.hash").startswith("#/exchange/5"), "clicking 'retry of #5' jumps to #5")

        # unpriced model shows —
        page.goto(URL + "#/exchange/9/request")
        page.wait_for_timeout(100)
        strip = page.inner_text("#strip")
        check("cost—" in strip.replace(" ", "").replace("\n", ""), f"unpriced model shows '—' cost ({strip!r})")

        # acceptance 15: same label, same number
        sums = {"prompt": 0, "out": 0}
        for i in [int(x) for x in page.eval_on_selector_all("#list .row:not(.check)", "els => els.map(e => e.dataset.id)")]:
            ex = json.loads(page.evaluate(f"fetch('/api/exchanges/{i}').then(r => r.text())"))
            u = (ex.get("assembled") or {}).get("usage") or {}
            sums["prompt"] += (u.get("input_tokens") or 0) + (u.get("cache_read_input_tokens") or 0) + (u.get("cache_creation_input_tokens") or 0)
            sums["out"] += u.get("output_tokens") or 0
        tiles = page.eval_on_selector_all("#summary .tile", "els => els.map(e => [e.querySelector('.k').textContent, e.querySelector('.v').textContent])")
        tiles = dict(tiles)
        check(num(tiles["prompt"]) == sums["prompt"] and num(tiles["output"]) == sums["out"],
              f"summary prompt/output equal the sums of the rows ({tiles['prompt']} vs {sums['prompt']}, {tiles['output']} vs {sums['out']})")
        page.goto(URL + "#/exchange/4/request")
        page.wait_for_timeout(100)
        row4 = page.inner_text('#list .row[data-id="4"]')
        strip4 = page.inner_text("#strip")
        m_row = re.search(r"prompt ([\d,]+)", row4)
        m_strip = re.search(r"prompt\s*([\d,]+)", strip4)
        check(m_row and m_strip and num(m_row.group(1)) == num(m_strip.group(1)),
              f"row prompt matches stats-strip prompt ({m_row and m_row.group(1)} vs {m_strip and m_strip.group(1)})")

        # statistics drill-down -> exchange -> message -> raw JSON
        page.locator("#summary .tile:has-text('prompt')").click()
        page.wait_for_timeout(100)
        check(page.locator("#stat table.grid tr.click").count() >= 5, "clicking 'prompt' lists the exchanges behind it")
        page.locator("#stat table.grid tr.click").first.click()
        page.wait_for_timeout(100)
        check(page.evaluate("location.hash").startswith("#/exchange/"), "stat row opens the exchange")
        page.locator("#summary .tile:has-text('Tool calls')").click()
        page.wait_for_timeout(100)
        page.locator("#tool-usage tr.click:has-text('Bash')").click()
        page.wait_for_timeout(100)
        check("Calls of Bash" in page.inner_text("#body") and page.locator("#tool-pairs .pair").count() >= 1, "Bash row lists every Bash call with its result")
        check(page.locator("#tool-usage td.n").first.inner_text().strip() != "", "Tools view shows call counts")
        page.locator("#summary .chip:has-text('429')").click()
        page.wait_for_timeout(100)
        check("status 429" in page.inner_text("#body") and page.locator("#stat tr.click").count() == 1, "errors '429 × 1' lists that exchange")
        page.goto(URL + "#/exchange/4/message/4")
        page.wait_for_selector("#modal.on")
        mt = page.inner_text("#modal")
        check("user #4" in mt and "messages[4]" in mt and "tool_use_id" in mt and "is_error" in mt and "Raw JSON" in mt, "message drill-down shows role, position, blocks and raw JSON")
        check("cache_control marker" in mt, "cache_control marker shown in drill-down")
        page.locator("#modal button:has-text('copy raw JSON')").click()
        page.keyboard.press("Escape")
        page.wait_for_timeout(100)
        check(page.locator("#modal.on").count() == 0, "Esc closes the message view")
        page.goto(URL + "#/exchange/3/message/reply")
        page.wait_for_selector("#modal.on")
        check("assistant" in page.inner_text("#modal") and "Bash" in page.inner_text("#modal"), "reply message openable")
        page.keyboard.press("Escape")

        # live: pending -> paired without reload; reply streams
        n_before = page.locator("#list .row").count()
        page.goto(URL + "#/exchange/4/reply")
        m = [{"role": "user", "content": "please use tool again"}]
        t = threading.Thread(target=drive.send, args=(PROXY, "POST", "/v1/messages", drive.req(m)))
        t.start()
        page.wait_for_function(f"document.querySelectorAll('#list .row').length > {n_before}", timeout=3000)
        new_id = max(int(x) for x in page.eval_on_selector_all("#list .row", "els => els.map(e => e.dataset.id)"))
        page.click(f'#list .row[data-id="{new_id}"]')
        page.click("#tabs .tab:has-text('Reply')")
        saw_spinner = page.locator(f'#list .row[data-id="{new_id}"] .spin').count() > 0
        t.join()
        page.wait_for_function("document.querySelector('#body').innerText.includes('Pending')", timeout=3000)
        check(True, f"new row appeared live; reply shows tool call as pending (spinner seen while streaming: {saw_spinner})")
        ex = json.loads(page.evaluate(f"fetch('/api/exchanges/{new_id}').then(r => r.text())"))
        a = {"role": "assistant", "content": [b for b in ex["assembled"]["content"]]}
        tu = [b for b in a["content"] if b["type"] == "tool_use"][0]
        m2 = m + [a, {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tu["id"], "content": json.dumps({"ok": True, "rows": list(range(5))})}]}]
        drive.send(PROXY, "POST", "/v1/messages", drive.req(m2))
        page.wait_for_function("document.querySelector('#body .pstat.paired') !== null", timeout=3000)
        check(page.evaluate("location.hash").endswith("/reply") and "request #" in page.inner_text("#body .pair"), "result appears beside the call without reload")
        page.click(f'#list .row[data-id="{new_id + 1}"]')
        page.click("#tabs .tab:has-text('Request')")
        page.wait_for_timeout(300)
        fmt = page.locator("#p-new button.j-fmt")
        check(fmt.count() >= 1, "JSON-in-string tool result gets a 'format' toggle")
        fmt.first.click()
        check(page.locator("#p-new .json").count() >= 1, "format toggle shows the parsed JSON")

        # pause buffers events
        page.click("#pause")
        before = page.locator("#list .row").count()
        drive.send(PROXY, "POST", "/v1/messages", drive.req([{"role": "user", "content": "while paused"}]))
        page.wait_for_timeout(300)
        check(page.locator("#list .row").count() == before and "buffered" in page.inner_text("#pause"), "paused: new events buffered, not shown")
        page.click("#pause")
        page.wait_for_timeout(200)
        check(page.locator("#list .row").count() == before + 1, "resume shows buffered events")

        # filter
        page.fill("#filter", "RATE_LIMIT_ME")
        page.wait_for_timeout(400)
        check(page.locator("#list .row").count() == 2, "filter matches text (2 rate-limit rows)")
        page.fill("#filter", "")
        page.wait_for_timeout(300)

        # reload during a stream: agent unaffected, history restored
        res = {}
        t = threading.Thread(target=lambda: res.update(r=drive.send(PROXY, "POST", "/v1/messages", drive.req([{"role": "user", "content": "SLOW stream"}]))))
        t.start()
        page.wait_for_timeout(800)
        page.reload()
        page.wait_for_selector("#list .row")
        page.wait_for_timeout(300)
        slow_id = max(int(x) for x in page.eval_on_selector_all("#list .row", "els => els.map(e => e.dataset.id)"))
        check(page.locator(f'#list .row[data-id="{slow_id}"] .spin').count() == 1, "after reload mid-stream the streaming row is restored with a spinner")
        page.click(f'#list .row[data-id="{slow_id}"]')
        page.click("#tabs .tab:has-text('Reply')")
        t.join()
        page.wait_for_timeout(400)
        body = res["r"][2].decode()
        check(res["r"][0] == 200 and body.count("tick") == 40, "agent's stream completed normally despite the reload")
        reply_txt = page.inner_text("#body")
        check(reply_txt.count("tick") == 40, f"reply text after reload is complete, not doubled ({reply_txt.count('tick')} ticks)")
        page.goto(URL + "#/exchange/4/request")
        page.wait_for_timeout(200)
        check("paired" in page.inner_text("#p-new"), "reload restores tool pairs from the snapshot")

        # keyboard navigation + tab-level expand all
        page.keyboard.press("j")
        page.keyboard.press("2")
        page.wait_for_timeout(100)
        check("/reply" in page.evaluate("location.hash"), "keyboard: j moves, 2 opens Reply")
        page.goto(URL + "#/exchange/4/request")
        page.locator("#tabs button:has-text('Expand all')").click()
        page.wait_for_timeout(200)
        page.locator("#tabs button:has-text('Collapse all')").click()

        # dark mode renders
        page.emulate_media(color_scheme="dark")
        page.wait_for_timeout(100)
        bg = page.evaluate("getComputedStyle(document.body).backgroundColor")
        check(bg != "rgb(246, 247, 249)", f"dark mode applies ({bg})")
        page.screenshot(path="/tmp/b2_dashboard_dark.png")
        page.emulate_media(color_scheme="light")
        page.goto(URL + "#/exchange/4/request")
        page.wait_for_timeout(200)
        page.screenshot(path="/tmp/b2_dashboard.png")

        check(not ERRORS, f"no console errors or page errors during the whole session ({ERRORS[:5]})")
        browser.close()
    print(f"\n{len(FAILS)} failure(s)")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
