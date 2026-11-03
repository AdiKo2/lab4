#!/usr/bin/env node
// Browser tests: real headless Chrome driven over CDP, with a fake upstream.
//   node tests/test_ui.mjs            (SHOT_DIR=<dir> keeps screenshots)
// Uses ports 8071 and 8072. Chrome and the fake upstream use OS-chosen ports.
import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = dirname(HERE);
const PROXY = 8071, UI = 8072;
const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const SHOT_DIR = process.env.SHOT_DIR;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let passed = 0;
const failures = [];
function check(name, ok, detail) {
  if (ok) { passed++; console.log("  ok   " + name); }
  else { failures.push(name); console.log("  FAIL " + name + (detail !== undefined ? "  -> " + JSON.stringify(detail) : "")); }
}

const procs = [];
function start(cmd, args, env) {
  const p = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PYTHONDONTWRITEBYTECODE: "1", ...env } });
  procs.push(p);
  return p;
}
function firstMatch(stream, re) {
  return new Promise((resolve) => {
    let buf = "";
    stream.on("data", (d) => { buf += d; const m = buf.match(re); if (m) resolve(m[1]); });
  });
}

class Page {
  constructor(ws) { this.ws = ws; this.id = 0; this.wait = new Map(); this.errors = []; this.pendingEvents = []; }
  static async open(browserPort) {
    const target = await (await fetch(`http://127.0.0.1:${browserPort}/json/new?about:blank`, { method: "PUT" })).json();
    const ws = new WebSocket(target.webSocketDebuggerUrl);
    await new Promise((r) => { ws.onopen = r; });
    const page = new Page(ws);
    page.targetId = target.id;
    ws.onmessage = (m) => {
      const d = JSON.parse(m.data);
      if (d.id && page.wait.has(d.id)) { page.wait.get(d.id)(d); page.wait.delete(d.id); }
      if (d.method === "Runtime.exceptionThrown") page.errors.push(d.params.exceptionDetails.exception?.description || d.params.exceptionDetails.text);
      if (d.method === "Log.entryAdded" && d.params.entry.level === "error") page.errors.push(d.params.entry.text + " " + (d.params.entry.url || ""));
    };
    await page.send("Runtime.enable");
    await page.send("Log.enable");
    await page.send("Page.enable");
    await page.send("Emulation.setFocusEmulationEnabled", { enabled: true });
    await page.send("Browser.grantPermissions", { permissions: ["clipboardReadWrite", "clipboardSanitizedWrite"], origin: `http://localhost:${UI}` }).catch(() => {});
    return page;
  }
  send(method, params = {}) {
    const id = ++this.id;
    return new Promise((resolve) => { this.wait.set(id, resolve); this.ws.send(JSON.stringify({ id, method, params })); });
  }
  async eval(expr) {
    const r = await this.send("Runtime.evaluate", { expression: expr, awaitPromise: true, returnByValue: true });
    if (r.result?.exceptionDetails) throw new Error("eval failed: " + expr.slice(0, 120) + " :: " + (r.result.exceptionDetails.exception?.description));
    return r.result.result.value;
  }
  async until(expr, ms = 4000) {
    const t0 = Date.now();
    while (Date.now() - t0 < ms) {
      if (await this.eval(`!!(${expr})`)) return true;
      await sleep(20);
    }
    return false;
  }
  async goto(url) {
    await this.send("Page.navigate", { url });
    await sleep(300);
  }
  async shot(name) {
    if (!SHOT_DIR) return;
    const r = await this.send("Page.captureScreenshot", { format: "png" });
    writeFileSync(join(SHOT_DIR, name + ".png"), Buffer.from(r.result.data, "base64"));
  }
}

async function send(body, path = "/v1/messages?beta=true") {
  const r = await fetch(`http://127.0.0.1:${PROXY}${path}`, { method: "POST", headers: { "Content-Type": "application/json", "x-api-key": "sk-ant-api03-FULLSECRETVALUEFORUITEST0123456789" }, body: JSON.stringify(body) });
  return { status: r.status, text: await r.text() };
}

const LONG_SYSTEM = Array.from({ length: 400 }, (_, i) => `Rule ${i}: <b>keep</b> the words exactly as written.`).join("\n") + "\nFINAL-SYSTEM-SENTINEL";
const TOOLS = [
  { name: "Bash", description: "Run a command.\n" + "More detail. ".repeat(120) + "TOOL-DESC-END", input_schema: { type: "object", properties: { command: { type: "string", description: "cmd", nested: { deep: { deeper: { deepest: 1 } } } } }, required: ["command"] } },
  { name: "Read", description: "Read a file.", input_schema: { type: "object", properties: { path: { type: "string" } } } },
];
const SYSTEM = [{ type: "text", text: "You are a test harness." }, { type: "text", text: LONG_SYSTEM, cache_control: { type: "ephemeral" } }];
const base = (messages, extra) => ({ model: "claude-opus-5-5", max_tokens: 1024, stream: true, temperature: 1, metadata: { user_id: "u1" }, system: SYSTEM, tools: TOOLS, messages, ...extra });
const toolUse = { type: "tool_use", id: "toolu_01abc", name: "Bash", input: { command: "ls -la" } };
const XSS_RESULT = "<script>window.__pwned=1</script><img src=x onerror=window.__pwned=2>\n" + Array.from({ length: 200 }, (_, i) => `file-${i}.txt`).join("\n") + "\nRESULT-END-SENTINEL";

async function main() {
  const fake = start("python3", [join(HERE, "fake_upstream.py")]);
  const fakePort = await firstMatch(fake.stdout, /PORT (\d+)/);
  const dash = start("python3", [join(ROOT, "dashboard.py"), "--port", String(PROXY), "--ui-port", String(UI), "--upstream", `http://127.0.0.1:${fakePort}`, "--quiet"]);
  await firstMatch(dash.stdout, /(ANTHROPIC_BASE_URL)/);

  const profile = mkdtempSync(join(tmpdir(), "dash-chrome-"));
  const chrome = start(CHROME, ["--headless=new", "--remote-debugging-port=0", `--user-data-dir=${profile}`, "--no-first-run", "--no-default-browser-check", "--disable-extensions", "--window-size=1500,1000", "about:blank"]);
  const browserPort = await firstMatch(chrome.stderr, /DevTools listening on ws:\/\/127\.0\.0\.1:(\d+)/);

  try {
    await run(browserPort);
  } finally {
    for (const p of procs.reverse()) p.kill("SIGTERM");
    await sleep(500);
    rmSync(profile, { recursive: true, force: true });
  }
}

async function run(browserPort) {
  const page = await Page.open(browserPort);
  await page.goto(`http://localhost:${UI}/`);

  console.log("Connection and empty state");
  check("status shows live", await page.until(`document.getElementById('connText').textContent === 'live'`));
  check("warning banner present", await page.eval(`/screenshots or exports/.test(document.getElementById('banner').textContent)`));

  console.log("AC2: new row within 200 ms, reply streams");
  await page.eval(`window.__rowAt = null; new MutationObserver(() => { if (!window.__rowAt && document.querySelector('#list .row')) window.__rowAt = Date.now(); }).observe(document.getElementById('list'), {childList:true});`);
  const t0 = Date.now();
  const slow = send(base([{ role: "user", content: "slow prompt" }]));
  check("row appears", await page.until(`document.querySelector('#list .row')`));
  const rowAt = await page.eval(`window.__rowAt`);
  check(`row within about 200 ms (${rowAt - t0} ms)`, rowAt - t0 < 200, rowAt - t0);
  check("spinner while streaming", await page.eval(`!!document.querySelector('#list .row .spinner')`));
  await page.eval(`location.hash = '#/exchange/1/reply'`);
  await page.until(`document.querySelector('#reply-blocks .tv-body')`);
  const len1 = await page.eval(`document.querySelector('#reply-blocks .tv-body').textContent.length`);
  await sleep(400);
  const len2 = await page.eval(`document.querySelector('#reply-blocks .tv-body').textContent.length`);
  const stillRunning = await page.eval(`!!document.querySelector('#list .row .spinner')`);
  check("reply text grows while streaming", len2 > len1 && stillRunning, { len1, len2, stillRunning });
  await page.shot("01-streaming");
  await slow;
  check("spinner gone at the end", await page.until(`!document.querySelector('#list .row .spinner')`));
  check("reply tab shows stop reason, model", await page.until(`/end_turn/.test(document.getElementById('scroll').textContent) && /claude-opus-5-5/.test(document.getElementById('scroll').textContent)`));

  console.log("AC4 and AC9: usage, cost, timing");
  const strip = () => page.eval(`Object.fromEntries([...document.querySelectorAll('#strip .stat')].map(s => [s.querySelector('span').textContent, s.querySelector('b').textContent]))`);
  await page.until(`document.querySelector('#strip .stat')`);
  let st = await strip();
  check("strip tokens match the stream", st.input === "12" && st.output === "340" && st["cache read"] === "18k" && st["cache write"] === "500", st);
  check("cache hit is high", parseInt(st["cache hit"]) > 90, st);
  check("cost is priced", /^\$/.test(st.cost), st);
  check("total, ttfb and ttft shown", /\d/.test(st.total) && /\d/.test(st.ttfb) && /\d/.test(st.ttft), st);
  check("row shows tokens and cache", await page.eval(`(t => /in 12 \\/ out 340/.test(t) && /cache 97%/.test(t) && /end_turn/.test(t) && /ttft/.test(t))(document.querySelector('#list .row').textContent)`));
  check("usage panel has four numbers and ratio", await page.eval(`(t => /18,000/.test(t) && /340/.test(t) && /500/.test(t) && /97%/.test(t))(document.querySelector('.usage').textContent)`));

  console.log("AC7 and AC11: request tab");
  await page.eval(`location.hash = '#/exchange/1/request'`);
  await page.until(`document.getElementById('p-sys')`);
  check("panels in fixed order", await page.eval(`JSON.stringify([...document.querySelectorAll('#scroll .panel')].slice(0,4).map(p=>p.id))`) === JSON.stringify(["p-sys", "p-tools", "p-conv", "p-new"]));
  const heads = await page.eval(`[...document.querySelectorAll('#scroll .panel > header')].map(h=>h.textContent)`);
  check("headers name and count", /System instructions· 2 blocks/.test(heads[0]) && /~\d/.test(heads[0]) && /Tools· 2/.test(heads[1]) && /Conversation so far· 0 messages/.test(heads[2]) && /New message/.test(heads[3]), heads);
  const sysText = await page.eval(`document.querySelector('[data-sys="1"] .tv-body').textContent`);
  check("system text is word for word", sysText === LONG_SYSTEM);
  check("system starts collapsed with a fade", await page.eval(`(e => e.classList.contains('clamped') && e.clientHeight < 170 && getComputedStyle(e).whiteSpace === 'pre-wrap')(document.querySelector('[data-sys="1"] .tv-body'))`));
  const toggle = await page.eval(`document.querySelector('[data-sys="1"] .tv-ctl button').textContent`);
  check("toggle reports size", /^Show all \(401 lines, 2\dk?\.?\d* ?k? ?chars\)$/.test(toggle) || /^Show all \(401 lines, [\d.]+k chars\)$/.test(toggle), toggle);
  await page.eval(`document.querySelector('[data-sys="1"] .tv-ctl button').click()`);
  check("toggle opens fully", await page.eval(`(e => !e.classList.contains('clamped') && e.scrollHeight === e.clientHeight)(document.querySelector('[data-sys="1"] .tv-body'))`));
  check("toggle says Collapse", await page.eval(`document.querySelector('[data-sys="1"] .tv-ctl button').textContent === 'Collapse'`));
  check("short text has no toggle", await page.eval(`!document.querySelector('[data-sys="0"] .tv-ctl button')`));
  check("markup in system stays text", await page.eval(`!document.querySelector('[data-sys="1"] .tv-body b')`));
  check("cache_control flagged on the block", await page.eval(`!!document.querySelector('[data-sys="1"] .badge.cache')`));
  check("tool description complete", await page.eval(`document.getElementById('p-tools').textContent.includes('TOOL-DESC-END')`));
  check("input schema behind a toggle", await page.eval(`(p => [...p.querySelectorAll('.col-h')].length === 2 && !p.querySelector('.json'))(document.getElementById('p-tools'))`));
  await page.eval(`document.querySelector('#p-tools .col-h').click()`);
  check("schema opens as JSON", await page.eval(`!!document.querySelector('#p-tools .json .jk')`));
  check("deep nodes start folded with a summary", await page.eval(`/\\{…\\} 2 keys/.test(document.querySelector('#p-tools .json').textContent) || /\\{…\\} \\d keys?/.test(document.querySelector('#p-tools .json').textContent)`));
  check("other parameters table", await page.eval(`(t => /max_tokens/.test(t) && /temperature/.test(t) && /metadata/.test(t) && /model/.test(t))(document.getElementById('p-other').textContent)`));
  check("new message highlighted and labelled", await page.eval(`/user #0/.test(document.getElementById('p-new').textContent) && /slow prompt/.test(document.getElementById('p-new').textContent)`));
  check("accent colors differ", await page.eval(`new Set(['p-sys','p-tools','p-conv','p-new'].map(i=>getComputedStyle(document.getElementById(i)).borderLeftColor)).size === 4`));
  check("panel headers are sticky", await page.eval(`getComputedStyle(document.querySelector('#p-sys > header')).position === 'sticky'`));
  await page.eval(`document.getElementById('scroll').scrollTop = 900`);
  await sleep(50);
  check("pinned header holds while scrolling", await page.eval(`(() => { const s = document.getElementById('scroll').getBoundingClientRect().top; const hdr = [...document.querySelectorAll('#scroll .panel > header')].find(h => { const t = h.getBoundingClientRect().top - s; return t >= 30 && t <= 40; }); return !!hdr; })()`));
  await page.eval(`[...document.querySelectorAll('.index-link')].find(a=>a.textContent==='New message').click()`);
  await sleep(50);
  check("index jumps to a part", await page.eval(`(() => { const s = document.getElementById('scroll').getBoundingClientRect().top; const t = document.getElementById('p-new').getBoundingClientRect().top - s; return t < 400; })()`));
  await page.shot("02-request");

  console.log("AC3, AC8, AC11: tools across exchanges");
  await send(base([{ role: "user", content: "tool please" }]));
  await page.eval(`location.hash = '#/exchange/2/reply'`);
  await page.until(`document.querySelector('.pair')`);
  check("call pending in the reply", await page.eval(`document.querySelector('#reply-blocks .chip').textContent === 'pending'`));
  check("blocks are in model order", await page.eval(`JSON.stringify([...document.querySelectorAll('#reply-blocks > .block > .bh > .badge')].map(b=>b.textContent))`) === JSON.stringify(["thinking", "text", "tool_use"]));
  check("thinking is collapsed", await page.eval(`(c => !c.classList.contains('open') && !c.textContent.includes('I should list'))(document.querySelector('.block.b-thinking .col'))`));
  check("tool name, id and assembled input", await page.eval(`(t => /Bash/.test(t) && /toolu_01abc/.test(t) && /ls -la/.test(t))(document.querySelector('#reply-blocks .pair').textContent)`));
  await page.eval(`window.__marker = 'no reload'; window.__slotEl = document.querySelector('[data-pair="toolu_01abc"]');`);
  await page.shot("03-pending");
  await sleep(40);
  const follow = [
    { role: "user", content: "tool please" },
    { role: "assistant", content: [{ type: "text", text: "Listing." }, toolUse] },
    { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_01abc", content: XSS_RESULT, cache_control: { type: "ephemeral" } }] },
  ];
  await send(base(follow));
  check("result appears in place, no reload", await page.until(`document.querySelector('#reply-blocks .chip').textContent === 'paired'`));
  check("title has no stray text", await page.eval(`!/false/.test(document.getElementById('dtitle').textContent)`));
  check("no reload and same slot element", await page.eval(`window.__marker === 'no reload' && document.querySelector('[data-pair="toolu_01abc"]') === window.__slotEl`));
  check("sides shown side by side", await page.eval(`(() => { const [a, b] = document.querySelectorAll('#reply-blocks .pair .side'); const x = a.getBoundingClientRect(), y = b.getBoundingClientRect(); return y.left >= x.right - 1 && Math.abs(x.top - y.top) < 2; })()`));
  check("labels name both exchanges", await page.eval(`/reply #2 → request #3/.test(document.querySelector('#reply-blocks .pair .ph').textContent)`));
  check("script text shown, not run", await page.eval(`document.querySelector('#reply-blocks .pair .result').textContent.includes('<script>window.__pwned=1</script>') && window.__pwned === undefined && !document.querySelector('#scroll script') && !document.querySelector('#scroll img')`));
  check("long tool result starts collapsed", await page.eval(`document.querySelector('#reply-blocks .pair .result .tv-body').classList.contains('clamped')`));
  await page.eval(`document.querySelector('#reply-blocks .pair .result .tv-ctl button').click()`);
  check("long tool result opens in full", await page.eval(`(e => !e.classList.contains('clamped') && e.textContent.includes('RESULT-END-SENTINEL') && e.textContent === ${JSON.stringify(XSS_RESULT)})(document.querySelector('#reply-blocks .pair .result .tv-body'))`));
  await page.shot("04-paired");

  await page.eval(`document.querySelector('#reply-blocks .pair .result').click()`);
  check("clicking the result side jumps to its request", await page.until(`location.hash.startsWith('#/exchange/3/request')`));
  await page.until(`document.querySelector('#p-new .pair')`);
  check("pair beside result in the new message", await page.eval(`(t => /ls -la/.test(t) && /RESULT-END-SENTINEL/.test(t))(document.querySelector('#p-new .pair').textContent)`));
  check("conversation has two earlier messages with role labels", await page.eval(`(t => /user #0/.test(t) && /assistant #1/.test(t))(document.getElementById('p-conv').textContent)`));
  check("user and assistant cards look different", await page.eval(`getComputedStyle(document.querySelector('#p-conv .msg.user')).backgroundColor !== getComputedStyle(document.querySelector('#p-conv .msg.assistant')).backgroundColor`));
  check("tool_use has distinct styling and badge", await page.eval(`(b => b && getComputedStyle(b).backgroundColor !== getComputedStyle(document.querySelector('#p-conv .msg.assistant')).backgroundColor)(document.querySelector('#p-conv .block.b-tool_use'))`));
  check("cache_control flagged on a message block", await page.eval(`!!document.querySelector('#p-new .badge.cache')`));
  await page.eval(`document.querySelector('#p-new .pair .call').click()`);
  check("clicking the call side jumps to its reply", await page.until(`location.hash.startsWith('#/exchange/2/reply')`));

  console.log("AC3: diff shows only what is new");
  await page.eval(`location.hash = '#/exchange/3/diff'`);
  await page.until(`document.getElementById('diff-note')`);
  check("diff compares with #2", await page.eval(`/request #2/.test(document.getElementById('diff-note').textContent)`));
  check("diff shows only the two new messages", await page.eval(`JSON.stringify([...document.querySelectorAll('#p-diff .msg > .mh')].map(m => m.childNodes[0].textContent))`) === JSON.stringify(["assistant #1", "user #2"]));
  check("diff contains tool_use and tool_result", await page.eval(`!!document.querySelector('#p-diff .b-tool_use') && !!document.querySelector('#p-diff .b-tool_result')`));
  await page.shot("05-diff");
  // A moved cache_control marker must not break conversation matching.
  const moved = JSON.parse(JSON.stringify(follow));
  delete moved[2].content[0].cache_control;
  moved[0] = { role: "user", content: [{ type: "text", text: "tool please" }] };
  const next = [...follow.map((m, i) => i === 2 ? { ...m, content: [{ ...m.content[0], cache_control: undefined }] } : m), { role: "assistant", content: "done" }, { role: "user", content: "second question" }];
  await send(base(next));
  await page.eval(`location.hash = '#/exchange/4/diff'`);
  await page.until(`location.hash === '#/exchange/4/diff' && document.getElementById('diff-note')`);
  check("moved cache_control still same conversation", await page.eval(`/request #3/.test(document.getElementById('diff-note').textContent) && /2 messages new/.test(document.getElementById('diff-note').textContent)`), await page.eval(`document.getElementById('diff-note').textContent`));
  await send(base([{ role: "user", content: "unrelated question" }], { system: "another system" }));
  await page.eval(`location.hash = '#/exchange/5/diff'`);
  await page.until(`location.hash === '#/exchange/5/diff' && document.getElementById('diff-note')`);
  check("different system is a new conversation", await page.eval(`/No earlier request/.test(document.getElementById('diff-note').textContent)`));

  console.log("6.8 raw tab");
  await page.eval(`location.hash = '#/exchange/3/raw'`);
  await page.until(`/Raw SSE/.test(document.getElementById('scroll').textContent)`);
  check("raw sections and copy buttons", await page.eval(`(() => { const t = document.getElementById('scroll'); return ['Request headers','Response headers','Raw request JSON','Raw SSE'].every(s => t.textContent.includes(s)) && t.querySelectorAll('.panel button.mini').length >= 4; })()`));
  check("token is redacted in the UI", await page.eval(`(t => t.includes('sk-ant-api03-FUL...') && !t.includes('FULLSECRETVALUE'))(document.getElementById('scroll').textContent)`));
  check("no full token in snapshot or page", await page.eval(`fetch('/api/export').then(r=>r.text()).then(t => !t.includes('FULLSECRETVALUE'))`));
  await sleep(150);
  const clip = await page.eval(`(async () => { const b = [...document.querySelectorAll('#scroll .panel')].find(p => p.textContent.startsWith('Raw request JSON')); const btn = [...b.querySelectorAll('button.mini')].find(x => x.textContent === 'Copy'); btn.click(); await new Promise(r => setTimeout(r, 200)); return { label: btn.textContent, text: await navigator.clipboard.readText().catch(e => 'ERR ' + e) }; })()`);
  check("copy gives exact body", clip.text === JSON.stringify(base(follow)), { label: clip.label, head: String(clip.text).slice(0, 80) });
  await page.shot("06-raw");

  console.log("6.10.2 JSON display");
  const j = await page.eval(`(() => { const p = [...document.querySelectorAll('#scroll .panel')].find(p => p.textContent.startsWith('Raw request JSON')); const q = (c) => getComputedStyle(p.querySelector(c)).color; return { key: q('.jk'), str: q('.js'), num: q('.jn'), bool: q('.jb'), font: getComputedStyle(p.querySelector('.json')).fontFamily, indent: getComputedStyle(p.querySelector('.jc')).marginLeft, lineCount: p.querySelectorAll('.jl').length }; })()`);
  check("keys, strings, numbers, booleans differ in color", new Set([j.key, j.str, j.num, j.bool]).size === 4, j);
  check("JSON is monospace and indented, not one line", /mono|Menlo/i.test(j.font) && parseFloat(j.indent) > 0 && j.lineCount > 3, j);
  check("folding a node shows a summary", await page.eval(`(() => { const p = [...document.querySelectorAll('#scroll .panel')].find(p => p.textContent.startsWith('Raw request JSON')); p.querySelector('.jt').click(); return /\\{…\\} \\d+ keys/.test(p.textContent); })()`));
  await page.eval(`(() => { const p = [...document.querySelectorAll('#scroll .panel')].find(p => p.textContent.startsWith('Raw request JSON')); p.querySelector('.jt').click(); })()`);

  console.log("AC9 and AC10: tools view and statistics click-through");
  await page.eval(`location.hash = '#/tools'`);
  await page.until(`document.getElementById('tool-usage')`);
  const row = await page.eval(`[...document.querySelectorAll('#tool-usage tbody tr')].map(r => [...r.children].map(c => c.textContent))`);
  check("tool usage row for Bash", row[0][0] === "Bash" && row[0][1] === "1" && row[0][2] === "0" && /chars/.test(row[0][3]) && /\d/.test(row[0][4]), row);
  await page.shot("07-tools");
  await page.eval(`document.querySelector('#tool-usage tbody tr').click()`);
  check("clicking Bash lists its calls", await page.until(`location.hash === '#/tools/Bash' && document.querySelector('#p-pairs .pair')`));
  await page.eval(`[...document.querySelectorAll('#p-pairs .pair .result a')].find(a => a.textContent === 'open message').click()`);
  check("stat leads to the message", await page.until(`!document.getElementById('modal').hidden && /user #2/.test(document.getElementById('mbox').textContent)`));
  check("drill-down shows full content", await page.eval(`document.getElementById('drill-blocks').textContent.includes('RESULT-END-SENTINEL') && /tool_use_id/.test(document.getElementById('drill-blocks').textContent) && /is_error/.test(document.getElementById('drill-blocks').textContent) && /cache_control/.test(document.getElementById('drill-blocks').textContent)`));
  check("drill-down shows raw JSON with a copy button", await page.eval(`!!document.querySelector('#drill-raw .jk') && !!document.querySelector('#drill-raw button.mini')`));
  check("drill-down text is open, not clamped", await page.eval(`!document.querySelector('#drill-blocks .tv-body.clamped')`));
  check("link is stable", await page.eval(`location.hash`) === "#/exchange/3/message/2");
  await page.shot("08-drill");
  await page.eval(`document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', bubbles:true}))`);
  check("escape closes drill-down", await page.until(`document.getElementById('modal').hidden`));

  await page.eval(`[...document.querySelectorAll('#summary .stat')].find(s => s.textContent.includes('cost')).click()`);
  check("session cost click lists exchanges", await page.until(`location.hash === '#/stats/cost' && document.querySelector('#stat-table tbody tr')`));
  await page.eval(`document.querySelector('#stat-table tbody tr a[href*="message"]').click()`);
  check("then opens the reply message", await page.until(`!document.getElementById('modal').hidden && /assistant #/.test(document.getElementById('mbox').textContent)`));
  check("reply drill-down has raw JSON and usage", await page.eval(`!!document.querySelector('#drill-raw .jk') && /18,000/.test(document.getElementById('mbox').textContent)`));
  await page.eval(`document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', bubbles:true}))`);
  await page.until(`document.getElementById('modal').hidden`);
  await page.eval(`location.hash = '#/exchange/1/request'`);
  await page.until(`document.querySelector('#strip .stat')`);
  await page.eval(`document.querySelector('#strip .stat').click()`);
  check("per-request stat opens its message", await page.until(`location.hash === '#/exchange/1/message/1' && !document.getElementById('modal').hidden`));
  await page.eval(`document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', bubbles:true}))`);
  await page.until(`document.getElementById('modal').hidden`);

  console.log("AC9: unpriced models and other replies");
  await send(base([{ role: "user", content: "unpriced" }]));
  await page.eval(`location.hash = '#/exchange/6/request'`);
  await page.until(`location.hash === '#/exchange/6/request' && /mystery-model-9/.test(document.getElementById('dtitle').textContent)`);
  st = await strip();
  check('unpriced model shows a dash', st.cost === "—", st);
  check("row shows a dash for cost", await page.until(`document.querySelector('#list a[data-id="6"]').textContent.includes('—')`));
  check("session summary still shows a number and notes unpriced", await page.until(`/1 unpriced/.test(document.getElementById('summary').textContent)`));
  await send({ model: "claude-opus-5-5", messages: [{ role: "user", content: "x" }] }, "/v1/messages/count_tokens");
  await page.eval(`location.hash = '#/exchange/7/reply'`);
  check("non-streamed reply body is shown", await page.until(`location.hash === '#/exchange/7/reply' && /1234/.test(document.getElementById('scroll').textContent)`));
  await send({ model: "claude-opus-5-5", messages: [{ role: "user", content: "http429" }] });
  await page.eval(`location.hash = '#/exchange/8/reply'`);
  check("HTTP error is visible", await page.until(`/slow down/.test(document.getElementById('scroll').textContent) && document.querySelector('#list a[data-id="8"] .st-bad')`));

  console.log("6.10.1 state is kept; expand and collapse all");
  await page.eval(`location.hash = '#/exchange/1/request'`);
  await page.until(`location.hash === '#/exchange/1/request' && document.querySelector('[data-sys="1"]')`);
  check("system stays open after switching exchanges", await page.eval(`!document.querySelector('[data-sys="1"] .tv-body').classList.contains('clamped')`));
  await page.eval(`location.hash = '#/exchange/3/request'`);
  await page.until(`location.hash === '#/exchange/3/request' && document.querySelector('[data-sys="1"]')`);
  check("exchange 3 system starts closed", await page.eval(`document.querySelector('[data-sys="1"] .tv-body').classList.contains('clamped')`));
  const panelBtn = (label) => `[...document.querySelectorAll('#p-tools > header button')].find(b => b.textContent === '${label}').click()`;
  await page.eval(panelBtn("Expand all"));
  check("panel expand all opens schema and text", await page.eval(`!document.querySelector('#p-tools .tv-body.clamped') && !document.querySelector('#p-tools .col:not(.open)')`));
  check("expand all stayed inside the panel", await page.eval(`document.querySelector('[data-sys="1"] .tv-body').classList.contains('clamped')`));
  await page.eval(panelBtn("Collapse all"));
  check("panel collapse all closes them", await page.eval(`!!document.querySelector('#p-tools .tv-body.clamped') && !document.querySelector('#p-tools .json')`));
  await page.eval(`[...document.querySelectorAll('.tabbar button')].find(b => b.textContent === 'Expand all').click()`);
  check("tab expand all opens everything", await page.eval(`!document.querySelector('#scroll .tv-body.clamped')`));
  await page.eval(`[...document.querySelectorAll('.tabbar button')].find(b => b.textContent === 'Collapse all').click()`);
  check("tab collapse all closes everything", await page.eval(`!!document.querySelector('#scroll .tv-body.clamped')`));
  const newText = await page.eval(`(async () => { const b = document.querySelector('[data-sys="1"] .tv-ctl button'); b.click(); return !document.querySelector('[data-sys="1"] .tv-body').classList.contains('clamped'); })()`);
  check("reopen for live test", newText);
  const liveDuring = send(base([{ role: "user", content: "slow during state test" }]));
  await page.until(`document.querySelector('#list a[data-id="9"]')`);
  await sleep(300);
  check("open state unaffected while events stream", await page.eval(`!document.querySelector('[data-sys="1"] .tv-body').classList.contains('clamped')`));
  await liveDuring;

  console.log("Long live reply");
  const longLive = send(base([{ role: "user", content: "longreply" }]));
  await page.until(`document.querySelector('#list a[data-id="10"]')`);
  await page.eval(`location.hash = '#/exchange/10/reply'`);
  await page.until(`location.hash === '#/exchange/10/reply' && document.querySelector('#reply-blocks .tv-body')`);
  await sleep(350);
  check("a long reply stays visible while it streams", await page.eval(`!document.querySelector('#reply-blocks .tv-body').classList.contains('clamped')`));
  await longLive;
  check("and has a Collapse toggle afterwards", await page.until(`[...document.querySelectorAll('#reply-blocks .tv-ctl button')].some(b => b.textContent === 'Collapse') && !document.querySelector('#list .row .spinner')`));
  await page.eval(`location.hash = '#/exchange/10/message/1'`);
  await page.until(`!document.getElementById('modal').hidden`);
  await page.eval(`document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', bubbles:true}))`);
  await page.until(`document.getElementById('modal').hidden`);

  console.log("Filter and pause");
  await page.eval(`(f => { f.value = 'mystery-model'; f.dispatchEvent(new Event('input')); })(document.getElementById('filter'))`);
  check("filter by model", await page.until(`document.querySelectorAll('#list .row').length === 1 && document.querySelector('#list a[data-id="6"]')`));
  await page.eval(`(f => { f.value = 'count_tokens'; f.dispatchEvent(new Event('input')); })(document.getElementById('filter'))`);
  check("filter by path", await page.until(`document.querySelectorAll('#list .row').length === 1 && document.querySelector('#list a[data-id="7"]')`));
  await page.eval(`(f => { f.value = 'unrelated question'; f.dispatchEvent(new Event('input')); })(document.getElementById('filter'))`);
  check("filter by text", await page.until(`document.querySelectorAll('#list .row').length === 1 && document.querySelector('#list a[data-id="5"]')`));
  await page.eval(`(f => { f.value = ''; f.dispatchEvent(new Event('input')); })(document.getElementById('filter'))`);
  await page.until(`document.querySelectorAll('#list .row').length === 10`);
  const ids = await page.eval(`[...document.querySelectorAll('#list .row')].map(r => Number(r.dataset.id))`);
  check("list is newest first", JSON.stringify(ids) === JSON.stringify([...ids].sort((a, b) => b - a)), ids);

  await page.eval(`document.getElementById('pause').click()`);
  await send(base([{ role: "user", content: "while paused" }]));
  await sleep(200);
  check("paused: new exchange is not shown", await page.eval(`!document.querySelector('#list a[data-id="11"]')`));
  check("paused: count of held events", await page.eval(`/Resume \\(\\d+ held\\)/.test(document.getElementById('pause').textContent)`));
  await page.eval(`document.getElementById('pause').click()`);
  check("resume shows it", await page.until(`document.querySelector('#list a[data-id="11"]') && !document.querySelector('#list a[data-id="11"] .spinner')`));

  console.log("Keyboard, theme, layout");
  await page.eval(`location.hash = '#/exchange/11/request'`);
  await page.until(`location.hash === '#/exchange/11/request'`);
  await page.eval(`document.dispatchEvent(new KeyboardEvent('keydown', {key:'j', bubbles:true}))`);
  check("j selects the next row", await page.until(`location.hash === '#/exchange/10/request'`));
  await page.eval(`document.dispatchEvent(new KeyboardEvent('keydown', {key:'2', bubbles:true}))`);
  check("2 opens the reply tab", await page.until(`location.hash === '#/exchange/10/reply'`));
  await page.eval(`location.hash = '#/exchange/3/request'`);
  await page.until(`document.getElementById('p-sys') && location.hash === '#/exchange/3/request'`);
  check("no sideways scroll in the detail pane", await page.eval(`(s => s.scrollWidth <= s.clientWidth + 1)(document.getElementById('scroll'))`));
  await page.eval(`document.getElementById('theme').click(); document.getElementById('theme').click();`);
  check("dark theme applies", await page.until(`document.documentElement.getAttribute('data-theme') === 'dark' && getComputedStyle(document.body).backgroundColor === 'rgb(20, 23, 27)'`));
  await sleep(100);
  await page.shot("09-dark");
  await page.eval(`document.getElementById('theme').click()`);

  console.log("AC5: reload and close during a stream");
  const during = send(base([{ role: "user", content: "slow reload test" }]));
  await page.until(`document.querySelector('#list a[data-id="12"]')`);
  await sleep(200);
  await page.goto(`http://localhost:${UI}/#/exchange/12/reply`);
  check("reload restores history", await page.until(`document.querySelectorAll('#list .row').length === 12`));
  check("in-flight reply keeps streaming after reload", await page.until(`/word/.test(document.getElementById('scroll').textContent)`));
  check("tool pairs restored after reload", await page.until(`/tool calls/.test(document.getElementById('summary').textContent) && [...document.querySelectorAll('#summary .stat')].find(s => s.textContent.includes('tool calls')).querySelector('b').textContent === '1'`));
  const res = await during;
  check("agent got its whole reply through the reload", res.status === 200 && res.text.includes("event: message_stop"));
  check("exchange finishes in the browser", await page.until(`!document.querySelector('#list a[data-id="12"] .spinner')`));
  check("deep link to a message works from a fresh load", await (async () => { await page.goto(`http://localhost:${UI}/#/exchange/3/message/1`); return page.until(`!document.getElementById('modal').hidden && /assistant #1/.test(document.getElementById('mbox').textContent)`); })());
  await page.eval(`document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', bubbles:true}))`);

  const second = await Page.open(browserPort);
  await second.goto(`http://localhost:${UI}/`);
  await second.until(`document.querySelectorAll('#list .row').length === 12`);
  const during2 = send(base([{ role: "user", content: "slow close test" }]));
  await sleep(250);
  await fetch(`http://127.0.0.1:${browserPort}/json/close/${second.targetId}`);
  const res2 = await during2;
  check("closing a tab mid-stream leaves the agent unaffected", res2.status === 200 && res2.text.includes("event: message_stop"));
  check("the other tab sees it finish", await page.until(`document.querySelector('#list a[data-id="13"]') && !document.querySelector('#list a[data-id="13"] .spinner')`));

  console.log("Clear");
  await page.eval(`document.getElementById('clear').click()`);
  check("clear empties the list and index", await page.until(`document.querySelectorAll('#list .row').length === 0`));
  check("export is empty after clear", (await (await fetch(`http://localhost:${UI}/api/export`)).json()).length === 0);
  await send(base([{ role: "user", content: "after clear" }]));
  check("new traffic after clear appears and ids keep rising", await page.until(`document.querySelector('#list a[data-id="14"]')`));
  check("the script in the tool result never ran", await page.eval(`window.__pwned === undefined`));

  console.log("Browser errors");
  const errs = page.errors.filter((e) => !/favicon/.test(e));
  check("no page errors or CSP violations", errs.length === 0, errs.slice(0, 5));
}

main().then(() => {
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length) console.log("Failed:\n  " + failures.join("\n  "));
  process.exit(failures.length ? 1 : 0);
}).catch((e) => { console.error(e); for (const p of procs) p.kill("SIGTERM"); process.exit(2); });
