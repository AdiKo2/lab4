#!/usr/bin/env node
// Drives dashboard.html in headless Chrome through the DevTools protocol, with
// the proxy and fake upstream. Uses its own temporary Chrome profile.
//   node tests/browser_test.mjs <scratch dir>
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const SCRATCH = process.argv[2] || mkdtempSync("/tmp/dash-");
mkdirSync(SCRATCH, {recursive: true});
const PROXY = 8061, UI = 8062, FAKE = 8063;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const procs = [];
let passes = 0; const fails = [];
function check(name, ok, detail = "") {
  if (ok) { passes++; console.log("  ok   " + name); }
  else { fails.push(name); console.log(`  FAIL ${name} ${typeof detail === "string" ? detail : JSON.stringify(detail)}`); }
}
function start(cmd, args, opts = {}) {
  const p = spawn(cmd, args, {stdio: "ignore", ...opts});
  procs.push(p);
  return p;
}
async function waitHttp(url) {
  for (let i = 0; i < 100; i++) { try { await fetch(url); return; } catch { await sleep(100); } }
  throw new Error("no answer from " + url);
}
const LONG_SYSTEM = Array.from({length: 342}, (_, i) => `System rule ${i + 1}: follow the instructions carefully, <b>exactly</b>.`).join("\n") + "\nFINAL-SYSTEM-LINE";
const TOOLS = [
  {name: "Bash", description: "Run a shell command. ".repeat(80) + "END-OF-BASH-DESCRIPTION", input_schema: {type: "object", properties: {command: {type: "string", description: "cmd"}}, required: ["command"]}},
  {name: "Read", description: "Read a file.", input_schema: {type: "object", properties: {path: {type: "string"}}}},
  {type: "web_search_20260209", name: "web_search"},
];
const LONG_RESULT = "<script>window.__pwned = true</script><img src=x onerror=\"window.__pwned=true\">\n" + Array.from({length: 400}, (_, i) => `result line ${i}`).join("\n") + "\nFINAL-RESULT-LINE";
const request = (messages, model = "claude-opus-5-5") => fetch(`http://127.0.0.1:${PROXY}/v1/messages?beta=true`, {method: "POST",
  headers: {"content-type": "application/json", "x-api-key": "sk-ant-api03-BROWSER-TEST-FULL-SECRET-0123456789", "anthropic-version": "2023-06-01"},
  body: JSON.stringify({model, max_tokens: 1000, temperature: 0.5, stream: true, metadata: {user_id: "u"}, thinking: {type: "adaptive"},
    system: [{type: "text", text: "You are Claude Code."}, {type: "text", text: LONG_SYSTEM, cache_control: {type: "ephemeral"}}], tools: TOOLS, messages})}).then(r => r.text());

async function main() {
  start("python3", [join(ROOT, "tests/fake_upstream.py"), "--port", String(FAKE)]);
  await waitHttp(`http://127.0.0.1:${FAKE}/x`);
  const startDashboard = () => start("python3", ["-B", join(ROOT, "dashboard.py"), "--port", String(PROXY), "--ui-port", String(UI), "--quiet", "--upstream", `http://127.0.0.1:${FAKE}`]);
  let server = startDashboard();
  await waitHttp(`http://127.0.0.1:${UI}/`);

  const profile = mkdtempSync(join(SCRATCH, "chrome-"));
  start("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", ["--headless=new", "--no-first-run", "--no-default-browser-check",
    "--disable-gpu", "--remote-debugging-port=0", `--user-data-dir=${profile}`, "--window-size=1500,1000", "about:blank"]);
  let port;
  for (let i = 0; i < 100 && !port; i++) {
    try { port = readFileSync(join(profile, "DevToolsActivePort"), "utf8").split("\n")[0]; } catch { await sleep(100); }
  }
  const target = (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()).find(t => t.type === "page");
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise(r => { ws.onopen = r; });
  let nextId = 1; const waiting = new Map(); const errors = [];
  ws.onmessage = m => {
    const msg = JSON.parse(m.data);
    if (msg.id) { waiting.get(msg.id)(msg); waiting.delete(msg.id); }
    if (msg.method === "Runtime.exceptionThrown") errors.push(msg.params.exceptionDetails.exception?.description || msg.params.exceptionDetails.text);
    if (msg.method === "Runtime.consoleAPICalled" && msg.params.type === "error") errors.push(JSON.stringify(msg.params.args.map(a => a.value)));
    if (msg.method === "Log.entryAdded" && msg.params.entry.level === "error" && !/favicon/.test(msg.params.entry.url || "")) errors.push(msg.params.entry.text + " " + (msg.params.entry.url || ""));
  };
  const send = (method, params = {}) => new Promise(r => { const id = nextId++; waiting.set(id, r); ws.send(JSON.stringify({id, method, params})); });
  const ev = async expr => {
    const r = await send("Runtime.evaluate", {expression: expr, returnByValue: true, awaitPromise: true});
    if (r.result.exceptionDetails) throw new Error(expr + " => " + JSON.stringify(r.result.exceptionDetails));
    return r.result.result.value;
  };
  const go = async hash => { await ev(`location.hash=${JSON.stringify(hash)}`); await sleep(120); };
  const shot = async name => {
    const r = await send("Page.captureScreenshot", {format: "png"});
    writeFileSync(join(SCRATCH, name + ".png"), Buffer.from(r.result.data, "base64"));
  };
  await send("Runtime.enable"); await send("Log.enable"); await send("Page.enable");
  await send("Page.navigate", {url: `http://localhost:${UI}/`});
  await sleep(600);

  console.log("page");
  check("connection shows live", await ev(`document.getElementById('conn').textContent`) === "live");
  check("warning banner is shown", /shown in full/.test(await ev(`document.getElementById('banner').textContent`)));

  console.log("§9.2 new row and live reply");
  const t0 = Date.now();
  const slow = request([{role: "user", content: "slow first prompt"}]);
  let rowAt = null;
  for (let i = 0; i < 100 && rowAt == null; i++) { if (await ev(`document.querySelectorAll('.row').length`) > 0) rowAt = Date.now() - t0; else await sleep(5); }
  check(`row appears within about 200 ms (${rowAt} ms)`, rowAt != null && rowAt < 300, String(rowAt));
  check("row shows a spinner while streaming", await ev(`!!document.querySelector('.row .spinner')`));
  await go("#/exchange/1/reply");
  await sleep(1500);
  const part1 = await ev(`document.querySelector('#p-reply .block.text .lt-body')?.textContent || ''`);
  await sleep(800);
  const part2 = await ev(`document.querySelector('#p-reply .block.text .lt-body')?.textContent || ''`);
  check("reply text grows while streaming", part1.length > 0 && part2.length > part1.length, [part1, part2]);
  await shot("reply-streaming");
  await slow; await sleep(200);
  check("spinner gone when done", !(await ev(`!!document.querySelector('.row .spinner')`)));
  const replyText = await ev(`document.querySelector('#p-reply').textContent`);
  check("reply shows full text, stop reason, model", replyText.includes("Héllo wörld ✓ from the fake upstream.") && replyText.includes("end_turn") && replyText.includes("claude-opus-5-5"));
  check("usage matches events: 12, 340, 18,000, 500, 97%", ["12", "340", "18,000", "500", "97%"].every(v => replyText.includes(v)), replyText);

  console.log("§9.7 §9.11 request tab");
  await go("#/exchange/1/request");
  const ids = await ev(`[...document.querySelectorAll('#content .panel')].map(p => p.id)`);
  check("four parts in the fixed order", ids.slice(0, 4).join() === "p-system,p-tools,p-conv,p-new", ids);
  const headers = await ev(`[...document.querySelectorAll('.panel > header .name')].map(n => n.parentElement.querySelector('.name').textContent + ' ' + (n.parentElement.querySelector('.muted')?.textContent||''))`);
  check("panel headers have counts", /System instructions · 2 blocks/.test(headers[0]) && /tokens/.test(headers[0]) && /Tools · 3/.test(headers[1]) && /Conversation so far · 0 messages/.test(headers[2]) && /New message/.test(headers[3]), headers);
  const colors = await ev(`['p-system','p-tools','p-conv','p-new'].map(i => getComputedStyle(document.getElementById(i)).borderLeftColor)`);
  check("each part has its own accent color", new Set(colors).size === 4, colors);
  check("panel headers are pinned", await ev(`getComputedStyle(document.querySelector('#p-system > header')).position`) === "sticky");
  const sys = `[...document.querySelectorAll('#p-system .lt')].pop()`;
  check("long system prompt starts collapsed", await ev(`${sys}.classList.contains('closed')`));
  check("toggle shows size", /^Show all \(343 lines, \d+\.\dk chars\)$/.test(await ev(`${sys}.querySelector('.lt-toggle').textContent`)), await ev(`${sys}.querySelector('.lt-toggle').textContent`));
  check("collapsed height is about 6 lines", await ev(`${sys}.querySelector('.lt-body').getBoundingClientRect().height`) < 140);
  await ev(`${sys}.querySelector('.lt-toggle').click()`);
  check("open: word for word, nothing cut off", await ev(`${sys}.querySelector('.lt-body').textContent`) === LONG_SYSTEM);
  check("toggle now says Collapse", await ev(`${sys}.querySelector('.lt-toggle').textContent`) === "Collapse");
  check("no sideways scrolling, pre-wrap", await ev(`getComputedStyle(${sys}.querySelector('.lt-body')).whiteSpace`) === "pre-wrap" && await ev(`document.getElementById('content').scrollWidth <= document.getElementById('content').clientWidth`));
  check("markup in the system prompt is text", await ev(`${sys}.querySelector('b') === null`));
  await go("#/exchange/1/reply"); await go("#/exchange/1/request");
  check("open state survives switching tabs", !(await ev(`${sys}.classList.contains('closed')`)));
  check("short system block has no toggle", await ev(`document.querySelector('#p-system .block').querySelector('.lt-toggle') === null`));
  const toolText = await ev(`document.querySelector('#p-tools').textContent`);
  check("every tool, full description", toolText.includes("Bash") && toolText.includes("Read") && toolText.includes("web_search"));
  await ev(`document.querySelector('#p-tools button[class~="lt-toggle"]').click()`);
  check("long tool description opens in full", (await ev(`document.querySelector('#p-tools').textContent`)).includes("END-OF-BASH-DESCRIPTION"));
  await ev(`[...document.querySelectorAll('#p-tools button')].find(b => /Input schema/.test(b.textContent)).click()`);
  check("input schema expandable", (await ev(`document.querySelector('#p-tools').textContent`)).includes("required"));
  check("other parameters table", ["temperature", "max_tokens", "metadata", "thinking", "model"].every(k => ids.includes("p-params")) && /temperature/.test(await ev(`document.querySelector('#p-params').textContent`)));

  console.log("§9.3 §9.8 tool turn");
  await ev(`0`);
  const u1 = {role: "user", content: "tool t1"};
  await request([u1]); await sleep(150);
  await go("#/exchange/2/reply");
  check("call shows as pending", await ev(`document.querySelector('#p-reply .pair .badge.pending')?.textContent`) === "pending");
  const order = await ev(`[...document.querySelectorAll('#p-reply > .body > div > .block')].map(b => b.querySelector('.badge').textContent)`);
  check("blocks in order: thinking, text, tool_use", order.join() === "thinking,text,tool_use", order);
  check("thinking collapsed by default", !(await ev(`document.querySelector('#p-reply .block.thinking').textContent`)).includes("I should run"));
  await ev(`document.querySelector('#p-reply .block.thinking .jf').click()`);
  check("and opens", (await ev(`document.querySelector('#p-reply .block.thinking').textContent`)).includes("I should run"));
  await ev(`document.querySelector('#p-reply .block.thinking .jf').click()`);
  const toolBlock = await ev(`document.querySelector('#p-reply .block.tool_use').textContent`);
  check("tool name, id, assembled input", toolBlock.includes("Bash") && toolBlock.includes("toolu_t1") && toolBlock.includes('"ls -la"'), toolBlock);
  await ev(`window.__marker = 'no reload'`);
  const assistant = await ev(`0`) ;
  const content = await (await fetch(`http://127.0.0.1:${UI}/api/exchanges/2`)).json();
  const u2 = {role: "user", content: [{type: "tool_result", tool_use_id: "toolu_t1", content: LONG_RESULT, is_error: true, cache_control: {type: "ephemeral"}}]};
  const history = [u1, {role: "assistant", content: content.assembled.content}, u2];
  await request(history); await sleep(250);
  check("no page reload", await ev(`window.__marker`) === "no reload");
  check("result appears beside the call, in place", await ev(`document.querySelector('#p-reply .pair .badge.paired')?.textContent`) === "paired");
  const sides = await ev(`document.querySelector('#p-reply .pair').children.length`);
  const rects = await ev(`(() => { const [a, b] = document.querySelector('#p-reply .pair').children; return [a.getBoundingClientRect().right <= b.getBoundingClientRect().left + 1, a.getBoundingClientRect().top === b.getBoundingClientRect().top]; })()`);
  check("side by side on one row", sides === 2 && rects[0] && rects[1], rects);
  check("is_error red marker, size", await ev(`!!document.querySelector('#p-reply .pair .side.iserr .badge.error')`) && /k chars/.test(await ev(`document.querySelector('#p-reply .pair .side.iserr').textContent`)));
  check("labels name both exchanges", /reply #2.*request #3/.test(await ev(`document.querySelector('#p-reply .pair').textContent`)));
  await shot("reply-paired");
  check("<script> shown as text and not run", (await ev(`document.querySelector('#p-reply .pair .side.iserr').textContent`)).includes("<script>window.__pwned") && await ev(`window.__pwned === undefined`) && await ev(`document.querySelector('#content script, #content img') === null`));
  check("long result starts collapsed", await ev(`document.querySelector('#p-reply .pair .side.iserr .lt').classList.contains('closed')`));
  await ev(`document.querySelector('#p-reply .pair .side.iserr .lt-toggle').click()`);
  check("and opens in full", (await ev(`document.querySelector('#p-reply .pair .side.iserr .lt-body').textContent`)).includes("FINAL-RESULT-LINE"));
  await ev(`[...document.querySelectorAll('#p-reply .pair .jump')].find(b => b.textContent === 'request #3').click()`); await sleep(120);
  check("clicking a side jumps to that exchange", await ev(`location.hash`) === "#/exchange/3/request");
  check("tool_result styled, flagged cache_control in the new message", await ev(`!!document.querySelector('#p-new .block.tool_result .badge.cache')`));
  check("pair beside the tool_result in the new message", await ev(`!!document.querySelector('#p-new .pair .badge.paired')`));
  check("conversation so far is a timeline of cards", await ev(`[...document.querySelectorAll('#p-conv .card .chead')].map(c => c.textContent.split('Open')[0].trim()).join()`) === "user #0,assistant #1");
  check("new message card labelled", (await ev(`document.querySelector('#p-new .chead').textContent`)).includes("user #2"));
  await shot("request-tab");

  await go("#/exchange/3/diff");
  const diff = await ev(`document.querySelector('#p-diff').textContent`);
  const cards = await ev(`[...document.querySelectorAll('#p-diff .card .chead')].map(c => c.textContent.split('Open')[0].replace('cache_control','').trim()).join()`);
  check("diff shows only the new messages", cards === "assistant #1,user #2" && /1 message resent unchanged/.test(diff) && /2 new/.test(diff) && /request #2/.test(diff), [cards, diff.slice(0, 200)]);
  await shot("diff");
  await go("#/exchange/1/diff");
  check("first request of a conversation: all new", /First request/.test(await ev(`document.querySelector('#p-diff').textContent`)));

  console.log("§9.9 §9.10 statistics to raw JSON");
  await go("#/tools");
  const usage = await ev(`document.querySelector('#p-usage tbody tr').textContent`);
  check("Tools view shows calls, errors, avg size, avg time", /Bash/.test(usage) && /1/.test(usage) && /k chars/.test(usage) && /(ms| s)/.test(usage), usage);
  await shot("tools");
  await ev(`document.querySelector('#p-usage tbody tr').click()`); await sleep(120);
  check("clicking Bash lists every Bash call", await ev(`location.hash`) === "#/tools/Bash" && await ev(`document.querySelectorAll('#p-pairs .pair').length`) === 1);
  await ev(`[...document.querySelectorAll('#p-pairs .pair .side')].at(-1).querySelector('.jump').click()`); await sleep(120);
  check("through to the message by its stable link", await ev(`location.hash`) === "#/exchange/3/message/2");
  const open = await ev(`document.querySelector('#p-msg').textContent`);
  check("opened message: role, position, block details", /user #2/.test(open) && /position 2 of 3/.test(open) && /tool_use_id: toolu_t1/.test(open) && /is_error: true/.test(open) && /cache_control/.test(open), open.slice(0, 200));
  check("opened message shows the full result", open.includes("FINAL-RESULT-LINE"));
  const rawPanel = `[...document.querySelectorAll('.panel')].find(p => /Raw JSON/.test(p.textContent.slice(0, 20)))`;
  check("raw JSON and a copy button", await ev(`!!${rawPanel}`) && await ev(`!!${rawPanel}.querySelector('.jcopy')`) && (await ev(`${rawPanel}.textContent`)).includes('"tool_use_id"'));
  await send("Browser.grantPermissions", {permissions: ["clipboardReadWrite", "clipboardSanitizedWrite"], origin: `http://localhost:${UI}`});
  await send("Page.bringToFront");
  await ev(`${rawPanel}.querySelector('.jcopy').click()`); await sleep(100);
  const copied = await ev(`navigator.clipboard.readText().catch(e => 'ERR ' + e)`);
  let same = false; try { same = !copied.includes("\n  ") && JSON.parse(copied).content[0].tool_use_id === "toolu_t1"; } catch {}
  check("copy gives exact unformatted JSON", same, copied.slice(0, 100));

  await go("#/exchange/2/message/1");
  check("the reply opens as a message", /assistant #1/.test(await ev(`document.querySelector('#p-msg').textContent`)) && /I should run a command/.test(await ev(`document.querySelector('#p-msg').textContent`)));
  await go("#/exchange/2/request");
  const strip = await ev(`document.getElementById('strip').textContent`);
  check("stats strip: tokens, cost, times", ["input12", "output340", "cache read18,000", "cache write500", "first byte", "first token", "tok/s", "$"].every(s => strip.replace(/\s/g, m => m).includes(s)), strip);
  const row = await ev(`document.querySelector('.row[data-id="2"]').textContent`);
  check("row: id, model, status, tokens, cache, stop reason, cost", ["#2", "claude-opus-5-5", "200", "12 in / 340 out", "cache 97%", "tool_use", "$"].every(s => row.includes(s)), row);
  await ev(`[...document.querySelectorAll('#strip .stat')].find(b => /cost/.test(b.textContent)).click()`); await sleep(120);
  check("stat in the strip shows its source", await ev(`location.hash`) === "#/stats/cost/2" && /cost_usd/.test(await ev(`document.querySelector('#p-stat').textContent`)));
  await ev(`document.querySelector('#summary .tile:nth-child(3)').click()`); await sleep(120);
  check("session cost lists the exchanges behind it", await ev(`location.hash`) === "#/stats/cost" && await ev(`document.querySelectorAll('#p-stat tbody tr').length`) === 3);
  await ev(`[...document.querySelectorAll('#p-stat tbody tr')][0].querySelector('.jump').click()`); await sleep(120);
  check("and through to a message", /#\/exchange\/3\/message\/2/.test(await ev(`location.hash`)));
  const tiles = await ev(`document.getElementById('summary').textContent`);
  check("session panel: requests, tokens, cost, wall, latency, cache", ["requests", "tokens", "estimated cost", "wall clock", "average latency", "cache hit"].every(s => tiles.includes(s)) && tiles.includes("97%"), tiles);

  await request([{role: "user", content: "hello"}], "unpriced-model"); await sleep(200);
  check('unpriced model shows "—"', (await ev(`document.querySelector('.row[data-id="4"] .l2').textContent`)).includes("—"), await ev(`document.querySelector('.row[data-id="4"] .l2').textContent`));
  const list = await ev(`[...document.querySelectorAll('.row')].map(r => Number(r.dataset.id))`);
  check("list is newest first", list.join() === "4,3,2,1", list);

  console.log("§6.10.2 JSON");
  await go("#/exchange/3/raw");
  await sleep(200);
  const jsonRaw = `[...document.querySelectorAll('.panel')].find(p => /Raw request JSON/.test(p.textContent.slice(0, 30)))`;
  const classes = await ev(`['k','s','n','b','u'].map(c => { const el = ${jsonRaw}.querySelector('.' + c); return el && getComputedStyle(el).color; })`);
  check("keys, strings, numbers, booleans each colored", classes[0] && classes[1] && classes[2] && classes[3] && new Set(classes.filter(Boolean)).size === classes.filter(Boolean).length, classes);
  const summary = await ev(`${jsonRaw}.textContent`);
  check("nodes deeper than 2 levels start folded with a summary", /\{…\} \d+ keys?|\[…\] \d+ items?/.test(summary));
  check("indented, not one line", await ev(`getComputedStyle(${jsonRaw}.querySelector('.jkids')).marginLeft`) !== "0px");
  check("JSON uses monospace", /mono/i.test(await ev(`getComputedStyle(${jsonRaw}.querySelector('.json')).fontFamily`)));
  await ev(`${jsonRaw}.querySelector('.jn > .jf').click()`);
  check("every node folds", /\{…\} \d+ keys/.test((await ev(`${jsonRaw}.querySelector('.json').textContent`)).slice(0, 40)));
  await ev(`${jsonRaw}.querySelector('.jn > .jf').click()`);
  await shot("raw-json");
  await ev(`document.querySelector('#tabs button:nth-last-child(2)').click()`); await sleep(100);
  check("Expand all opens every node", !/\{…\}|\[…\]/.test(await ev(`document.getElementById('content').textContent`)));
  await ev(`document.querySelector('#tabs button:nth-last-child(1)').click()`); await sleep(100);
  check("Collapse all folds them", /\{…\}/.test(await ev(`document.getElementById('content').textContent`)));
  const rawText = await ev(`document.getElementById('content').textContent`);
  check("raw has both header sets, request and SSE", ["Request headers", "Response headers", "Raw request JSON", "Raw SSE lines"].every(s => rawText.includes(s)));
  check("no full token in the UI", !(await ev(`document.documentElement.outerHTML`)).includes("BROWSER-TEST-FULL-SECRET"));

  // A tool result that is itself JSON gets a format toggle.
  const u3 = {role: "user", content: [{type: "tool_result", tool_use_id: "toolu_t2", content: '{"files": ["a", "b"], "n": 2, "ok": true, "x": null}'}]};
  await request([{role: "user", content: "tool t2"}]); await sleep(120);
  const c2 = await (await fetch(`http://127.0.0.1:${UI}/api/exchanges/5`)).json();
  await request([{role: "user", content: "tool t2"}, {role: "assistant", content: c2.assembled.content}, u3]); await sleep(200);
  await go("#/exchange/6/request");
  const fmt = `document.querySelector('#p-new .block.tool_result .fmt')`;
  check("JSON in a string has a format toggle", await ev(`${fmt}.textContent`) === "format");
  await ev(`${fmt}.click()`);
  check("format shows it parsed", await ev(`!!document.querySelector('#p-new .block.tool_result .json .k')`));

  console.log("controls");
  await ev(`document.getElementById('filter').value='fake-no-such-text'; document.getElementById('filter').dispatchEvent(new Event('input'))`); await sleep(300);
  check("filter hides what does not match", await ev(`[...document.querySelectorAll('.row')].filter(r => r.style.display !== 'none').length`) === 0);
  await ev(`document.getElementById('filter').value='tool t2'; document.getElementById('filter').dispatchEvent(new Event('input'))`); await sleep(300);
  check("filter matches text", await ev(`[...document.querySelectorAll('.row')].filter(r => r.style.display !== 'none').length`) === 2);
  await ev(`document.getElementById('filter').value='unpriced'; document.getElementById('filter').dispatchEvent(new Event('input'))`); await sleep(300);
  check("filter matches model", await ev(`[...document.querySelectorAll('.row')].filter(r => r.style.display !== 'none').map(r => r.dataset.id).join()`) === "4");
  await ev(`document.getElementById('filter').value=''; document.getElementById('filter').dispatchEvent(new Event('input'))`); await sleep(300);

  const before = await ev(`document.querySelectorAll('.row').length`);
  await ev(`document.getElementById('pause').click()`);
  await request([{role: "user", content: "hello while paused"}]); await sleep(200);
  check("paused: nothing new is shown", await ev(`document.querySelectorAll('.row').length`) === before && /held/.test(await ev(`document.getElementById('pause').textContent`)));
  await ev(`document.getElementById('pause').click()`); await sleep(150);
  check("resume shows what was held", await ev(`document.querySelectorAll('.row').length`) === before + 1 && await ev(`document.querySelector('.row').querySelector('.spinner') === null`));

  await ev(`document.body.dispatchEvent(new KeyboardEvent('keydown', {key: 'j', bubbles: true}))`); await sleep(100);
  await ev(`document.body.dispatchEvent(new KeyboardEvent('keydown', {key: '2', bubbles: true}))`); await sleep(100);
  check("keyboard: 2 goes to the Reply tab", /\/reply$/.test(await ev(`location.hash`)));
  await ev(`document.getElementById('theme').click()`); await sleep(50);
  const dark = await ev(`document.documentElement.dataset.theme`);
  await go("#/exchange/3/request"); await shot("theme-" + dark);
  await ev(`document.getElementById('theme').click()`);

  console.log("§9.5 reload");
  const countBefore = await ev(`document.querySelectorAll('.row').length`);
  const during = request([{role: "user", content: "slow during reload"}]);
  await sleep(500);
  await send("Page.reload"); await sleep(800);
  check("history restored from snapshot", await ev(`document.querySelectorAll('.row').length`) === countBefore + 1);
  check("selected link survives reload", await ev(`location.hash`) === "#/exchange/3/request" && await ev(`!!document.querySelector('#p-system')`));
  check("tool pairs restored", await ev(`!!document.querySelector('#p-new .pair .badge.paired')`));
  const answered = await during;
  check("the agent's stream was unaffected", answered.includes("message_stop"));
  await sleep(200);
  check("the in-flight exchange finishes after reload", await ev(`document.querySelector('.row .spinner') === null`));
  await go(`#/exchange/999`);
  check("an evicted link says so", /not in the buffer/.test(await ev(`document.getElementById('content').textContent`)));

  console.log("other shapes");
  const png = "A".repeat(4000);
  const longValue = Array.from({length: 40}, (_, i) => `value line ${i}`).join("\n") + "\nFINAL-VALUE";
  await request([
    {role: "user", content: [{type: "image", source: {type: "base64", media_type: "image/png", data: png}}, {type: "text", text: "look", cache_control: {type: "ephemeral"}}]},
    {role: "assistant", content: [{type: "tool_use", id: "toolu_h", name: "Write", input: {file: "a", body: longValue}}]},
    {role: "user", content: [{type: "tool_result", tool_use_id: "toolu_h", content: [{type: "text", text: "written"}, {type: "image", source: {type: "base64", media_type: "image/gif", data: "AAAA"}}]}]},
    {role: "assistant", content: "ok"}, {role: "user", content: "hello"}]);
  await sleep(200);
  const shapeId = await ev(`Number(document.querySelector('.row').dataset.id)`);
  await go(`#/exchange/${shapeId}/message/0`);
  const imageText = await ev(`document.querySelector('#p-msg').textContent`);
  check("image: media type and size, not the data", imageText.includes("image/png") && imageText.includes("2.9 KB") && !imageText.slice(0, 400).includes("AAAAAAAAAA"), imageText.slice(0, 200));
  await go(`#/exchange/${shapeId}/request`);
  const longInput = `[...document.querySelectorAll('#p-conv .block.tool_use .lt')][0]`;
  check("a long JSON string value starts collapsed", await ev(`${longInput}.classList.contains('closed')`));
  await ev(`${longInput}.querySelector('.lt-toggle').click()`);
  check("and opens in full", (await ev(`${longInput}.textContent`)).includes("FINAL-VALUE"));
  check("a list-shaped tool result is shown", (await ev(`document.querySelector('#p-conv').textContent`)).includes("written"));

  await fetch(`http://127.0.0.1:${PROXY}/responses`, {method: "POST", headers: {"content-type": "application/json", authorization: "Bearer sk-CODEX-FULL-SECRET-TOKEN-012345"},
    body: JSON.stringify({model: "gpt-5-codex", instructions: "CODEX-INSTRUCTIONS", stream: true, tools: [{type: "function", name: "shell", description: "Runs a shell", parameters: {type: "object"}}],
      input: [{type: "message", role: "user", content: [{type: "input_text", text: "codex prompt"}]}]})}).then(r => r.text());
  await sleep(200);
  const codexId = await ev(`Number(document.querySelector('.row').dataset.id)`);
  await go(`#/exchange/${codexId}/request`);
  check("Codex request in the four panels", (await ev(`document.querySelector('#p-system').textContent`)).includes("CODEX-INSTRUCTIONS") && (await ev(`document.querySelector('#p-tools').textContent`)).includes("Runs a shell") && (await ev(`document.querySelector('#p-new').textContent`)).includes("codex prompt"));
  await go(`#/exchange/${codexId}/reply`);
  check("Codex reply", (await ev(`document.querySelector('#p-reply').textContent`)).includes("Codex says hi") && await ev(`document.querySelector('#p-reply .pair .badge.pending')?.textContent`) === "pending");

  console.log("export");
  await send("Browser.setDownloadBehavior", {behavior: "allow", downloadPath: SCRATCH});
  await ev(`document.getElementById('export').click()`); await sleep(500);
  const names = readdirSync(SCRATCH).filter(n => /^traffic-.*\.json$/.test(n));
  check("Export button downloads traffic-<timestamp>.json", names.length === 1, names);
  if (names.length) {
    const text = readFileSync(join(SCRATCH, names[0]), "utf8");
    check("export has no full token", !text.includes("BROWSER-TEST-FULL-SECRET") && !text.includes("CODEX-FULL-SECRET") && JSON.parse(text).length > 5);
    rmSync(join(SCRATCH, names[0]));
  }

  console.log("reconnect");
  const realErrors = errors.length;
  server.kill(); await sleep(2500);
  check("server gone: shows reconnecting", await ev(`document.getElementById('conn').textContent`) === "reconnecting");
  server = startDashboard();
  await waitHttp(`http://127.0.0.1:${UI}/`);
  await sleep(2500);
  errors.length = realErrors;      // refused connections while the server was down are expected
  check("server back: live again without a reload", await ev(`document.getElementById('conn').textContent`) === "live");
  await request([{role: "user", content: "hello again"}]); await sleep(200);
  check("new traffic shows after reconnecting", await ev(`document.querySelectorAll('.row').length`) === 1);

  await ev(`document.getElementById('clear').click()`); await sleep(200);
  check("clear empties the list", await ev(`document.querySelectorAll('.row').length`) === 0);
  check("no uncaught errors in the page", errors.length === 0, errors.slice(0, 5));
}

try { await main(); }
catch (e) { fails.push("crashed"); console.log("CRASH", e); }
finally {
  for (const p of procs) p.kill();
  await sleep(400);
}
console.log(`\n${passes} passed, ${fails.length} failed`);
for (const f of fails) console.log("  FAILED: " + f);
console.log("screenshots in " + SCRATCH);
process.exit(fails.length ? 1 : 0);
