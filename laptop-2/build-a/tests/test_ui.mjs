#!/usr/bin/env node
// Drives the real page in headless Chrome over CDP, with traffic from the fake upstream.
//   node tests/test_ui.mjs [screenshot-dir]
// Uses ports 8051 (proxy) and 8052 (UI). Starts and stops everything it needs, and uses
// a throwaway Chrome profile.
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const PROXY = 8051, UI = 8052;
const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const SHOTS = process.argv[2];
const sleep = ms => new Promise(r => setTimeout(r, ms));
let passed = 0; const failed = [];
function check(name, ok, detail) {
  if (ok) { passed++; console.log("ok    " + name); }
  else { failed.push(name); console.log("FAIL  " + name + (detail !== undefined ? "  [" + JSON.stringify(detail) + "]" : "")); }
}

const procs = [];
function run(cmd, args) {
  const p = spawn(cmd, args, { cwd: ROOT, env: { ...process.env, PYTHONDONTWRITEBYTECODE: "1" }, stdio: ["ignore", "pipe", "ignore"] });
  procs.push(p);
  return p;
}
const profile = mkdtempSync(join(tmpdir(), "dash-ui-test-"));

function cleanup() {
  for (const p of procs) { try { p.kill("SIGKILL"); } catch {} }
}

// ---- CDP
class Page {
  constructor(ws) { this.ws = ws; this.id = 0; this.wait = new Map(); this.errors = [];
    ws.onmessage = ev => {
      const m = JSON.parse(ev.data);
      if (m.id && this.wait.has(m.id)) { const w = this.wait.get(m.id); this.wait.delete(m.id); m.error ? w.rej(new Error(m.error.message)) : w.res(m.result); }
      if (m.method === "Runtime.exceptionThrown") this.errors.push(m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text);
    };
  }
  send(method, params = {}) { const id = ++this.id; return new Promise((res, rej) => { this.wait.set(id, { res, rej }); this.ws.send(JSON.stringify({ id, method, params })); }); }
  async eval(expr) {
    const r = await this.send("Runtime.evaluate", { expression: expr, returnByValue: true, awaitPromise: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);
    return r.result.value;
  }
  async until(expr, timeout = 6000) {
    const end = Date.now() + timeout;
    while (Date.now() < end) { if (await this.eval(expr)) return true; await sleep(20); }
    return false;
  }
  async shot(name) {
    if (!SHOTS) return;
    const r = await this.send("Page.captureScreenshot", { format: "png" });
    writeFileSync(join(SHOTS, name + ".png"), Buffer.from(r.data, "base64"));
  }
  // Page.reload returns at once, so wait until the old document is really gone.
  async reload() {
    await this.eval(`window.__oldDocument=true`);
    await this.send("Page.reload");
    await this.until(`window.__oldDocument===undefined && document.readyState!=="loading" && !!document.getElementById("conn")`);
  }
  async nav(hash) { await this.eval(`location.hash=${JSON.stringify(hash)}`); await sleep(80); }
  text(sel) { return this.eval(`(document.querySelector(${JSON.stringify(sel)})||{}).innerText`); }
}

// ---- traffic
const API_KEY = "sk-ant-api03-FULLSECRETTOKENVALUE-abcdefghijklmnopqrstuvwxyz";
async function send(messages, opts = {}) {
  const body = { model: opts.model || "claude-opus-5-5", max_tokens: 100, stream: true, temperature: 0.5, metadata: { user_id: "u1", nested: { a: { b: { c: { d: 1 } } } } },
    system: opts.system ?? SYSTEM, tools: opts.tools ?? TOOLS, messages };
  const r = await fetch(`http://127.0.0.1:${PROXY}/v1/messages?beta=true`, { method: "POST", headers: { "content-type": "application/json", "x-api-key": API_KEY, "cookie": "s=" + API_KEY }, body: JSON.stringify(body) });
  return r.text();
}
const LONG_LINES = Array.from({ length: 342 }, (_, i) => `System rule ${i}: <b>keep</b> this line exactly   as written.`).join("\n");
const SYSTEM = [
  { type: "text", text: "You are a test agent." },
  { type: "text", text: LONG_LINES, cache_control: { type: "ephemeral" } },
  { type: "text", text: "Final block." },
];
const TOOLS = [
  { name: "Bash", description: "Runs a shell command.", input_schema: { type: "object", properties: { command: { type: "string", description: "cmd" } }, required: ["command"] } },
  { name: "Read", description: "D".repeat(1500), input_schema: { type: "object", properties: { path: { type: "string" } } }, cache_control: { type: "ephemeral" } },
  { name: "<img src=x onerror=window.__pwned=2>", description: "<script>window.__pwned=3</script>", input_schema: { type: "object" } },
];
const XSS_RESULT = "<script>window.__pwned=1</script><img src=x onerror='window.__pwned=1'>\n".repeat(30);

// ---- main
async function main() {
  const fake = run("python3", ["tests/fake_upstream.py"]);
  const port = await new Promise(res => fake.stdout.once("data", d => res(Number(String(d).trim()))));
  run("python3", ["dashboard.py", "--port", PROXY, "--ui-port", UI, "--upstream", `http://127.0.0.1:${port}`, "--quiet"].map(String));
  for (let i = 0; i < 100; i++) { try { await fetch(`http://127.0.0.1:${UI}/api/export`); break; } catch { await sleep(50); } }

  run(CHROME, ["--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", `--user-data-dir=${profile}`, "--window-size=1500,1000", "about:blank"]);
  const portFile = join(profile, "DevToolsActivePort");
  for (let i = 0; i < 200 && !existsSync(portFile); i++) await sleep(50);
  await sleep(100);
  const cdp = readFileSync(portFile, "utf8").split("\n")[0];
  const targets = await (await fetch(`http://127.0.0.1:${cdp}/json`)).json();
  const ws = new WebSocket(targets.find(t => t.type === "page").webSocketDebuggerUrl);
  await new Promise(r => { ws.onopen = r; });
  const p = new Page(ws);
  await p.send("Runtime.enable"); await p.send("Page.enable");
  await p.send("Emulation.setDeviceMetricsOverride", { width: 1500, height: 1000, deviceScaleFactor: 1, mobile: false });

  await p.send("Page.navigate", { url: `http://localhost:${UI}/` });
  await p.until(`document.getElementById("conn")?.className==="live"`);
  check("§6.1 connection status shows live", await p.text("#conn") === "live");
  check("§7 warning banner about sharing", (await p.text("#warn")).includes("shown in full"));
  check("§3 page loads nothing external", await p.eval(`performance.getEntriesByType("resource").every(r=>r.name.includes("localhost"))`));
  await p.eval(`window.__marker=1; window.__rows=[]; new MutationObserver(m=>{for(const r of m)for(const n of r.addedNodes)if(n.classList&&n.classList.contains("row"))window.__rows.push([n.dataset.id,Date.now()])}).observe(document.getElementById("list"),{childList:true})`);

  // AC2: a new row within about 200 ms, and text streaming into the reply.
  const t0 = Date.now();
  const slow = send([{ role: "user", content: "SLOW" }]);
  await p.until(`window.__rows.length>0`);
  const rowDelay = (await p.eval(`window.__rows[0][1]`)) - t0;
  check("AC2 new row within about 200ms", rowDelay < 250, rowDelay);
  console.log(`      (row appeared after ${rowDelay} ms)`);
  check("§6.1 spinner while streaming", await p.eval(`getComputedStyle(document.querySelector(".row .spin")).display!=="none"`));
  await p.nav("#/exchange/1/reply");
  await p.until(`document.getElementById("body").innerText.includes("word1")`);
  const early = await p.text("#body");
  await p.until(`document.getElementById("body").innerText.includes("word7")`);
  const later = await p.text("#body");
  check("AC2 reply text streams into the Reply tab", early.includes("word1") && !early.includes("word11") && later.includes("word7"), early);
  await slow;
  check("§6.1 spinner stops when done", await p.until(`document.querySelector(".row .spin").style.display==="none"`));
  const row1 = await p.text(".row");
  check("§6.1/§6.5 row shows id, model, status, duration, tokens, cache hit, stop reason, cost, ttfb, ttft, tok/s",
    ["#1", "claude-opus-5-5", "200", "end_turn", "in 12", "out 340", "read 18,000", "write 500", "hit 97%", "$", "ttfb", "ttft", "tok/s"].every(s => row1.includes(s)), row1);

  // Request tab.
  await p.nav("#/exchange/1/request");
  const heads = await p.eval(`[...document.querySelectorAll(".panel-head .title")].map(n=>n.innerText)`);
  check("AC7/§6.10.3 four panels in fixed order, then parameters",
    /^System instructions · 3 blocks · ~\d+k? tokens$/.test(heads[0]) && heads[1] === "Tools · 3" && heads[2] === "Conversation so far · 0 messages" && heads[3] === "New message" && heads[4].startsWith("Other parameters"), heads);
  check("§6.10.3 each panel has its own accent color", await p.eval(`new Set(["p-system","p-tools","p-conv","p-new"].map(i=>getComputedStyle(document.getElementById(i)).borderLeftColor)).size===4`));
  check("§6.10.3 panels separated by gaps", await p.eval(`parseFloat(getComputedStyle(document.getElementById("p-system")).marginBottom)>=10`));
  const collapsed = await p.eval(`(()=>{const lt=document.querySelector("#p-system .lt.collapsed");return lt&&{toggle:lt.querySelector(".lt-toggle").innerText,h:lt.querySelector(".lt-body").clientHeight,lh:parseFloat(getComputedStyle(lt.querySelector(".lt-body")).lineHeight),mask:getComputedStyle(lt.querySelector(".lt-body")).webkitMaskImage}})()`);
  check("AC11/§6.10.1 long system prompt starts collapsed with size toggle", collapsed && /^Show all \(342 lines, \d+\.\dk chars\)$/.test(collapsed.toggle), collapsed);
  check("§6.10.1 collapsed shows 6 lines with a fade", collapsed && Math.round(collapsed.h / collapsed.lh) === 6 && collapsed.mask.includes("gradient"), collapsed);
  check("§6.10.1 short text has no toggle", await p.eval(`document.querySelectorAll("#p-system .block")[0].querySelector(".lt-toggle")===null && document.querySelectorAll("#p-system .block")[2].querySelector(".lt-toggle")===null`));
  await p.eval(`document.querySelector("#p-system .lt-toggle").click()`);
  const fullSystem = await p.eval(`document.querySelectorAll("#p-system .block")[1].querySelector(".lt-body").textContent`);
  check("AC7 system text word for word when opened", fullSystem === LONG_LINES, fullSystem.length);
  check("§6.10.1 toggle becomes Collapse; pre-wrap, no sideways scroll", await p.eval(`(()=>{const lt=document.querySelectorAll("#p-system .block")[1].querySelector(".lt");const b=lt.querySelector(".lt-body");return lt.querySelector(".lt-toggle").innerText==="Collapse"&&getComputedStyle(b).whiteSpace==="pre-wrap"&&b.scrollWidth<=b.clientWidth+1})()`));
  check("§6.2 system blocks flagged with cache_control", await p.eval(`document.querySelector("#p-system .badge.cache")!==null`));
  const toolText = await p.text("#p-tools");
  check("AC7 all tools with name and description", toolText.includes("Bash") && toolText.includes("Runs a shell command.") && toolText.includes("Read") && toolText.includes("<script>window.__pwned=3</script>"), toolText.slice(0, 200));
  check("§6.2 long tool description collapsed", await p.eval(`document.querySelector("#p-tools .lt.collapsed")!==null`));
  await p.eval(`document.querySelector("#p-tools .fold-head").click()`);
  check("§6.2 input schema expands", (await p.text("#p-tools .json")).includes('"required"'));
  check("§6.2 other parameters table", await (async () => { const t = await p.text(".p-params"); return ["max_tokens", "temperature", "metadata", "model"].every(k => t.includes(k)); })());

  // Sticky headers and jump index.
  const sticky = await p.eval(`(async()=>{const b=document.getElementById("body");document.querySelectorAll("#p-system .lt-toggle");b.scrollTop=800;await new Promise(r=>setTimeout(r,50));const h=document.querySelector("#p-system .panel-head").getBoundingClientRect(),bb=b.getBoundingClientRect(),i=document.getElementById("index").getBoundingClientRect();return{off:h.top-bb.top,idx:i.top-bb.top,pos:getComputedStyle(document.querySelector("#p-system .panel-head")).position,s:b.scrollTop}})()`);
  check("§6.10.3 panel header and index stay pinned while scrolling", sticky.pos === "sticky" && sticky.s === 800 && Math.round(sticky.idx) === 0 && Math.round(sticky.off) === 38, sticky);
  await p.shot("request-scrolled");
  await p.eval(`document.querySelectorAll(".jump")[3].click()`); await sleep(100);
  check("§6.10.3 index jumps to a part", await p.eval(`(()=>{const r=document.getElementById("p-new").getBoundingClientRect(),b=document.getElementById("body").getBoundingClientRect();return r.top-b.top<300})()`));
  await p.eval(`document.getElementById("body").scrollTop=0`);

  // A tool turn, with the pending state held open.
  const firstReply = send([{ role: "user", content: "run a TOOL one" }]);
  await firstReply;
  await p.until(`document.querySelectorAll(".row").length===2`);
  const ids = await p.eval(`[...document.querySelectorAll(".row")].map(r=>r.dataset.id)`);
  check("AC7 list is newest first", ids[0] === "2" && ids[1] === "1", ids);
  await p.nav("#/exchange/2/reply");
  await p.until(`document.querySelector(".pair")!==null`);
  const reply = await p.text("#body");
  check("AC8 the call shows as pending in the reply", (await p.text(".block-head .badge.pending")) === "pending" && reply.includes("Pending"), reply);
  check("§6.3 tool name, id and assembled, pretty input", reply.includes("toolu_one") && reply.includes("Bash") && reply.includes('"command": "ls -la"'), reply);
  check("§6.3 stop reason, model", reply.includes("tool_use") && reply.includes("claude-opus-5-5"));
  check("§6.3 usage panel", ["Input", "Output", "Cache read", "Cache write", "Cache hit", "18,000", "500", "97%"].every(s => reply.includes(s)), reply);
  check("§6.3 thinking collapsed by default", !reply.includes("I should run it.") && reply.includes("thinking"));
  const order = await p.eval(`[...document.querySelectorAll(".panel.p-reply")[0].querySelectorAll(":scope > .panel-body > .block")].map(b=>b.querySelector(".badge").innerText)`);
  check("§6.3 blocks in the order produced", JSON.stringify(order) === JSON.stringify(["thinking", "text", "tool_use"]), order);
  await p.eval(`document.querySelector(".b-thinking .fold-head").click()`);
  check("§6.3 thinking opens", (await p.text("#body")).includes("I should run it."));
  await p.shot("reply-pending");

  const blocks = (await (await fetch(`http://127.0.0.1:${UI}/api/exchanges/2`)).json()).assembled.content;
  const history = [{ role: "user", content: "run a TOOL one" }, { role: "assistant", content: blocks }];
  await send([...history, { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_one", content: XSS_RESULT, is_error: true, cache_control: { type: "ephemeral" } }] }]);
  await p.until(`document.querySelector(".side.result .badge.err")!==null`);
  const paired = await p.text(".pair");
  check("AC8 the result appears beside the call with no reload", (await p.eval(`window.__marker`)) === 1 && paired.includes("request #3") && paired.includes("reply #2") && paired.includes("<script>window.__pwned=1</script>"), paired);
  check("§6.4 pair is side by side", await p.eval(`(()=>{const a=document.querySelector(".side.call").getBoundingClientRect(),b=document.querySelector(".side.result").getBoundingClientRect();return Math.abs(a.top-b.top)<2&&b.left>a.right-1})()`));
  check("§6.4 red error marker and size", await p.eval(`(()=>{const m=document.querySelector(".side.result .badge.err");return m&&getComputedStyle(m).backgroundColor!=="rgba(0, 0, 0, 0)"})()`) && paired.includes("chars"));
  check("§6.3 status updated in place", (await p.eval(`document.querySelector(".block-head .badge.paired")!==null`)));
  check("AC11 long tool result starts collapsed", await p.eval(`document.querySelector(".side.result .lt.collapsed .lt-toggle")!==null`));
  await p.eval(`document.querySelector(".side.result .lt-toggle").click()`);
  check("AC11 long tool result opens in full", (await p.eval(`document.querySelector(".side.result .lt-body").textContent`)) === XSS_RESULT);
  check("AC11 <script> shown as text and not run", (await p.eval(`window.__pwned`)) === undefined && await p.eval(`document.querySelector("#body script, #body img")===null`));
  await p.shot("reply-paired");

  // Jumping from a pair.
  await p.eval(`document.querySelector(".side.result").click()`); await sleep(100);
  check("§6.4 clicking the result jumps to its request", (await p.eval(`location.hash`)) === "#/exchange/3/request", await p.eval(`location.hash`));
  const hasPair = await p.text("#p-new");
  check("§6.4 pair beside the tool_result in the new message", hasPair.includes("reply #2") && hasPair.includes("request #3") && await p.eval(`document.querySelector("#p-new .pair .side.call")!==null`));
  check("AC7 tool result request shows earlier conversation", (await p.text("#p-conv")).includes("user #0") && (await p.text("#p-conv")).includes("assistant #1"));
  check("§6.10.3 role and block cards", await p.eval(`!!document.querySelector("#p-conv .card.user")&&!!document.querySelector("#p-conv .card.assistant")&&["tool_use","thinking","text"].every(t=>document.querySelector("#p-conv .badge."+t))&&getComputedStyle(document.querySelector("#p-conv .card.user .head")).backgroundColor!==getComputedStyle(document.querySelector("#p-conv .card.assistant .head")).backgroundColor`));
  await p.eval(`document.querySelector("#p-new .side.call").click()`); await sleep(100);
  check("§6.4 clicking the call jumps to its reply", (await p.eval(`location.hash`)) === "#/exchange/2/reply");

  // Diff.
  await p.nav("#/exchange/3/diff");
  const diff = await p.eval(`[...document.querySelectorAll("#body .card .who")].map(n=>n.innerText)`);
  check("AC3/§6.7 diff shows only the new messages", JSON.stringify(diff) === JSON.stringify(["assistant #1", "user #2"]), diff);
  const note = await p.text(".note");
  check("§6.7 diff says what was resent", note.includes("request #2") && note.includes("1 earlier message was resent") && note.includes("2 are new"), note);
  await p.nav("#/exchange/2/diff");
  check("§6.7 a new conversation is explained", (await p.text(".note")).includes("starts a conversation"));
  await p.shot("diff");

  // Raw.
  await p.nav("#/exchange/3/raw");
  const raw = await p.text("#body");
  check("§6.8 raw tab sections", ["Request headers", "Request JSON", "Response headers", "Raw SSE"].every(s => raw.includes(s)), raw.slice(0, 300));
  check("§6.8 each has a copy button", await p.eval(`[...document.querySelectorAll(".panel-head")].every(h=>h.querySelector(".copy"))`));
  check("§7 redaction in the raw tab", raw.includes(API_KEY.slice(0, 16) + "...") && !raw.includes("FULLSECRET"));
  check("§6.8 raw SSE lines", raw.includes("event: message_start"), raw.slice(-300));

  // JSON formatting.
  await p.nav("#/exchange/2/request");
  await p.nav("#/exchange/2/raw");
  const j = await p.eval(`(()=>{const b=document.querySelectorAll(".json")[1];const t=b.innerText;return{colors:["j-key","j-str","j-num"].map(c=>getComputedStyle(b.querySelector("."+c)||document.body).color),t,bool:!!b.querySelector(".j-bool"),sum:[...b.querySelectorAll(".j-sum")].map(s=>s.innerText)}})()`);
  check("AC11 JSON has distinct key, string, number colors", new Set(j.colors).size === 3, j.colors);
  check("AC11/§6.10.2 JSON is indented across lines", await p.eval(`(()=>{const b=document.querySelectorAll(".json")[1];const n=b.querySelectorAll(".jl").length;const k=b.querySelector(".jl .jl");return n>5&&parseFloat(getComputedStyle(k).paddingLeft)>0})()`));
  check("§6.10.2 deep nodes start folded with a summary", j.sum.some(s => /^\{…\} \d+ keys?$/.test(s)), j.sum);
  check("§6.10.2 array summary wording", await p.eval(`(async()=>{const b=document.querySelectorAll(".json")[1];const c=[...b.querySelectorAll(".caret")];c[0].click();await new Promise(r=>setTimeout(r,30));const s=document.querySelectorAll(".json")[1].innerText;document.querySelectorAll(".json")[1].querySelector(".caret").click();return /\\{…\\} \\d+ keys/.test(s)})()`));
  // Copy is exact and compact.
  const copied = await p.eval(`(async()=>{let got;navigator.clipboard.writeText=async t=>{got=t};document.querySelectorAll(".json")[1].querySelector(".json-copy .copy").click();await new Promise(r=>setTimeout(r,20));return got})()`);
  const bodyNow = (await (await fetch(`http://127.0.0.1:${UI}/api/exchanges/2`)).json()).request_body;
  check("§6.10.2 copy gives exact, unformatted JSON", copied === JSON.stringify(bodyNow), (copied || "").slice(0, 100));
  check("§6.10.2 fonts: JSON monospace, prose regular", await p.eval(`getComputedStyle(document.querySelector(".json")).fontFamily.includes("monospace")`) && await p.eval(`(async()=>{location.hash="#/exchange/2/reply";await new Promise(r=>setTimeout(r,80));return !getComputedStyle(document.querySelector(".b-text .lt-body")).fontFamily.includes("monospace")})()`));

  // JSON inside a string gets a format toggle.
  await send([{ role: "user", content: "x" }, { role: "assistant", content: [{ type: "tool_use", id: "toolu_j", name: "Bash", input: {} }] },
    { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_j", content: JSON.stringify({ files: ["a", "b"], ok: true, none: null, n: 1 }) }] }]);
  await p.until(`document.querySelectorAll(".row").length===4`);
  await p.nav("#/exchange/4/request");
  check("§6.10.2 JSON string has a format toggle", await p.eval(`document.querySelector("#p-new .fmt")!==null`));
  await p.eval(`document.querySelector("#p-new .fmt").click()`);
  check("§6.10.2 format shows it parsed", (await p.text("#p-new")).includes('"files": [') && await p.eval(`!!(document.querySelector("#p-new .j-bool")&&document.querySelector("#p-new .j-null"))`), await p.text("#p-new"));

  // Message drill-down.
  await p.nav("#/exchange/3/request");
  await p.eval(`document.querySelector("#p-conv .card.assistant a").click()`); await sleep(100);
  check("§6.6 a message opens with a stable link", (await p.eval(`location.hash`)) === "#/exchange/3/message/1");
  const msg = await p.text("#body");
  check("§6.6 role, position and block details", msg.includes("assistant · position 1 of 3 in messages") && msg.includes("I should run it.") && msg.includes("toolu_one") && msg.includes("Raw JSON"), msg);
  await p.nav("#/exchange/3/message/2");
  const msg2 = await p.text("#body");
  check("§6.6 tool_result: id, is_error, cache_control", msg2.includes("tool_use_id toolu_one") && msg2.includes("is_error") && await p.eval(`document.querySelector("#body .badge.cache")!==null`), msg2.slice(0, 300));
  const rawMsg = await p.eval(`(async()=>{let got;navigator.clipboard.writeText=async t=>{got=t};const ps=document.querySelectorAll(".panel");ps[ps.length-1].querySelector(".json-copy .copy").click();await new Promise(r=>setTimeout(r,20));return got})()`);
  const body3 = (await (await fetch(`http://127.0.0.1:${UI}/api/exchanges/3`)).json()).request_body;
  check("§6.6 raw JSON copy of the message", rawMsg === JSON.stringify(body3.messages[2]), (rawMsg || "").slice(0, 100));
  await p.reload(); await p.until(`document.getElementById("conn")?.className==="live" && document.querySelectorAll(".row").length===4`);
  await sleep(150);
  check("§6.6 a link still resolves after reload", (await p.text("#body")).includes("position 2 of 3"));
  check("AC5 the reloaded page is a new document", (await p.eval(`window.__marker`)) === undefined);
  check("AC5 reload restores history from the snapshot", (await p.eval(`document.querySelectorAll(".row").length`)) === 4 && (await p.eval(`document.querySelector(".row").dataset.id`)) === "4");
  await p.nav("#/exchange/2/message/reply");
  const replyMsg = await p.text("#body");
  check("§6.6 the reply message opens too", replyMsg.includes("assistant · reply") && replyMsg.includes("toolu_one") && replyMsg.includes("Raw JSON"), replyMsg.slice(0, 200));
  await p.nav("#/exchange/2/reply");
  check("AC5 tool pairing survives reload", await p.eval(`document.querySelector(".block-head .badge.paired")!==null`));
  await p.nav("#/exchange/99/request");
  check("§6.6 a missing exchange explains itself", (await p.text("#body")).includes("not in the buffer"));

  // Images.
  await send([{ role: "user", content: [{ type: "image", source: { type: "base64", media_type: "image/png", data: "A".repeat(4000) } }, { type: "text", text: "what is this" }] }]);
  await p.until(`document.querySelectorAll(".row").length===5`);
  await p.nav("#/exchange/5/message/0");
  const img = await p.text("#body");
  check("§6.6 images show media type and size", img.includes("image/png") && img.includes("3.0k bytes"), img.slice(0, 200));

  // Statistics click through.
  await p.nav("#/exchange/2/reply");
  const strip = await p.text("#strip");
  check("AC9 stats strip: tokens, cost, duration, ttft and more", ["INPUT", "OUTPUT", "CACHE READ", "CACHE WRITE", "COST", "TOTAL", "TTFB", "TTFT", "OUTPUT/S"].every(s => strip.toUpperCase().includes(s)) && strip.includes("$"), strip);
  const sumText = await p.text("#summary");
  check("§6.5 session summary", ["REQUESTS", "INPUT", "OUTPUT", "COST", "WALL CLOCK", "AVG LATENCY", "CACHE HIT"].every(s => sumText.toUpperCase().includes(s)) && sumText.includes("5"), sumText);
  await p.eval(`[...document.querySelectorAll("#summary .stat")].find(s=>s.innerText.toUpperCase().includes("COST")).click()`); await sleep(100);
  const statsView = await p.text("#body");
  check("AC10 a session statistic lists its exchanges", (await p.eval(`location.hash`)) === "#/stats/cost" && statsView.includes("#2") && statsView.includes("$"), statsView.slice(0, 200));
  await p.eval(`[...document.querySelectorAll("#body td a")].find(a=>a.innerText==="reply").click()`); await sleep(100);
  const statMsg = await p.text("#body");
  check("AC10 and on to the message, its full content and raw JSON", statMsg.includes("assistant · reply") && statMsg.includes("Raw JSON"), statMsg.slice(0, 200));
  await p.nav("#/exchange/2/reply");
  await p.eval(`[...document.querySelectorAll("#strip .stat")].find(s=>s.innerText.toUpperCase().includes("OUTPUT")).click()`); await sleep(100);
  check("AC10 a per-request statistic reaches its message", (await p.eval(`location.hash`)) === "#/exchange/2/message/reply");

  // Tools view.
  await send([{ role: "user", content: "TOOLS2" }]);
  await p.until(`document.querySelectorAll(".row").length===6`);
  await sleep(100);
  await p.eval(`document.querySelector('.tab[data-tab="tools"]').click()`); await sleep(100);
  const tv = await p.text("#body");
  check("AC9 Tools view shows calls per tool", tv.includes("Tool usage") && /Bash\s+2/.test(tv) && /Read\s+1/.test(tv), tv.slice(0, 400));
  check("§6.5 errors, average size, average wait columns", ["Errors", "Avg result size", "Avg call → result"].every(s => tv.includes(s)));
  const usedRow = await p.eval(`[...document.querySelectorAll("#body tr.link")].find(r=>r.innerText.startsWith("Bash")).innerText`);
  check("§6.5 Bash row: 2 calls, 1 error, average size and wait", /^Bash\s+2\s+1\s+[\d.]+k chars\s+\d+(ms|\.\d+s)$/.test(usedRow), usedRow);
  check("§6.4 Tools view lists every pair, newest first", await p.eval(`(()=>{const l=[...document.querySelectorAll("#body .pair-label a:first-child")].map(a=>Number(a.innerText.replace(/\\D/g,"")));return l.length===3&&l.every((v,i)=>!i||l[i-1]>=v)})()`));
  await p.shot("tools");
  await p.eval(`[...document.querySelectorAll("#body tr.link")].find(r=>r.innerText.startsWith("Bash")).click()`); await sleep(100);
  check("§6.5 clicking Bash lists only Bash calls", (await p.eval(`location.hash`)) === "#/tools/Bash" && await p.eval(`[...document.querySelectorAll("#body .pair .side.call h4 b")].every(b=>b.innerText==="Bash")`) && await p.eval(`document.querySelectorAll("#body .pair").length`) === 2);
  await p.eval(`document.querySelector("#body .pair a[href*='message/reply']").click()`); await sleep(100);
  check("AC10 from a tool statistic to its message", (await p.text("#body")).includes("Raw JSON"));

  // no_result rendering.
  await send([{ role: "user", content: "TOOL gone" }]);
  await p.until(`document.querySelectorAll(".row").length===7`);
  const goneBlocks = (await (await fetch(`http://127.0.0.1:${UI}/api/exchanges/7`)).json()).assembled.content;
  await p.nav("#/exchange/7/reply");
  check("§6.4 pending before moving on", await p.eval(`document.querySelector(".block-head .badge.pending")!==null`));
  await send([{ role: "user", content: "TOOL gone" }, { role: "assistant", content: goneBlocks }, { role: "user", content: "forget it" }]);
  await p.until(`document.querySelector(".block-head .badge.no_result")!==null`);
  check("§6.4 no result once the conversation moves on", (await p.text(".block-head .badge.no_result")) === "no result");

  // State is kept across streaming and switching.
  await p.nav("#/exchange/1/request");
  await p.eval(`document.querySelector("#p-tools .lt-toggle").click()`);
  const open1 = await p.eval(`document.querySelector("#p-tools .lt:not(.collapsed) .lt-toggle")?.innerText`);
  await p.nav("#/exchange/2/request");
  check("§6.10.1 other exchanges unaffected", await p.eval(`document.querySelector("#p-tools .lt.collapsed")!==null`));
  await p.nav("#/exchange/1/request");
  check("§6.10.1 state kept when switching between exchanges", open1 === "Collapse" && await p.eval(`document.querySelector("#p-tools .lt-toggle").innerText`) === "Collapse");
  const live = send([{ role: "user", content: "LONGREPLY" }]);
  await p.until(`document.querySelectorAll(".row").length===9`);
  await p.nav("#/exchange/9/reply");
  await p.until(`document.querySelector(".b-text")!==null`);
  await p.until(`document.getElementById("body").innerText.includes("line 40")`);
  check("§6.3 a reply stays visible while streaming, past the long-text limit", await p.eval(`document.querySelector(".b-text .lt.collapsed")===null`));
  await live; await sleep(150);
  check("§6.10.1 a long reply collapses once finished", await p.eval(`document.querySelector(".b-text .lt.collapsed")!==null`));
  await p.eval(`document.querySelector(".b-text .lt-toggle").click()`);
  const during = send([{ role: "user", content: "SLOW" }]);
  await p.until(`document.querySelectorAll(".row").length===10`);
  await p.nav("#/exchange/9/reply");
  await sleep(200);
  check("§6.10.1 state kept while new events stream in", await p.eval(`document.querySelector(".b-text .lt-toggle").innerText`) === "Collapse");
  await during;

  // Expand all / collapse all.
  await p.nav("#/exchange/3/request");
  await p.eval(`document.querySelector("#p-system .panel-head button").click()`); await sleep(50);
  check("§6.10.1 panel Expand all opens the panel only", await p.eval(`document.querySelector("#p-system .lt.collapsed")===null && document.querySelector("#p-tools .lt.collapsed")!==null`));
  await p.eval(`document.querySelector("#index button:nth-last-child(2)").click()`); await sleep(50);
  check("§6.10.1 tab Expand all opens everything, schemas too", await p.eval(`document.querySelector("#body .lt.collapsed")===null && document.querySelector("#p-tools .json")!==null`));
  await p.eval(`document.querySelector("#index button:nth-last-child(1)").click()`); await sleep(50);
  check("§6.10.1 tab Collapse all closes everything", await p.eval(`document.querySelector("#p-system .lt.collapsed")!==null && document.querySelector("#p-tools .json")===null`));
  check("§6.10.1 every panel and tab has the controls", await p.eval(`(async()=>{for(const t of ["request","reply","diff","raw"]){location.hash="#/exchange/3/"+t;await new Promise(r=>setTimeout(r,60));if(![...document.querySelectorAll("#body .bar button")].some(b=>b.innerText==="Expand all"))return t;if(![...document.querySelectorAll(".panel")].every(p=>[...p.querySelector(".panel-head").querySelectorAll("button")].some(b=>b.innerText==="Collapse all")))return t+" panel"}return true})()`) === true);

  // Filter.
  await p.eval(`(()=>{const f=document.getElementById("filter");f.value="toolu_gone";f.dispatchEvent(new Event("input"))})()`); await sleep(200);
  const visible = () => p.eval(`[...document.querySelectorAll(".row:not(.hide)")].map(r=>r.dataset.id)`);
  let v = await visible();
  check("§6.1 filter matches text", v.includes("7") && v.includes("8") && v.length === 2, v);
  await p.eval(`(()=>{const f=document.getElementById("filter");f.value="NO-SUCH-THING";f.dispatchEvent(new Event("input"))})()`); await sleep(200);
  check("§6.1 filter hides non-matches", (await visible()).length === 0);
  await p.eval(`(()=>{const f=document.getElementById("filter");f.value="CLAUDE-OPUS";f.dispatchEvent(new Event("input"))})()`); await sleep(200);
  check("§6.1 filter matches model", (await visible()).length === 10);
  await p.eval(`(()=>{const f=document.getElementById("filter");f.value="/v1/messages?beta";f.dispatchEvent(new Event("input"))})()`); await sleep(200);
  check("§6.1 filter matches path", (await visible()).length === 10);
  await p.eval(`(()=>{const f=document.getElementById("filter");f.value="";f.dispatchEvent(new Event("input"))})()`); await sleep(200);

  // Unpriced.
  await send([{ role: "user", content: "hello" }], { model: "mystery-model" });
  await p.until(`document.querySelectorAll(".row").length===11`);
  await p.nav("#/exchange/11/reply"); await sleep(120);
  check("AC9 unpriced model shows —", (await p.text(".row")).includes("—") && /COST\s*—/i.test(await p.text("#strip")), await p.text("#strip"));
  check("§6.5 session cost notes the unpriced exchange", (await p.text("#summary")).includes("+ 1 unpriced"), await p.text("#summary"));

  // Pause.
  await p.eval(`document.getElementById("pause").click()`);
  await send([{ role: "user", content: "while paused" }]);
  await sleep(200);
  check("§6.1 while paused, new events are not shown", (await p.eval(`document.querySelectorAll(".row").length`)) === 11 && (await p.text("#pause")).startsWith("Resume ("), await p.text("#pause"));
  await p.eval(`document.getElementById("pause").click()`); await sleep(150);
  check("§6.1 resume shows the buffered events", (await p.eval(`document.querySelectorAll(".row").length`)) === 12 && (await p.text("#pause")) === "Pause" && (await p.text(".row")).includes("end_turn"));

  // Keyboard.
  await p.nav("#/exchange/12/request");
  const key = k => p.send("Input.dispatchKeyEvent", { type: "keyDown", key: k, text: k.length === 1 ? k : undefined });
  await key("j"); await sleep(100);
  check("M5 keyboard: j moves down the list", (await p.eval(`location.hash`)) === "#/exchange/11/request");
  await key("2"); await sleep(100);
  check("M5 keyboard: number switches tab", (await p.eval(`location.hash`)) === "#/exchange/11/reply");
  await p.eval(`document.getElementById("theme").click()`);
  const dark = await p.eval(`getComputedStyle(document.body).backgroundColor`);
  await p.shot("theme-toggled");
  await p.eval(`document.getElementById("theme").click()`);
  check("M5 theme toggle changes colors", dark !== await p.eval(`getComputedStyle(document.body).backgroundColor`));

  // Reload during a stream.
  const inflight = send([{ role: "user", content: "SLOW" }]);
  await p.until(`document.querySelectorAll(".row").length===13`);
  await p.nav("#/exchange/13/reply");
  await sleep(500);
  await p.reload();
  await p.until(`document.getElementById("conn")?.className==="live" && document.querySelectorAll(".row").length===13`);
  const reloaded = await p.until(`document.getElementById("body").innerText.includes("word3")`);
  const text = await inflight;
  check("AC5 reload mid-stream: the agent still gets everything", text.includes("message_stop") && (text.match(/text_delta/g) || []).length === 12);
  await sleep(150);
  check("AC5 reload mid-stream: the page rejoins the stream", reloaded && (await p.text("#body")).includes("word11") && await p.eval(`document.querySelector(".row .spin").style.display==="none"`));

  // Secrets anywhere in the page.
  await p.nav("#/exchange/1/raw");
  const whole = await p.eval(`document.documentElement.outerHTML + JSON.stringify([...document.querySelectorAll(".row")].length)`);
  check("AC6 no full token in the page", !whole.includes("FULLSECRETTOKENVALUE"));

  // Export and clear.
  check("§6.9 export link", await p.eval(`document.getElementById("export").getAttribute("href")`) === "/api/export");
  const exported = await fetch(`http://127.0.0.1:${UI}/api/export`);
  check("§6.9 export file name", /traffic-\d{8}-\d{6}\.json/.test(exported.headers.get("content-disposition")));
  await p.shot("final");
  await p.eval(`document.getElementById("clear").click()`);
  await p.until(`document.querySelectorAll(".row").length===0`);
  check("§6.1 clear empties the list", (await p.eval(`document.querySelectorAll(".row").length`)) === 0 && (await p.text("#summary")).includes("0"));
  check("§6.10.3 no script exceptions during the whole run", p.errors.length === 0, p.errors);

  // A long session: 30 requests of about 600 KB each, 400 messages per request.
  const bulk = Array.from({ length: 400 }, (_, i) => ({ role: i % 2 ? "assistant" : "user", content: `message ${i} ` + "lorem ipsum ".repeat(120) }));
  bulk.push({ role: "user", content: "hello" });
  for (let i = 0; i < 30; i++) await send(bulk);
  await p.until(`document.querySelectorAll(".row").length===30`, 15000);
  const paint = await p.eval(`(async()=>{const t=performance.now();location.hash="#/exchange/1/request";location.hash="#/exchange/30/request";await new Promise(r=>setTimeout(r,0));renderDetail();return performance.now()-t})()`);
  check("large request (401 messages) renders in under 1.5s", paint < 1500, paint);
  console.log(`      (request tab rendered in ${Math.round(paint)} ms)`);
  const t1 = Date.now();
  await p.reload();
  const restored = await p.until(`document.querySelectorAll(".row").length===30`, 20000);
  const reloadMs = Date.now() - t1;
  check("reload restores 18 MB of history within 5s", restored && reloadMs < 5000, reloadMs);
  console.log(`      (snapshot restored in ${reloadMs} ms)`);
  await fetch(`http://127.0.0.1:${UI}/api/clear`, { method: "POST" });
  await p.until(`document.querySelectorAll(".row").length===0`);

  // A phone-width sanity check.
  await p.send("Emulation.setDeviceMetricsOverride", { width: 390, height: 800, deviceScaleFactor: 1, mobile: false });
  await send([{ role: "user", content: "TOOL phone" }]); await p.until(`document.querySelectorAll(".row").length===1`);
  await p.nav("#/exchange/" + await p.eval(`document.querySelector(".row").dataset.id`) + "/reply"); await sleep(120);
  check("narrow layout has no sideways scroll", await p.eval(`document.documentElement.scrollWidth<=390`), await p.eval(`document.documentElement.scrollWidth`));

  console.log(`\n${passed} passed, ${failed.length} failed`);
  for (const f of failed) console.log("  FAILED: " + f);
}

main().catch(e => { console.error(e); failed.push("crash"); }).finally(() => {
  cleanup();
  setTimeout(() => { try { rmSync(profile, { recursive: true, force: true }); } catch {} process.exit(failed.length ? 1 : 0); }, 300);
});
