#!/usr/bin/env node
/* Lightweight QA linter for the authored HTML fragments. Not a full HTML
   validator — a fast backstop for the things that actually break this site:
   raw unescaped "<" inside diagrams/code, unbalanced block tags, thin content. */
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const FRAG = path.join(ROOT, "_fragments");

// every page in the manifest (Part I/II/III) — so all fragments get balance-checked
const EXPECT = Object.keys(require(path.join(ROOT, "assets", "manifest.js")).PAGES);

// tags whose "<" legitimately appears inside <pre> blocks
const INLINE_OK = ["code","span","b","strong","em","i","a","sub","sup","mark"];
const okStart = new RegExp("^/?(?:" + INLINE_OK.join("|") + ")[ >/]", "i");

function balance(html, tag) {
  const open = (html.match(new RegExp("<" + tag + "(?:\\s[^>]*)?>", "gi")) || []).length;
  const close = (html.match(new RegExp("</" + tag + ">", "gi")) || []).length;
  return open - close;
}

function checkPre(html) {
  // find <pre ...> ... </pre> blocks; inside, every "<" must be &lt;, the closing
  // </pre>, or a whitelisted inline tag. Anything else is a likely render-breaker.
  const issues = [];
  const re = /<pre\b[^>]*>([\s\S]*?)<\/pre>/gi;
  let m;
  while ((m = re.exec(html))) {
    const body = m[1];
    for (let i = 0; i < body.length; i++) {
      if (body[i] !== "<") continue;
      const rest = body.slice(i + 1);
      if (okStart.test(rest)) continue;          // <code>, </span>, etc.
      // it's a raw "<" — context for the report
      const ctx = body.slice(Math.max(0, i - 18), i + 18).replace(/\s+/g, " ");
      issues.push(ctx);
    }
  }
  return issues;
}

let pass = 0, problems = [];
for (const slug of EXPECT) {
  const fp = path.join(FRAG, slug + ".html");
  const r = { slug, ok: true, notes: [] };
  if (!fs.existsSync(fp)) { r.ok = false; r.notes.push("MISSING"); problems.push(r); continue; }
  const html = fs.readFileSync(fp, "utf8");
  const words = (html.replace(/<[^>]+>/g, " ").match(/\S+/g) || []).length;
  r.words = words;
  r.h2 = (html.match(/<h2\b/gi) || []).length;
  r.diagrams = (html.match(/class="diagram"/g) || []).length;
  r.tables = (html.match(/<table\b/gi) || []).length;
  r.callouts = (html.match(/class="callout/g) || []).length;
  r.cites = (html.match(/\.(java|scala):\d+/g) || []).length;

  if (words < 1400) { r.ok = false; r.notes.push("THIN(" + words + "w)"); }
  if (!/<h1\b/i.test(html)) { r.ok = false; r.notes.push("no-h1"); }
  if (r.h2 < 4) { r.ok = false; r.notes.push("few-h2(" + r.h2 + ")"); }
  for (const tag of ["div","table","pre","figure","blockquote","dl","ul","ol","thead","tbody"]) {
    const b = balance(html, tag);
    if (b !== 0) { r.ok = false; r.notes.push("unbalanced-" + tag + "(" + b + ")"); }
  }
  const pre = checkPre(html);
  if (pre.length) { r.ok = false; r.notes.push("raw-< in pre x" + pre.length); r.preSamples = pre.slice(0, 4); }
  // stray "&" not forming an entity
  const amp = (html.match(/&(?!#?\w{1,8};)/g) || []).length;
  if (amp > 0) { r.notes.push("loose-& x" + amp); }

  if (r.ok) pass++; else problems.push(r);
  const flag = r.ok ? "ok " : "FAIL";
  console.log(
    `${flag} ${slug.padEnd(24)} ${String(r.words).padStart(5)}w  h2=${r.h2}  dia=${r.diagrams}  tbl=${r.tables}  call=${r.callouts}  cite=${r.cites}` +
    (r.notes.length ? "  [" + r.notes.join(", ") + "]" : "")
  );
}
console.log(`\n${pass}/${EXPECT.length} fragments pass.`);
if (problems.length) {
  console.log("\nPROBLEMS:");
  for (const p of problems) {
    console.log("  " + p.slug + ": " + p.notes.join(", "));
    if (p.preSamples) p.preSamples.forEach(s => console.log("      raw< near: …" + s + "…"));
  }
  process.exitCode = 1;
}
