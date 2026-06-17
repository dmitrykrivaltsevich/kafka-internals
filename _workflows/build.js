#!/usr/bin/env node
/* Build static site: wrap _fragments/<slug>.html in the shared skeleton ->
   <slug>.html at the site root, and generate index.html. Page metadata and the
   3-part navigation come from the single source of truth, assets/manifest.js. */
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const FRAG = path.join(ROOT, "_fragments");
const M = require(path.join(ROOT, "assets", "manifest.js"));
const META = M.PAGES, PARTS = M.PARTS, PROVENANCE = M.PROVENANCE;

function esc(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); }

function skeleton(slug, m, fragment) {
  return `<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${esc(m.title)} · Kafka Internals</title>
<meta name="description" content="${esc(m.desc)}">
<link rel="stylesheet" href="assets/style.css">
</head>
<body data-page="${slug}">
<header class="topbar">
  <button id="menu-toggle" class="icon menu-toggle" aria-label="Open navigation">☰</button>
  <a class="brand" href="index.html"><span class="logo">K</span><span>Kafka Internals</span><small>4.4</small></a>
  <span class="spacer"></span>
  <button id="theme-toggle" class="icon" aria-label="Toggle theme">☽</button>
</header>
<div class="scrim"></div>
<div class="layout">
  <aside class="sidebar" id="sidebar"></aside>
  <main class="content">
    <div class="content-inner">
      <article>
${fragment}
      </article>
      <footer class="footer-meta">
        <p>Part of <strong>Apache Kafka — The Complete Field Guide</strong> · derived from Apache Kafka 4.4 source · <a href="https://github.com/dmitrykrivaltsevich/kafka-internals">GitHub</a> · MIT-licensed.</p>
        <p>Apache Kafka® is a registered trademark of the Apache Software Foundation. This is an independent, unofficial guide — not affiliated with or endorsed by the ASF.</p>
      </footer>
    </div>
  </main>
  <aside class="toc" id="toc"></aside>
</div>
<script src="assets/manifest.js"></script>
<script src="assets/app.js"></script>
</body>
</html>
`;
}

function indexPage() {
  let body = `      <div class="hero">
        <h1>Apache Kafka — The Complete Field Guide</h1>
        <p>A deep, source-derived guide to Apache Kafka in three parts: how it works inside, how to operate it at scale, and what the distributed log teaches us as an architectural blueprint.</p>
        <div class="hero-meta">
          <span class="pill">${PROVENANCE}</span>
          <span class="pill">Derived from source · not copied from official docs</span>
          <span class="pill">3 parts · ${Object.keys(META).length} chapters</span>
        </div>
      </div>
      <p class="lead">New here? Start with the <a href="00-overview.html">Architecture Overview</a>. Operating a cluster? Jump to <a href="op00-operator-model.html">Part II — Operations</a>. Designing a system? See <a href="bp00-log-pattern.html">Part III — The Log as a Blueprint</a>.</p>
`;
  const blurb = {
    "I": "How Kafka actually works inside — from the on-disk byte layout of a record batch up to the KRaft controller quorum, the coordinators, and the client runtimes.",
    "II": "How to run it: limits, tuning, capacity & partition sizing, failure runbooks, the signals to watch, cost, and what changes at 1M / 10M / 100M events per second.",
    "III": "Kafka as one implementation of the distributed-log pattern — when to choose it, its inherent tradeoffs, the reusable engineering tactics, and the design space."
  };
  for (const part of PARTS) {
    body += `      <div class="part-head"><h2>${esc(part.title)}</h2><p>${esc(blurb[part.id] || "")}</p></div>\n`;
    for (const g of part.groups) {
      body += `      <div class="section-label">${esc(g.title)}</div>\n      <div class="card-grid">\n`;
      for (const slug of g.items) {
        const m = META[slug];
        if (!m) continue;
        const num = m.num === "—" ? "REF" : m.num;
        body += `        <a class="card" href="${slug}.html"><div class="card-num">${esc(num)}</div><div class="card-title">${esc(m.title)}</div><div class="card-desc">${esc(m.desc)}</div></a>\n`;
      }
      body += `      </div>\n`;
    }
  }
  return skeleton("index", { title: "Apache Kafka — The Complete Field Guide", desc: "A deep, source-derived guide to Apache Kafka: architecture internals, operations, and the log as an architectural blueprint." }, body)
    .replace('data-page="index"', 'data-page=""');
}

// --- build ---
let built = 0, missing = [];
for (const slug of Object.keys(META)) {
  const m = META[slug];
  const fpath = path.join(FRAG, slug + ".html");
  let fragment;
  if (fs.existsSync(fpath)) {
    fragment = fs.readFileSync(fpath, "utf8").trim();
    built++;
  } else {
    // placeholder so every nav link resolves while the chapter is still being written
    fragment = `<h1>${esc(m.num + " · " + m.title)}</h1>
<blockquote class="provenance">Apache Kafka 4.4.0-SNAPSHOT · this chapter is in preparation.</blockquote>
<div class="callout note"><span class="callout-title">Coming soon</span><p>${esc(m.desc)}</p></div>`;
    missing.push(slug);
  }
  fs.writeFileSync(path.join(ROOT, slug + ".html"), skeleton(slug, m, fragment));
}
fs.writeFileSync(path.join(ROOT, "index.html"), indexPage());
console.log(`Built ${built} pages + index.html` + (missing.length ? `  (not yet authored: ${missing.length})` : ""));
if (missing.length) console.log("Missing fragments: " + missing.join(", "));
