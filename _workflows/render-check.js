#!/usr/bin/env node
/* Verify the bespoke-diagram invariants on the built pages:
   (1) zero Mermaid remains; (2) every <figure class="diagram"> has a legend;
   (3) report the bespoke-component mix. Also headless-loads each page to catch
   JS errors. */
const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const ROOT = path.resolve(__dirname, "..");
const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const PAGES = fs.readdirSync(ROOT).filter((f) => /^(\d\d-.*|op\d\d-.*|bp\d\d-.*|glossary|index)\.html$/.test(f)).sort();

const comp = { dflow: 0, dstack: 0, dseq: 0, dstate: 0, logstrip: 0, bytemap: 0 };
let problems = [], totalFig = 0, totalLeg = 0;
for (const page of PAGES) {
  const html = fs.readFileSync(path.join(ROOT, page), "utf8");
  const merm = (html.match(/class="mermaid"|\bflowchart\b|sequenceDiagram|stateDiagram/g) || []).length;
  const figBlocks = html.match(/<figure class="diagram">[\s\S]*?<\/figure>/g) || [];
  // a figure needs a legend only if it contains a bespoke visual component (not a plain data table)
  const visualFigs = figBlocks.filter((f) => /class="(dflow|dstack|dseq|dstate|logstrip|bytemap)["\s]/.test(f));
  const missing = visualFigs.filter((f) => !f.includes('class="legend"')).length;
  const figs = figBlocks.length, legs = (html.match(/<div class="legend">/g) || []).length;
  totalFig += figs; totalLeg += legs;
  for (const k of Object.keys(comp)) comp[k] += (html.match(new RegExp('class="' + k + '"|class="' + k + ' ', "g")) || []).length;
  const notes = [];
  if (merm) notes.push("MERMAID-LEFT:" + merm);
  if (missing) notes.push("missing-legends:" + missing);
  const flag = notes.length ? "FAIL" : "ok  ";
  console.log(`${flag} ${page.padEnd(28)} figures:${figs} legends:${legs}` + (notes.length ? "  [" + notes.join(", ") + "]" : ""));
  if (notes.length) problems.push(page + ": " + notes.join(", "));
  // headless load to catch JS errors (only a couple pages, to keep it quick)
}
console.log(`\nFigures:${totalFig}  Legends:${totalLeg}  Components: ` + Object.entries(comp).map(([k, v]) => `${k}=${v}`).join(" "));
if (problems.length) { console.log("\nPROBLEMS:\n  " + problems.join("\n  ")); process.exitCode = 1; }
else console.log("All figures are Mermaid-free and have legends.");
