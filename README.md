# Apache Kafka — The Complete Field Guide

A deep, **source-derived** guide to Apache Kafka, in three parts, as a self-contained static
website. It spans the whole system top-to-bottom: how it works inside, how to operate it at
scale, and what the distributed log teaches us as an architectural blueprint.

🔗 **Live:** https://dmitrykrivaltsevich.github.io/kafka-internals/

## The three parts (45 chapters)

- **Part I — Architecture Internals** (23 ch): how Kafka actually works, from the on-disk byte
  layout of a record batch up to the KRaft controller quorum, the coordinators, and the clients.
- **Part II — Operations Manual** (14 ch): limits, configuration, capacity & partition sizing,
  performance/durability tuning, failure runbooks, the signals to watch, cost, multitenancy &
  isolation, proactive (leading-indicator) monitoring, and what changes at 1M → 10M → 100M events/s.
- **Part III — The Log as a Blueprint** (8 ch): Kafka as one implementation of the distributed-log
  pattern — when to choose it, its inherent tradeoffs, the reusable engineering tactics, the
  evolution case studies, and the comparative design space.

## How to read it

Open **[`index.html`](index.html)** in any browser, or visit the live link above. No server,
build step, or internet connection required — the site is fully static and dependency-free
(sidebar nav, on-page TOC, theme toggle, code-copy, and all diagrams are vanilla JS/CSS; **no
Mermaid, no CDN**). All ~200 diagrams are hand-built components with a legend on every figure.

## Provenance & accuracy

- **Source:** Apache Kafka `4.4.0-SNAPSHOT`, git commit `04bfe7d` (2026-06-15), **KRaft mode**.
- Every concrete claim is grounded in the actual Java/Scala source and cited as `path/File.ext:line`.
  The content is **derived from the code** (and, for empirical/operational numbers, from cited
  benchmarks and KIPs) — **not copied** from the official documentation.
- The Apache source itself is **not** vendored here (see `.gitignore`); clone it to follow the
  citations: `git clone --depth 1 https://github.com/apache/kafka.git kafka-source`.

## Repository layout

```
index.html              generated landing page
NN-*.html / op*/bp*.html the 46 chapter pages (generated — do not hand-edit; edit the fragment)
assets/                 style.css · app.js · manifest.js (the single source of truth for nav)
_fragments/             per-chapter HTML content fragments  ← the editable source of the pages
_research/              design-rationale & operations/blueprint reference notes
_workflows/             build, QA-lint, render-check, and the authoring/orchestration scripts
samples/                preview screenshots
```

## Rebuilding

Pages are assembled from `_fragments/` by a small Node build step (no dependencies):

```sh
node _workflows/build.js          # wrap fragments in the shared skeleton + regenerate index.html
node _workflows/render-check.js   # QA: assert zero Mermaid + a legend on every figure
node _workflows/validate.js       # QA: HTML well-formedness, citation density, structure
```

Edit a chapter by editing its `_fragments/<slug>.html`, then re-run `build.js`. Editing
`assets/style.css` or `assets/app.js` restyles the whole site globally.

## License & trademark

- Licensed under the **MIT License** (see [`LICENSE`](LICENSE)).
- **Apache Kafka® is a registered trademark of the Apache Software Foundation.** This is an
  independent, unofficial guide and is **not affiliated with, sponsored by, or endorsed by** the
  Apache Software Foundation.
