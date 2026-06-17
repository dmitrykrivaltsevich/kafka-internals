export const meta = {
  name: 'kafka-math-transparency-fix',
  description: 'Audit and fix cryptic/magic-number capacity math across the quantitative Part II chapters — every constant a labeled assumption, every step shown with units',
  phases: [{ title: 'Fix', detail: 'one agent per quantitative chapter: make all worked math fully followable' }]
};

const OUT  = '/Users/user/projects/_playground/apache-kafka-architecture';
const ROOT = OUT + '/kafka-source';

const RUBRIC =
'You are fixing CRYPTIC, un-followable quantitative math in ONE chapter of a Kafka operations manual. A reader complained, correctly, that capacity numbers appear with NO derivation — e.g. in the scaling chapter: "why divide 1,024 by 10? where did the 10 come from? why does the consumer side dominate? is 8 MB/s an imaginary number? where did 250 MB/s per broker come from? how is 6 brokers = 30 leaders?". Your job: make EVERY worked calculation fully followable so a reader can reproduce the arithmetic themselves. Apply this rubric STRICTLY:\n\n' +
'(1) LABEL EVERY CONSTANT AS AN ASSUMPTION BEFORE IT IS USED. Any number that is not derived from earlier numbers — per-partition produce throughput (~10 MB/s), per-consumer drain rate (~8 MB/s), per-broker usable ingress (~250 MB/s), NIC rate (10 GbE ≈ 1,250 MB/s), the replication+egress amplification (~5×), box RAM (64 GiB), JVM heap (6 GiB), retention days, $/GB, $/partition, etc. — MUST be introduced explicitly in an "Assumptions" block (a small table or a dl) with: the value WITH UNITS, a one-line WHY or SOURCE, and its kind (workload-dependent / hardware / config / a cited Kafka default or a research figure). If a number is illustrative, say so explicitly ("for this worked example we assume X — substitute your measured value"). NO constant may appear mid-calculation without having been introduced first.\n\n' +
'(2) SHOW UNITS AT EVERY STEP, and let them cancel to the result\'s unit. Write "1,024 MB/s ÷ 10 MB/s per partition ≈ 103 partitions", never "1,024 / 10 ≈ 103".\n\n' +
'(3) DERIVE every number that follows from others — show the step. e.g. "usable ingress per broker ≈ NIC ÷ amplification = 1,250 MB/s ÷ 5 ≈ 250 MB/s"; "leaders per broker = partitions ÷ brokers = 180 ÷ 6 = 30".\n\n' +
'(4) STATE WHY each conclusion holds, in one clause. e.g. "the consumer side dominates because the assumed per-consumer drain (8 MB/s) is below the per-partition produce rate (10 MB/s), so consumer parallelism — not produce throughput or disk — sets the partition count".\n\n' +
'(5) Distinguish a REAL Kafka limit/default (cite the source file:line or KIP, or the empirical reference) from an ILLUSTRATIVE workload assumption. Never present an example number as a guarantee.\n\n' +
'(6) Ideal shape: a short "Assumptions" table/list, then a "Derivation" that references those assumptions step by step, then the takeaway. You may keep using the bespoke components, but the math inside must obey the rubric.\n\n' +
'Do NOT change correct conclusions or the chapter\'s structure or its prose beyond what the rubric requires — ONLY make the math transparent (introduce the missing assumptions, show the units, derive the asserted numbers, justify the asserted conclusions). Where a constant is genuinely a planning heuristic, cite where it comes from (this guide\'s op03 / the empirical reference / Jun Rao). \n\n' +
'HTML SAFETY: preserve every bespoke diagram (figure/dflow/dstack/dseq/dstate/logstrip/bytemap) and its <div class="legend">; keep the HTML well-formed; NO Mermaid; escape any literal < > & in code/diagram text as &lt; &gt; &amp;.';

const CHAPTERS = [
  { slug: 'op11-scaling-scenarios', extra:
    'THIS IS THE READER-FLAGGED EXEMPLAR — make it spotless. Fix specifically, across ALL three tiers (1M, 10M, 100M): introduce an explicit Assumptions block per tier (message size; per-partition produce rate ~10 MB/s with source; per-consumer drain rate ~8 MB/s as a measure-yours workload assumption; per-broker NIC e.g. 10 GbE ≈ 1,250 MB/s; the ~5× replication+egress amplification, itself derived from "1 in + 2 replicate-out + 2 consumer-out"; box RAM 64 GiB, heap 6 GiB). Then DERIVE: producer-side partitions = ingress ÷ per-partition-produce; consumer-side partitions = ingress ÷ per-consumer-drain; state WHY consumer dominates (8 < 10); usable ingress/broker = NIC ÷ amplification = 1,250 ÷ 5 ≈ 250 MB/s; brokers = ingress ÷ usable-per-broker, then +1 for N−1; leaders/broker = partitions ÷ brokers (e.g. 180 ÷ 6 = 30). Every "≈" must trace back to a labeled assumption.' },
  { slug: 'op04-capacity-planning', extra:
    'Make each sizing formula explicit (named variables), then a single worked example where every constant is a labeled assumption and every step shows units (ingress = rate × size; replication traffic = ingress × (RF−1); disk = ingress × retention × RF; page-cache working set; broker count). No bare numbers.' },
  { slug: 'op03-partitioning', extra:
    'The partition-count math: state partitions = max(T/Tp, T/Tc) with Tp (per-partition produce) and Tc (per-consumer drain) introduced as labeled assumptions with their values, units and source; show the division with units; label the cost figures (5 ms/partition election, etc.) with their source (the empirical reference).' },
  { slug: 'op10-cost', extra:
    'The cost arithmetic: every rate ($/GB-month storage, $/GB cross-AZ, $/partition) must be a labeled assumption with a source/"illustrative, check your cloud bill"; every worked $ figure derived step by step with units (storage = ingress × retention × RF × $/GB; cross-AZ = traffic × $/GB).' },
  { slug: 'op14-proactive-monitoring', extra:
    'Any runway/headroom formula (days-to-full = free_bytes ÷ (ingress_per_day × RF); headroom % = 1 − used/ceiling) must be shown as a formula with units and one worked example with labeled inputs.' },
  { slug: 'op02-limits', extra:
    'The limit derivations must show their steps: e.g. per-broker partition ceiling ≈ vm.max_map_count ÷ (memory-maps per partition) = 65,530 ÷ 2 ≈ 32,765 (cite the source); and the "emergent" topic throughput = partitions × per-partition ceiling — show it as a product with units, not an assertion.' },
  { slug: 'op06-durability', extra:
    'Any durability arithmetic (the RF=3 / min.insync.replicas=2 reasoning, tolerated failures = RF − min.isr, any probability-of-loss sketch) must show the reasoning as explicit steps, not asserted numbers.' }
];

const SCHEMA = {
  type: 'object',
  properties: {
    chapter: { type: 'string' },
    crypticPassagesFound: { type: 'array', items: { type: 'object', properties: {
      location: { type: 'string' }, problem: { type: 'string' } }, required: ['location', 'problem'] } },
    passagesFixed: { type: 'number' },
    assumptionsBlocksAdded: { type: 'number' },
    htmlOk: { type: 'boolean' }
  },
  required: ['chapter', 'crypticPassagesFound', 'passagesFixed', 'htmlOk']
};

function prompt(c) {
  const frag = OUT + '/_fragments/' + c.slug + '.html';
  return RUBRIC + '\n\n=== YOUR CHAPTER ===\nFragment to edit IN PLACE (Read it, then Edit; for large rewrites Read then Write back): ' + frag + '\n' +
    'Repository (for citing real defaults/limits, if needed): ' + ROOT + '\nEmpirical reference for planning numbers: ' + OUT + '/_research/ops-blueprint-reference.md\n\n' +
    'CHAPTER-SPECIFIC FOCUS: ' + c.extra + '\n\n' +
    'Audit the whole chapter for any calculation that violates the rubric (magic numbers, bare divisions, asserted-not-derived results, un-stated assumptions, unjustified conclusions), and rewrite those passages to comply. Return the structured summary — list every cryptic passage you found (so we can report where the problem existed) and how many you fixed.';
}

async function withRetry(items, fn, rounds) {
  rounds = rounds || 3;
  let out = await parallel(items.map((it) => () => fn(it)));
  for (let r = 1; r <= rounds; r++) {
    const todo = items.filter((it, i) => out[i] == null);
    if (!todo.length) break;
    log('Retry round ' + r + ' for ' + todo.length + ' chapter(s).');
    const redo = await parallel(todo.map((it) => () => fn(it)));
    todo.forEach((it, k) => { out[items.indexOf(it)] = redo[k]; });
  }
  return out;
}

phase('Fix');
const results = await withRetry(CHAPTERS, (c) => agent(prompt(c), {
  label: 'mathfix:' + c.slug, phase: 'Fix', schema: SCHEMA, agentType: 'general-purpose'
}));

const ok = results.filter(Boolean);
const total = ok.reduce((a, r) => a + (r.passagesFixed || 0), 0);
log('Math-transparency fix: ' + ok.length + '/' + CHAPTERS.length + ' chapters; ' + total + ' passages fixed.');
return results;
