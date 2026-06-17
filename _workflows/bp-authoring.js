export const meta = {
  name: 'kafka-blueprint-authoring',
  description: 'Author Part III (The Log as a Blueprint): 8 architect-manual chapters in the bespoke-diagram HTML system, each reviewed',
  phases: [
    { title: 'Author', detail: 'one agent per chapter: pattern-focused, tradeoff analysis, bespoke diagrams + legends' },
    { title: 'Verify', detail: 'review for accuracy of Kafka mechanism claims + HTML/diagram invariants' }
  ]
};

const ROOT = '/Users/user/projects/_playground/apache-kafka-architecture/kafka-source';
const OUT  = '/Users/user/projects/_playground/apache-kafka-architecture';
const RESEARCH = OUT + '/_research/ops-blueprint-reference.md';

const XLINKS =
'Cross-link by filename. Part I: 00-overview, 01-record-format, 02-wire-protocol, 03-storage-log-engine, 04-storage-management, 05-tiered-storage, 06-network-and-threading, 07-request-processing, 08-replication, 09-fetch-path, 10-kraft-consensus, 11-kraft-controller, 12-metadata-propagation, 13-group-coordination, 14-transactions-eos, 15-share-groups, 16-producer-client, 17-consumer-client, 18-security, 19-quotas, 20-kafka-streams, 21-kafka-connect, glossary. ' +
'Part II: op00-operator-model, op01-configuration, op02-limits, op03-partitioning, op04-capacity-planning, op05-performance-tuning, op06-durability, op07-failure-modes, op08-metrics-signals, op09-topologies, op10-cost, op11-scaling-scenarios, op12-lifecycle. ' +
'Part III (this part): bp00-log-pattern, bp01-when-to-use, bp02-design-decisions, bp03-inherent-limits, bp04-tactics-toolkit, bp05-evolution, bp06-comparative, bp07-architect-cheatsheet.';

const HOUSE =
'You are a distinguished software architect writing Part III — THE LOG AS A BLUEPRINT — the architect\'s manual of a high-end Apache Kafka book-site. Apache Kafka 4.4 is the running example, but the FOCUS is the architectural PATTERN it implements: the distributed, partitioned, replicated commit log. The audience is engineers and architects designing systems; the goal is to teach WHEN this architecture (and each subsystem\'s design) is the right choice, the tradeoffs and tunable space, where it INHERENTLY falls short, and the reusable engineering tactics they can carry to other systems. Treat Kafka as a blueprint, not just a product.\n\n' +
'GROUNDING:\n' +
'1. The ARCHITECTURE — when you assert a Kafka mechanism, it must be REAL. Cross-link the Part I chapter that documents it (those chapters are already source-verified); you may also read source at ' + ROOT + ' to confirm a specific claim, but this part is analytical, not line-citation-heavy.\n' +
'2. The EMPIRICAL/COMPARATIVE REFERENCE at ' + RESEARCH + ' — read it for the comparative-systems facts (Pulsar/Redpanda/Kinesis/Pub-Sub/RabbitMQ/WarpStream), critiques, and the design-space material; cite the source named there.\n\n' +
'BE OPINIONATED BUT FAIR. The value of this part is honest tradeoff analysis: give the forces on both sides, name where the log / Kafka is the WRONG choice, and distinguish what is INHERENT (structural) from what is MITIGATED by a feature or TUNABLE by a config. Avoid marketing; avoid hand-waving. Every "this is good because" and "this falls short because" must trace to a concrete mechanism or tradeoff (cross-link it).\n\n' +
'Make it genuinely useful and transferable — an engineer should finish each chapter with sharper judgement and reusable tactics. Aim for 3,000–6,000 words of dense, original, analytical content per chapter.';

const COMPONENTS =
'OUTPUT = an HTML FRAGMENT for inside <article> (NO <html>/<head>/<body>/<nav>/<script>). Begin with <h1> "III · NN · Title" (use the provided display number), then <blockquote class="provenance">Source: Apache Kafka 4.4.0-SNAPSHOT (git 04bfe7d, 2026-06-15), KRaft mode. Architectural analysis grounded in the source-verified Part I and cited comparative sources.</blockquote>, then <p class="lead">…abstract…</p>, then the body with <h2>/<h3> (no id attributes).\n\n' +
'TEXT: <p>, <ul>/<ol>/<li>, <strong>, <code>. Cross-links <a href="bp02-design-decisions.html">…</a> / to Part I <a href="08-replication.html">…</a> / Part II <a href="op03-partitioning.html">…</a>. KIP badges <span class="pill kip">KIP-500</span>.\n' +
'TABLES (use heavily for tradeoff matrices & comparisons): <div class="table-wrap"><table>…</table></div>.\n' +
'CALLOUTS: <div class="callout key|rationale|invariant|gotcha|warning|note"><span class="callout-title">…</span><p>…</p></div> — "rationale" for the design WHY, "key" for the takeaway/tactic, "warning"/"gotcha" for where it falls short.\n' +
'BESPOKE DIAGRAMS — NO Mermaid, NO ASCII art. Every diagram is a <figure class="diagram">…</figure> and MUST end with a <div class="legend">. Semantic colours: cat-client (blue), cat-broker (purple), cat-storage (green), cat-control (amber), cat-coord (teal), cat-error (red).\n' +
'  • dflow — DECISION TREES (the workhorse here: "use a log?" / "how many partitions?") and flows. Vertical; add class "h" for horizontal. Nodes <div class="node cat-X"><span class="nt">…</span><span class="nd">…</span></div>; node modifiers "decision"(rounded)/"store"(cylinder)/"pill". Connectors <div class="conn" data-label="yes"></div> ("dashed","ec-data/ec-async/ec-err"). Branches: <div class="row"><div class="col"><div class="conn" data-label="…"></div><div class="node">…</div></div>…</div> (fork manifold auto-drawn).\n' +
'  • dstack — layered/contrast diagrams (e.g. compute/storage separation): <div class="dstack"><div class="layer cat-X"><div class="ln">…</div><div class="ld">…</div></div><div class="sep">↕</div>…</div>\n' +
'  • dseq — protocol/interaction comparisons over time: <div class="dseq" style="--cols:N"><div class="lanes"><span class="actor cat-X">…</span>…</div><div class="body"><div class="lifelines"><i></i>…</div><div class="m r" style="--from:1;--to:2"><span class="ml">…</span><span class="ln"></span></div>…</div></div>\n' +
'  • dstate — state machines / evolution timelines: <div class="dstate"><span class="term"></span><span class="tr"><span class="tl">…</span><span class="ta"></span></span><span class="st">…</span>…<span class="term end"></span></div>\n' +
'  • Prefer a clean <table> for tradeoff matrices and the comparative-systems comparison; <dl class="kv"> for term/definition lists.\n' +
'LEGEND content: explain only the encodings used (swatches <span class="sw cat-X"></span>, arrows <span class="arr"></span>/<span class="arr dashed"></span>, <span class="mono">…</span>).\n' +
'SAFETY: never put a raw < > & inside <pre>/<code>/diagram text — escape as &lt; &gt; &amp; (use Unicode ≥ ≤ → in labels). Well-formed HTML.';

const CH = [
  { n: 'bp00', disp: 'III · 00', slug: 'bp00-log-pattern', title: 'The Distributed Log as a Pattern',
    focus: 'Abstract the pattern from Kafka. Define its essence precisely: an append-only, per-shard totally-ordered, partitioned, replicated, offset-addressed, retention-bounded record sequence that consumers read at a position they own. Its INVARIANTS (records immutable once written; per-partition order; offset a stable address; producers/consumers decoupled in time and space). The universal problems it solves: temporal decoupling (a durable buffer absorbing rate mismatches), spatial decoupling (fan-out to many independent readers at zero marginal broker cost), REPLAY (the offset is a time machine — reprocess history), ordering, durability, and serving as the integration backbone (collapsing O(N²) point-to-point pipelines into an O(N) hub-and-spoke — Kreps, "The Log"). What distinguishes the log from a QUEUE (consumed-once, broker-tracked) and from a DATABASE (mutable, queryable, current-state). Why it is a PATTERN (a recurring solution) and where the same primitive appears elsewhere: database write-ahead/redo logs, state-machine replication (Raft/Paxos), event sourcing, even blockchains. Ground in Kafka\'s realization but stay at the abstraction.',
    files: 'Cross-ref Part I 00,03,08; Part II op03. Read the empirical reference\'s log-pattern section (Kreps, Kleppmann).' },
  { n: 'bp01', disp: 'III · 01', slug: 'bp01-when-to-use', title: 'When to Use the Log — and When Not To',
    focus: 'A rigorous decision framework. The FORCES THAT FAVOR a log: high sustained throughput; the need to replay/reprocess; multiple independent consumers (fan-out); durable buffering and backpressure absorption; per-key ordering; event-driven decoupling; an auditable source of truth. The FORCES AGAINST: low-latency request/response (the log adds replication + batching latency — the floor); per-message routing / priority / selective consumption (the log is sequential and "dumb" — share groups KIP-932 only partially address this, cross-ref 15-share-groups); per-message TTL/expiry; multi-entity transactional updates; tiny scale (operational overhead unjustified); and point queries / random access (a log is not an index). The ANTI-PATTERNS, each with its concrete failure mode tied to a mechanism: Kafka-as-database (no random read/update/query; retention deletes your data; compaction is not a query engine); Kafka-as-RPC (request/reply over a log is high-latency and awkward); Kafka-as-priority/task-queue (no per-message ack, priority, or visibility timeout in classic consumer groups — and what share groups change). Deliver a real DECISION TREE (a dflow) and a forces table.',
    files: 'Cross-ref Part I 14,15,08; Part II op02,op03,op06; the empirical reference (log-pattern, comparative).' },
  { n: 'bp02', disp: 'III · 02', slug: 'bp02-design-decisions', title: 'Design Decisions & Their Alternatives',
    focus: 'The core architect chapter: each major Kafka design as a deliberate choice in a tradeoff space, WITH the roads not taken. (1) PULL vs PUSH consumers (consumer-controlled rate/replay/backpressure vs lower latency but overwhelm risk). (2) LEADER-BASED ISR replication vs leaderless/quorum: ISR (every in-sync replica acks; leader serves) gives strong consistency tolerating f failures with f+1 replicas and a tunable latency/durability dial, vs Dynamo-style sloppy quorum (W+R>N, eventual) or Paxos/Raft majority. AND THE DELIBERATE SPLIT — Kafka uses ISR for DATA partitions but a Raft majority quorum for the METADATA log (KRaft): explain WHY this is the right split (data wants high throughput, huge partition counts, and a tunable durability dial; metadata wants a small, always-strongly-consistent, self-managed, fast-failover quorum) — the single best architectural lesson in Kafka. (3) PARTITION as the unit of parallelism+ordering vs consistent-hashing / range sharding. (4) CONSUMER-TRACKED offsets vs broker-tracked acknowledgement (the "dumb broker, smart client" — cheap fan-out & replay, state pushed to clients). (5) PAGE-CACHE + zero-copy reliance vs managed memory / direct IO (mechanical sympathy vs control — contrast Redpanda thread-per-core/DMA). (6) APPEND-ONLY SEGMENTS vs LSM-tree / B-tree (sequential-write throughput vs random read/update). (7) SINGLE-WRITER (leader) per partition vs multi-writer. (8) COORDINATORS AS REPLICATED LOGS. For each: the alternatives, the tradeoff axes, when each is right. Use tradeoff tables + a dstack/dseq contrast where it helps.',
    files: 'Cross-ref Part I 08,09,10,11,03,13,16,17; Part II op06; the empirical reference (comparative).' },
  { n: 'bp03', disp: 'III · 03', slug: 'bp03-inherent-limits', title: 'Inherent Limitations',
    focus: 'Where the pattern STRUCTURALLY falls short — limits that follow from the design, not bugs — and an honest INHERENT vs MITIGATED vs TUNABLE classification for each. (1) Ordering only per partition (global order ⇒ one partition ⇒ no parallelism). (2) The PARTITION-COUNT scaling ceiling (parallelism quantized; consumers capped at partition count; repartitioning breaks keyed order — cross-ref op03). (3) No per-message routing/priority/TTL/selective consumption (sequential dumb log; share groups KIP-932 partially lift it). (4) Per-partition HEAD-OF-LINE blocking (one poison/slow record stalls the partition for a consumer). (5) The LATENCY FLOOR (ISR replication + batching ⇒ ms-scale minimum; not sub-ms request/reply). (6) All-or-nothing topic retention (no per-record TTL except compaction tombstones). (7) No server-side filtering/query/secondary index (read-everything-and-filter; wasteful for sparse needs). (8) Complexity pushed to CLIENTS (fat clients, rebalance complexity, the smart-client burden). (9) Rebalance disruption (historically stop-the-world; cooperative/KIP-848 mitigate). (10) Cross-AZ replication cost (failure-independent replicas vs cloud network billing — cross-ref op10). (11) The metadata-as-a-single-log scaling bound. Be candid; this honesty is the chapter\'s worth.',
    files: 'Cross-ref Part I 08,13,14,15,09; Part II op02,op03,op10; the empirical reference (critique).' },
  { n: 'bp04', disp: 'III · 04', slug: 'bp04-tactics-toolkit', title: 'The Tactics Toolkit',
    focus: 'The reusable engineering tactics Kafka embodies, each generalized into a tool the reader can apply elsewhere — the most practically valuable chapter. For each: the principle, how Kafka uses it (cross-link Part I), and where it applies beyond Kafka. (1) MECHANICAL SYMPATHY (sequential IO + OS page cache + zero-copy/sendfile + batching — ride the hardware; 03). (2) BATCHING to amortize fixed per-op costs (01,16). (3) THE LOG AS A COORDINATION PRIMITIVE — "everything is a replicated log": data, __consumer_offsets, __transaction_state, __cluster_metadata, __share_group_state reuse the same mechanism (12,13,14). (4) IDEMPOTENCE via (producerId, epoch, sequence) dedup (14). (5) EPOCH-FENCING as a universal anti-zombie tactic — monotonic counters (leader epoch, producer epoch, partition epoch, member epoch) that fence stale actors (08,13,14). (6) SINGLE-THREADED EVENT LOOP + MVCC/TIMELINE data structures (one writer thread over copy-on-write timeline maps for lock-free consistent reads; 11). (7) OPTIMISTIC CONCURRENCY via epoch compare-and-set (AlterPartition; 08). (8) IMMUTABLE IMAGE + DELTA (fold deltas onto an immutable snapshot; 12). (9) PURGATORY + HIERARCHICAL TIMING WHEELS for O(1) delayed-operation scheduling (06). (10) THE WATERMARK (a committed-boundary pointer; 08). (11) PULL-BASED BACKPRESSURE. (12) THE IN-SYNC SET as a tunable consistency dial. (13) FEATURE-FLAGGING A DISTRIBUTED SYSTEM (metadata.version gates behaviour safely across versions; 12). (14) SPI/PLUGGABILITY (Authorizer, RemoteStorageManager, assignors). Present as a toolkit with a summary table (tactic | Kafka use | applies-elsewhere).',
    files: 'Cross-ref Part I 03,06,08,11,12,13,14,16; Part III bp05.' },
  { n: 'bp05', disp: 'III · 05', slug: 'bp05-evolution', title: 'Architectural Evolution as Case Studies',
    focus: 'Kafka\'s evolution as case studies in evolving a large distributed system. Each: the problem, the change, the architectural lesson. (1) ZooKeeper → KRaft (KIP-500): replacing an external dependency with self-managed consensus — cost (build Raft) vs benefit (one system, hot-standby failover, scale); the lesson on owning your coordination plane. (2) MESSAGE FORMAT v0→v2 (KIP-98): evolving an on-disk/on-wire format (batching, relative offsets, idempotence fields) with backward compatibility and down-conversion. (3) EXACTLY-ONCE (KIP-98/447): layering strong guarantees on an at-least-once core via idempotence + transactions without sacrificing throughput. (4) REBALANCE eager → cooperative (KIP-429) → broker-driven (KIP-848): incrementally removing stop-the-world disruption. (5) TIERED STORAGE (KIP-405): decoupling retention from local disk via an SPI. (6) QUEUES / share groups (KIP-932): adding a new consumption model without disturbing the log. THE META-LESSONS: feature flags (metadata.version) for safe evolution; SPIs to extend without forking; backward compatibility as a hard constraint; absorb-vs-externalize complexity; multi-release deprecation discipline. A timeline (dstate) + lessons table. Cross-ref Part I.',
    files: 'Cross-ref Part I 10,11,12,01,14,13,15,05; the empirical reference.' },
  { n: 'bp06', disp: 'III · 06', slug: 'bp06-comparative', title: 'Comparative Architecture & the Design Space',
    focus: 'Map the design space by comparing Kafka to the alternatives. For each: model, storage, ordering/consistency, the KEY tradeoff vs Kafka, and what it teaches. (1) Apache PULSAR: compute/storage separation (stateless brokers + Apache BookKeeper segment store, + metadata store) → independent scaling and fast topic rebalance, at the cost of operating a second stateful system. (2) REDPANDA: C++ thread-per-core (Seastar), no JVM, no OS-page-cache reliance (own IO scheduling) — Kafka-protocol compatible → lower tail latency, fewer nodes, at the cost of the JVM ecosystem; teaches the mechanical-sympathy ceiling. (3) AWS KINESIS / Google PUB/SUB: fully managed; shard/stream (Kinesis) vs part-less push (Pub/Sub) → ops-free elasticity at the cost of control, hard limits, and lock-in. (4) RabbitMQ / traditional MQ: broker-tracked per-message delivery with flexible routing/priority/TTL, but not a replayable high-throughput log → the queue-vs-log axis. (5) WARPSTREAM / DISKLESS (KIP-1150): object-storage-native, zero local disk, leaderless → drastically cheaper storage/cross-AZ at the cost of higher latency; the emerging frontier. Deliver a strong comparison TABLE (system | model | storage | ordering | key tradeoff) and a "where Kafka sits and why" synthesis. Ground in the empirical/comparative reference.',
    files: 'The empirical reference comparative section (primary); cross-ref Part I 05,08,10 and Part II op10 for Kafka\'s positions.' },
  { n: 'bp07', disp: 'III · 07', slug: 'bp07-architect-cheatsheet', title: "The Architect's Cheat Sheet",
    focus: 'The capstone — a consolidated, mostly-visual takeaway card pulling Parts II and III together. (1) THE DESIGN DIALS table: each dial, its range, what it trades — consistency (min.insync.replicas/acks), durability (replication.factor), latency (batch.size/linger.ms), ordering scope (partitions & keys), throughput (partition count), cost (RF/retention/tiered/compression), availability (unclean.leader.election). (2) THE DECISION TREES as compact dflow diagrams: "should I use a log?" (from bp01), "how many partitions?" (from op03), "what RF / min.insync.replicas?" (from op06), "how do I scale to N?" (from op11). (3) THE HEURISTICS TABLE: the rules of thumb gathered across both parts, each with its one-line "why". (4) THE TACTICS quick-list (from bp04). (5) A single "choosing & tuning a log-based architecture" reference panel. Lead with the diagrams and tables; minimal prose. This is the page an architect bookmarks.',
    files: 'Synthesizes Part II op02,op03,op06,op11 and Part III bp01,bp03,bp04. Cross-link them.' }
];

const AUTHOR_SCHEMA = {
  type: 'object',
  properties: {
    fragmentPath: { type: 'string' }, title: { type: 'string' }, abstract: { type: 'string' },
    sections: { type: 'array', items: { type: 'string' } },
    keyTakeaways: { type: 'array', items: { type: 'string' } },
    diagrams: { type: 'number' }, legends: { type: 'number' }, approxWords: { type: 'number' }
  },
  required: ['fragmentPath', 'title', 'abstract', 'sections', 'keyTakeaways']
};

const VERIFY_SCHEMA = {
  type: 'object',
  properties: {
    slug: { type: 'string' }, accuracyScore: { type: 'number' },
    htmlOk: { type: 'boolean' }, mermaidRemaining: { type: 'number' }, legendsOk: { type: 'boolean' },
    errors: { type: 'array', items: { type: 'object', properties: {
      severity: { type: 'string', enum: ['critical', 'major', 'minor'] },
      claim: { type: 'string' }, correction: { type: 'string' } }, required: ['severity', 'claim', 'correction'] } },
    verdict: { type: 'string', enum: ['publishable', 'minor-fixes', 'needs-rework'] }
  },
  required: ['slug', 'accuracyScore', 'errors', 'verdict']
};

function authorPrompt(s) {
  const out = OUT + '/_fragments/' + s.slug + '.html';
  return HOUSE + '\n\n' + COMPONENTS + '\n\n' + XLINKS + '\n\n=== YOUR CHAPTER ===\n' +
    'h1 text: "' + s.disp + ' · ' + s.title + '".\nWrite the complete HTML fragment to: ' + out + '\n\n' +
    'SCOPE & WHAT THIS CHAPTER MUST DELIVER:\n' + s.focus + '\n\n' + 'GROUNDING POINTERS: ' + s.files + '\n\n' +
    'Write the dense, analytical, honest chapter. Every Kafka-mechanism claim real (cross-link Part I); every comparative fact cited to the reference; tradeoffs given on both sides; inherent-vs-tunable made explicit. Bespoke diagrams (especially decision trees) with legends. Return the structured summary.';
}

function verifyPrompt(s) {
  const out = OUT + '/_fragments/' + s.slug + '.html';
  return 'Review this Apache Kafka architectural-blueprint chapter at ' + out + '. Check: every Kafka MECHANISM claim is real and correctly described (verify against the Part I chapters\' subject matter and, if needed, source under ' + ROOT + '); comparative-systems facts (Pulsar/Redpanda/Kinesis/PubSub/RabbitMQ/WarpStream) are accurate and attributed; tradeoffs are presented fairly with inherent-vs-tunable distinctions; nothing treats ZooKeeper as current (KRaft-only); no invented mechanisms. Also HTML: no Mermaid/ASCII diagrams remain, every <figure class="diagram"> has a <div class="legend">, well-formed. Report concrete errors with corrections. Score 0-100; verdict publishable / minor-fixes / needs-rework; set mermaidRemaining and legendsOk.';
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

phase('Author');
const authored = await withRetry(CH, (s) => agent(authorPrompt(s), {
  label: 'author:' + s.slug, phase: 'Author', schema: AUTHOR_SCHEMA, agentType: 'general-purpose'
}));

phase('Verify');
const results = await withRetry(CH, (s, i) => {
  if (!authored[i]) return Promise.resolve({ slug: s.slug, failed: true });
  return agent(verifyPrompt(s), { label: 'verify:' + s.slug, phase: 'Verify', schema: VERIFY_SCHEMA, agentType: 'general-purpose' })
    .then((v) => ({ slug: s.slug, authored: authored[i], verify: v }));
});

const ok = authored.filter(Boolean).length;
log('Part III authored: ' + ok + '/' + CH.length + ' chapters.');
return results;
