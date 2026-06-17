export const meta = {
  name: 'kafka-arch-finalize',
  description: 'Apply source-verified fixes to each chapter, then synthesize the overview and glossary',
  phases: [
    { title: 'Fix', detail: 'one agent per chapter applies the verified corrections + render fixes' },
    { title: 'Synthesize', detail: 'author the top-level overview and the glossary from chapter briefs' }
  ]
};

const ROOT = '/Users/user/projects/_playground/apache-kafka-architecture/kafka-source';
const OUT  = '/Users/user/projects/_playground/apache-kafka-architecture';

const CHAPTERS = [
  { n: '01', slug: 'record-format' }, { n: '02', slug: 'wire-protocol' }, { n: '03', slug: 'storage-log-engine' },
  { n: '04', slug: 'storage-management' }, { n: '05', slug: 'tiered-storage' }, { n: '06', slug: 'network-and-threading' },
  { n: '07', slug: 'request-processing' }, { n: '08', slug: 'replication' }, { n: '09', slug: 'fetch-path' },
  { n: '10', slug: 'kraft-consensus' }, { n: '11', slug: 'kraft-controller' }, { n: '12', slug: 'metadata-propagation' },
  { n: '13', slug: 'group-coordination' }, { n: '14', slug: 'transactions-eos' }, { n: '15', slug: 'share-groups' },
  { n: '16', slug: 'producer-client' }, { n: '17', slug: 'consumer-client' }, { n: '18', slug: 'security' },
  { n: '19', slug: 'quotas' }, { n: '20', slug: 'kafka-streams' }, { n: '21', slug: 'kafka-connect' }
];

const MANIFEST = [
  '00-overview.html — Architecture Overview', '01-record-format.html — Record Format & Batches',
  '02-wire-protocol.html — Wire Protocol & RPC', '03-storage-log-engine.html — The Log Storage Engine',
  '04-storage-management.html — Log Management, Retention & Compaction', '05-tiered-storage.html — Tiered Storage',
  '06-network-and-threading.html — Network Layer & Threading', '07-request-processing.html — Request Processing (KafkaApis)',
  '08-replication.html — Replication, ISR & High Watermark', '09-fetch-path.html — Fetch Path & Replica Fetchers',
  '10-kraft-consensus.html — KRaft Consensus (Raft)', '11-kraft-controller.html — The KRaft Controller',
  '12-metadata-propagation.html — Metadata Propagation & Broker Lifecycle', '13-group-coordination.html — Group Coordination',
  '14-transactions-eos.html — Transactions & Exactly-Once', '15-share-groups.html — Share Groups (Queues)',
  '16-producer-client.html — The Producer Client', '17-consumer-client.html — The Consumer Client',
  '18-security.html — Security', '19-quotas.html — Quotas & Throttling', '20-kafka-streams.html — Kafka Streams',
  '21-kafka-connect.html — Kafka Connect', 'glossary.html — Glossary & Cross-Cutting Concepts'
].join('\n');

const HTML_RULES =
'OUTPUT = an HTML FRAGMENT for the inside of <article> (NO <html>/<head>/<body>/<nav>/<script>; the build wraps it and auto-builds the sidebar + on-page TOC from your headings). Start with <h1> "NN · Title", then <blockquote class="provenance">Source: Apache Kafka 4.4.0-SNAPSHOT (git 04bfe7d, 2026-06-15), KRaft mode. Derived from source code, not copied from official documentation.</blockquote>, then <p class="lead">…abstract…</p>, then the body using <h2>/<h3> (no id attributes — generated). Building blocks: <p>,<ul>/<ol>/<li>,<strong>,<code>; tables in <div class="table-wrap"><table>…; diagrams in <figure class="diagram"><pre>…</pre><figcaption>…</figcaption></figure>; callouts <div class="callout key|rationale|invariant|gotcha|note"><span class="callout-title">…</span><p>…</p></div>; term grids <dl class="kv"><dt>…</dt><dd>…</dd></dl>; KIP badges <span class="pill kip">KIP-848</span>; cross-links <a href="NN-slug.html">Title</a>. CRITICAL: inside <pre>/<code> use Unicode box-drawing/arrows (─ │ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼ → ← ↑ ↓ ⇄ ● ✓ ✗) and escape any literal < > & as &lt; &gt; &amp;; never leave a raw < in text/pre. Well-formed HTML only.';

async function withRetry(items, fn, rounds) {
  rounds = rounds || 3;
  let out = await parallel(items.map((it) => () => fn(it)));
  for (let r = 1; r <= rounds; r++) {
    const todo = items.filter((it, i) => out[i] == null);
    if (!todo.length) break;
    log('Retry round ' + r + ' for ' + todo.length + ' item(s).');
    const redo = await parallel(todo.map((it) => () => fn(it)));
    todo.forEach((it, k) => { out[items.indexOf(it)] = redo[k]; });
  }
  return out;
}

const FIX_SCHEMA = {
  type: 'object',
  properties: {
    chapter: { type: 'string' },
    issuesHandled: { type: 'number' },
    edits: { type: 'array', items: { type: 'object', properties: {
      severity: { type: 'string' }, location: { type: 'string' }, summary: { type: 'string', description: 'before → after' }
    }, required: ['summary'] } },
    falsePositives: { type: 'array', items: { type: 'object', properties: { claim: { type: 'string' }, why: { type: 'string' } }, required: ['claim', 'why'] } },
    htmlOk: { type: 'boolean' }
  },
  required: ['chapter', 'edits', 'htmlOk']
};

function fixPrompt(ch) {
  const key = ch.n + '-' + ch.slug;
  const frag = OUT + '/_fragments/' + key + '.html';
  return 'You are fixing flagged errors in ONE chapter of source-derived Apache Kafka 4.4.0-SNAPSHOT (KRaft-only) internals documentation. Ground truth is the source code under ' + ROOT + '.\n\n' +
    'Chapter fragment to edit IN PLACE (use Read then Edit): ' + frag + '\n' +
    'The flagged issues for this chapter are the JSON array stored under the key "' + key + '" in this file: ' + OUT + '/_research/verify-errors.json — Read that file and extract YOUR chapter\'s issues.\n\n' +
    'For EACH issue:\n' +
    '1. Locate the claim in the fragment.\n' +
    '2. INDEPENDENTLY CONFIRM the correct fact by reading the actual source (open the file:line in the correction, or grep). The provided correction is a strong hint, but verify against the source yourself; if the source shows something different from the suggested correction, follow the SOURCE.\n' +
    '3. Apply a MINIMAL, surgical Edit that fixes only the wrong claim, preserving the surrounding prose, markup, citations and HTML validity. Do not rewrite or expand the chapter.\n' +
    'For any issue with severity "render": fix unescaped < > & inside <pre>/<code> (escape as &lt; &gt; &amp;) and replace stray & in prose with &amp; — but never double-escape an existing &amp;/&lt;/&gt; entity.\n\n' +
    'If you judge a flagged issue to be a false positive (the document was already correct per source), leave it unchanged and record it under falsePositives with the reason. After editing, double-check the fragment is still well-formed HTML. Return the structured summary of what you changed.';
}

function overviewPrompt() {
  return 'You are writing the flagship TOP-LEVEL ARCHITECTURE OVERVIEW (chapter "00 · Architecture Overview") for a deep, source-derived Apache Kafka 4.4.0-SNAPSHOT (KRaft-only) internals guide. This is the front door of the whole site: a reader should come away with an accurate mental model of how Kafka works end to end, and a map into the 21 detailed chapters.\n\n' +
    HTML_RULES + '\n\n' +
    'INPUTS you should read first:\n' +
    '- ' + OUT + '/_research/chapter-briefs.json — the abstracts, section lists, and ~317 verified keyFacts from all 21 detailed chapters. This is your richest, already-fact-checked source material; build the overview on it.\n' +
    '- ' + OUT + '/_research/design-rationale-and-kips.md — design rationale and KIP history for the "why".\n' +
    'You MAY skim a few real source files under ' + ROOT + ' to ground a specific claim, but you do not need to re-derive details the chapters already established.\n\n' +
    'COVER (top to bottom), with generous ASCII diagrams:\n' +
    '1. What Kafka is: a distributed, partitioned, replicated commit log; the core abstractions (record, partition, topic, offset, broker, the append-only segmented log) and the "dumb broker / smart client, the consumer tracks its own offset" model.\n' +
    '2. Cluster anatomy in KRaft (no ZooKeeper): brokers (data plane) vs the controller quorum (metadata plane); the __cluster_metadata Raft log as the single source of truth; how nodes can be broker, controller, or both.\n' +
    '3. The control plane vs data plane split, and the metadata propagation backbone (controller writes records → Raft commit → brokers replay the MetadataImage).\n' +
    '4. The end-to-end DATA PATH for a record: producer batching → ProduceRequest to the partition leader → append to the log → replication to followers (ISR) → high-watermark commit (acks) → consumer fetch → group offset commit. Draw this as one big diagram and narrate each hop, linking the relevant chapter.\n' +
    '5. The storage model in one screen: segments, indexes, page cache, zero-copy, retention vs compaction, tiered storage.\n' +
    '6. Coordination & semantics: consumer groups & rebalancing, exactly-once/transactions, share groups (queues); delivery & ordering guarantees (at-least-once vs exactly-once, per-partition ordering, the HW/LSO read boundaries).\n' +
    '7. The threading model of a broker at a glance (acceptor/processor/handler/fetcher/coordinator/controller threads), and the request lifecycle + purgatory, at a high level.\n' +
    '8. A "Map of the codebase" table: each module (core, clients, storage, metadata, raft, group-coordinator, transaction-coordinator, share-coordinator, server, streams, connect) and what lives there, with links to the chapters.\n' +
    '9. A "How to read this guide" section: a recommended reading order and the chapter list grouped logically, each as a cross-link.\n\n' +
    'Full chapter set for cross-linking:\n' + MANIFEST + '\n\n' +
    'Be accurate, concrete and engaging; aim for 3,500–5,500 words. WRITE the complete HTML fragment to ' + OUT + '/_fragments/00-overview.html using your Write tool, then return a 3-sentence summary and the list of h2 section titles.';
}

function glossaryPrompt() {
  return 'You are writing the GLOSSARY & CROSS-CUTTING CONCEPTS chapter ("glossary") for a deep source-derived Apache Kafka 4.4 (KRaft) internals guide. It serves two purposes: (A) a crisp glossary of the vocabulary used across the chapters, and (B) short explainers of the cross-cutting concepts that recur everywhere.\n\n' +
    HTML_RULES + ' (For the <h1> use "Glossary & Cross-Cutting Concepts"; the provenance line is the same.)\n\n' +
    'Read ' + OUT + '/_research/chapter-briefs.json for the exact terminology used across chapters so your definitions match the docs. \n\n' +
    'PART A — Glossary: an alphabetical (or grouped) list of terms, each with a one-to-three sentence precise definition and a cross-link to the chapter that covers it. Include at minimum: offset, log end offset (LEO), high watermark (HW), last stable offset (LSO), log start offset, partition, topic, replica, leader/follower, ISR, ELR, leader epoch, partition epoch, segment, index (offset/time/transaction), tombstone, compaction vs retention, record batch (v2), control record, producer id (PID), producer epoch, sequence number, idempotent producer, transaction marker, coordinator (group/transaction/share), consumer group, rebalance (eager/cooperative/KIP-848), share group, KRaft, controller quorum, voter vs observer, metadata log, MetadataImage/Delta, metadata.version, broker epoch, fencing, purgatory, watermark checkpoint, zero-copy, page cache, quorum, quota/throttling, rack awareness. Use a <dl class="kv"> or a table.\n' +
    'PART B — Cross-cutting concepts (a few <h2>/<h3> with short prose + a diagram where useful): the log abstraction & stream-table duality; delivery semantics (at-least-once / at-most-once / exactly-once) and where each is enforced; ordering guarantees; the "everything is a replicated log" pattern (data partitions, __consumer_offsets, __transaction_state, __cluster_metadata, __share_group_state); epochs & fencing as a universal anti-zombie mechanism; the timeline/snapshot data structures; how internal topics are used. \n\n' +
    'Full chapter set for cross-linking:\n' + MANIFEST + '\n\n' +
    'Aim for 2,000–3,500 words. WRITE the complete HTML fragment to ' + OUT + '/_fragments/glossary.html using your Write tool, then return a one-sentence summary and the count of glossary terms defined.';
}

// ---- Phase 1: fix every chapter (source-verified, surgical) ----
phase('Fix');
const fixes = await withRetry(CHAPTERS, (ch) => agent(fixPrompt(ch), {
  label: 'fix:' + ch.n + '-' + ch.slug, phase: 'Fix', schema: FIX_SCHEMA, agentType: 'general-purpose'
}));

// ---- Phase 2: synthesize overview + glossary ----
phase('Synthesize');
const synth = await withRetry(
  [{ k: 'overview', p: overviewPrompt() }, { k: 'glossary', p: glossaryPrompt() }],
  (spec) => agent(spec.p, { label: 'synthesize:' + spec.k, phase: 'Synthesize', agentType: 'general-purpose' })
);

const fixedOk = fixes.filter(Boolean).length;
const totalEdits = fixes.filter(Boolean).reduce((a, f) => a + ((f && f.edits) ? f.edits.length : 0), 0);
log('Fixes applied to ' + fixedOk + '/' + CHAPTERS.length + ' chapters (' + totalEdits + ' edits); overview+glossary synthesized.');
return { fixes, synth };
