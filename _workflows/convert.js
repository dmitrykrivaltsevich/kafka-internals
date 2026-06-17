export const meta = {
  name: 'kafka-arch-visual-upgrade',
  description: 'Convert every ASCII diagram to polished Mermaid/byte-map visuals and apply the critic\'s factual fixes, per chapter',
  phases: [{ title: 'Upgrade', detail: 'one agent per chapter: visual diagrams + verified fixes + missing content' }]
};

const ROOT = '/Users/user/projects/_playground/apache-kafka-architecture/kafka-source';
const OUT  = '/Users/user/projects/_playground/apache-kafka-architecture';

const CHAPTERS = ['00-overview','01-record-format','02-wire-protocol','03-storage-log-engine','04-storage-management',
  '05-tiered-storage','06-network-and-threading','07-request-processing','08-replication','09-fetch-path',
  '10-kraft-consensus','11-kraft-controller','12-metadata-propagation','13-group-coordination','14-transactions-eos',
  '15-share-groups','16-producer-client','17-consumer-client','18-security','19-quotas','20-kafka-streams',
  '21-kafka-connect','glossary'];

const CONVERT_GUIDE =
'DIAGRAM UPGRADE — replace ugly ASCII art with polished, native visual diagrams. The page now loads Mermaid.js (vendored, offline) and has CSS for a colored byte-map component. Find every diagram in the chapter — they look like <figure class="diagram"><pre> …ascii… </pre><figcaption>…</figcaption></figure> (or a bare <pre class="diagram">…</pre>) — and replace the ASCII <pre> with the best VISUAL representation, ALWAYS keeping the enclosing <figure class="diagram"> and its <figcaption>.\n\n' +
'(1) FLOWS, PIPELINES, ARCHITECTURES, TREES, STATE MACHINES, PROTOCOL EXCHANGES → a MERMAID diagram. Put the Mermaid source inside <pre class="mermaid">…</pre> (replacing the ascii <pre>). Choose the type:\n' +
'   • data/control flow, request lifecycle, component/module architecture, decision trees → flowchart ("flowchart LR" or "flowchart TD"; use subgraphs for layers/planes).\n' +
'   • message exchange between parties over time (producer↔broker, consumer↔coordinator, controller↔broker, raft vote/fetch, txn commit) → sequenceDiagram.\n' +
'   • lifecycle / status machines (transaction states, consumer-group states, raft roles, share record states, broker lifecycle) → stateDiagram-v2.\n' +
'   MERMAID SYNTAX SAFETY — follow EXACTLY, errors will break the page:\n' +
'   • ALWAYS quote node labels containing spaces/punctuation: A["Producer (client)"]; node IDs are simple alphanumerics only (A, RM, GC1) — never spaces/dots/parens in an ID.\n' +
'   • Quote edge labels with spaces/punctuation: A -->|"ProduceRequest acks=all"| B.\n' +
'   • NEVER put a raw < > or & in Mermaid source — a raw < is parsed as HTML and breaks the page. Use Unicode instead (≥ ≤ → ← ↔ ⇒ … ∞) or words ("ge", "to", "and"). For "acks=all" the = is fine.\n' +
'   • sequenceDiagram: "participant X as Pretty Name" (the alias text may contain spaces, no quotes); messages "X->>Y: short text", replies "Y-->>X: text", "Note over X,Y: text". Keep one colon per message line.\n' +
'   • stateDiagram-v2: "[*] --> Empty", "Empty --> Ongoing: AddPartitionsToTxn"; state ids have no spaces, or use: state "Pretty Name" as S1.\n' +
'   • Keep each diagram ≤ ~14 nodes. If an ASCII diagram is huge, simplify to its essential structure or split into two <figure> blocks. Clarity over completeness.\n\n' +
'(2) BINARY / ON-DISK / RECORD / STRUCT FIELD LAYOUTS (the v2 batch header, a record/struct field list, segment-file or index-entry byte layouts) → a BYTE-MAP (not Mermaid). Replace the ascii <pre> with:\n' +
'   <div class="bytemap">\n' +
'     <div class="bf" style="--w:8"><span class="bf-name">baseOffset</span><span class="bf-meta">int64 · @0</span></div>\n' +
'     <div class="bf" style="--w:4"><span class="bf-name">batchLength</span><span class="bf-meta">int32 · @8</span></div>\n' +
'     … one .bf per field, in order …\n' +
'   </div>\n' +
'   Set --w to the field byte size (relative width; use a sensible width like 6 for variable-length fields); put the type and byte offset in .bf-meta (e.g. "int32 · @12", or "varint" / "bytes"). Fields auto-color in sequence. You may add a <div class="bytemap-legend">…</div> after it if helpful.\n\n' +
'(3) If a "diagram" is really just a 2-3 row labeled list or a tiny table, prefer a clean <table> or a tidy <pre class="diagram"> over a forced Mermaid. Use judgment — the goal is POLISHED and CLEAR, never ASCII art.\n\n' +
'Preserve every <figcaption>. Do NOT alter prose, tables, callouts, citations, or headings except where the factual-fix task below requires it. Produce well-formed HTML (every tag closed, attributes quoted).';

const SPECIAL = {
  '00-overview':
    'SPECIFIC OVERVIEW CORRECTIONS (verified against source — apply each): ' +
    '(a) The MetadataImage is composed of MetadataProvenance plus NINE sub-images (Features, Cluster, Topics, Configurations, ClientQuotas, ProducerIds, Acls, Scram, DelegationToken) — see metadata/src/main/java/org/apache/kafka/image/MetadataImage.java:35. Correct any claim of "10 sub-images" to nine. ' +
    '(b) KafkaApis has exactly 80 "case ApiKeys." dispatch cases (verified), not ~90 — change "~90 handlers" to "~80". ' +
    '(c) For acks=0 the broker sends NO produce response (a NoOpResponse); the offset −1 is fabricated client-side in RecordMetadata (core/src/main/scala/kafka/server/KafkaApis.scala around the acks==0 path). Reword any "replies immediately with offset −1" to make clear there is no broker reply. ' +
    '(d) Replace the invented record name "AclRecord" with the real "AccessControlEntryRecord". ' +
    '(e) Zero-pad the snapshot file name in the on-disk figure to 20 digits (e.g. 00000000000000004096.snapshot). ' +
    '(f) The KIP-1071 Streams rebalance protocol is Generally Available since Kafka 4.2 (Early Access in 4.1) — do not call it merely "Early Access".',
  'glossary':
    'ALSO ADD these missing high-value glossary terms (currently absent), with crisp definitions and cross-links: ' +
    'Tiered storage / remote log (and the __remote_log_metadata internal topic) → link 05-tiered-storage.html; and the security vocabulary — ACL, Authorizer (StandardAuthorizer), KafkaPrincipal / principal, SASL, SSL/TLS, delegation token, super.users → link 18-security.html. Place them in the correct alphabetical/grouped position.',
  '20-kafka-streams':
    'FACTUAL FIX: the KIP-1071 Streams rebalance protocol is Generally Available since Kafka 4.2 (it was Early Access in 4.1). Make EVERY mention consistent with GA-since-4.2; remove any standalone "Early Access" maturity label that implies it is not yet GA.',
  '09-fetch-path':
    'FACTUAL FIX: the consumer fetch.max.bytes DEFAULT is 52428800 (50 MiB), per clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java:200 (DEFAULT_FETCH_MAX_BYTES = 50*1024*1024). Correct any "55 MiB"/"57671680" to 50 MiB / 52428800.',
  '17-consumer-client':
    'FACTUAL FIX: ensure the consumer fetch.max.bytes DEFAULT is stated as 52428800 (50 MiB) per ConsumerConfig.java:200 — consistent with the fetch-path chapter.',
  '11-kraft-controller':
    'ALSO ADD a new section "<h2>Cluster formation &amp; bootstrap</h2>" (the guide currently lacks this entirely). Read the source and explain how a brand-new KRaft cluster is created: generating a cluster id (Uuid via "kafka-storage random-uuid"); the "kafka-storage format" command (kafka/tools/StorageTool.scala and metadata/.../storage/Formatter.java) writing meta.properties (server-common .../properties/MetaPropertiesEnsemble) into every log.dir; the bootstrap metadata (metadata/.../bootstrap/BootstrapMetadata + the bootstrap.checkpoint file) that seeds the initial metadata.version and any initial records; and how the initial controller quorum is established — static controller.quorum.voters vs the KIP-853 dynamic quorum ("--standalone" / "--initial-controllers" / "--no-initial-controllers"). ~450–700 words, with a small Mermaid flowchart (format → meta.properties + bootstrap.checkpoint → first controller elected leader → brokers register & fetch metadata). Cite files with path:line.'
};

const SCHEMA = {
  type: 'object',
  properties: {
    chapter: { type: 'string' },
    diagramsFound: { type: 'number' },
    convertedToMermaid: { type: 'number' },
    convertedToBytemap: { type: 'number' },
    keptOrTable: { type: 'number' },
    mermaidTypes: { type: 'array', items: { type: 'string' } },
    fixesApplied: { type: 'array', items: { type: 'string' } },
    contentAdded: { type: 'string' },
    htmlOk: { type: 'boolean' }
  },
  required: ['chapter', 'diagramsFound', 'convertedToMermaid', 'htmlOk']
};

function buildPrompt(key) {
  const frag = OUT + '/_fragments/' + key + '.html';
  let p = 'You are upgrading ONE chapter of an Apache Kafka 4.4.0-SNAPSHOT (KRaft-only) internals documentation site to be a polished, visual HTML artifact, and applying verified factual corrections. Ground truth is the source under ' + ROOT + '.\n\n' +
    'Chapter fragment to edit IN PLACE (Read it, then make surgical Edits; for big additions you may Read then Write the whole file back): ' + frag + '\n\n' +
    '=== TASK 1: VISUAL DIAGRAMS ===\n' + CONVERT_GUIDE + '\n\n' +
    '=== TASK 2: FACTUAL FIXES ===\n' +
    'Read the JSON file ' + OUT + '/_research/critic-findings.json and take the array at byChapter["' + key + '"] (may be empty). For each finding: locate it in the fragment, confirm against the cited source if any, and apply a minimal, source-grounded correction. Do not rewrite beyond what the fix needs.\n';
  if (SPECIAL[key]) p += '\n=== TASK 3: CHAPTER-SPECIFIC ===\n' + SPECIAL[key] + '\n';
  p += '\nAfter editing, re-read the fragment to confirm it is well-formed HTML (every <pre class="mermaid"> contains valid Mermaid with quoted labels and no raw < > &; every <figure> and tag is closed). Return the structured summary.';
  return p;
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

phase('Upgrade');
const results = await withRetry(CHAPTERS, (key) => agent(buildPrompt(key), {
  label: 'upgrade:' + key, phase: 'Upgrade', schema: SCHEMA, agentType: 'general-purpose'
}));

const ok = results.filter(Boolean);
const merm = ok.reduce((a, r) => a + (r.convertedToMermaid || 0), 0);
const bm = ok.reduce((a, r) => a + (r.convertedToBytemap || 0), 0);
log('Upgrade complete: ' + ok.length + '/' + CHAPTERS.length + ' chapters; ' + merm + ' Mermaid + ' + bm + ' byte-map diagrams.');
return results;
