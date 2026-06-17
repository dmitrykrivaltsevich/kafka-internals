export const meta = {
  name: 'kafka-arch-bespoke-diagrams',
  description: 'Replace every Mermaid diagram with a hand-crafted bespoke component (flow/stack/sequence/state/logstrip/bytemap) and add a legend to every figure',
  phases: [{ title: 'Bespoke', detail: 'one agent per chapter converts all diagrams to the bespoke component system' }]
};

const OUT = '/Users/user/projects/_playground/apache-kafka-architecture';
const ROOT = OUT + '/kafka-source';

const CHAPTERS = ['00-overview','01-record-format','02-wire-protocol','03-storage-log-engine','04-storage-management',
  '05-tiered-storage','06-network-and-threading','07-request-processing','08-replication','09-fetch-path',
  '10-kraft-consensus','11-kraft-controller','12-metadata-propagation','13-group-coordination','14-transactions-eos',
  '15-share-groups','16-producer-client','17-consumer-client','18-security','19-quotas','20-kafka-streams',
  '21-kafka-connect','glossary'];

const GUIDE =
'You are upgrading the diagrams in ONE chapter of a high-end Apache Kafka internals book-site to a BESPOKE, hand-crafted visual system. Mermaid has been REMOVED. Your job: replace every Mermaid diagram with the right bespoke HTML component, and put a LEGEND on every diagram. The result must look like a polished technical book — clean, consistent, with a legend that explains the shapes, arrows, colours and typography it uses.\n\n' +
'SEMANTIC COLOUR SYSTEM (apply with these classes; be consistent):\n' +
'  cat-client  (blue)   — producers, consumers, admin clients, anything client-side\n' +
'  cat-broker  (purple) — broker server components (KafkaApis, ReplicaManager, SocketServer…)\n' +
'  cat-storage (green)  — the log / segments / disk / internal topics (__consumer_offsets, __cluster_metadata…)\n' +
'  cat-control (amber)  — KRaft controller, metadata, Raft\n' +
'  cat-coord   (teal)   — coordinators (group/txn/share), purgatory, waiting\n' +
'  cat-error   (red)    — failure / abort / fenced paths\n\n' +
'THE COMPONENTS (pick the best fit for each diagram; copy these templates and fill them in):\n\n' +
'① dflow — flows, request/data/control pipelines, decision trees (the most common). Vertical by default; add class "h" on .dflow for horizontal.\n' +
'<figure class="diagram"><div class="dflow">\n' +
'  <div class="node cat-client"><span class="nt">producer.send()</span><span class="nd">serialize · partition</span></div>\n' +
'  <div class="conn" data-label="enqueue"></div>\n' +
'  <div class="node cat-broker store"><span class="nt">Partition leader</span><span class="nd">UnifiedLog.append</span></div>\n' +
'  <div class="conn"></div>\n' +
'  <div class="node cat-storage decision">acks?</div>\n' +
'  <div class="conn"></div>\n' +
'  <div class="row">\n' +
'    <div class="col"><div class="conn" data-label="0 / 1"></div><div class="node"><span class="nt">reply now</span></div></div>\n' +
'    <div class="col"><div class="conn dashed ec-async" data-label="all"></div><div class="node cat-coord"><span class="nt">await HW (purgatory)</span></div></div>\n' +
'  </div>\n' +
'</div><figcaption>…</figcaption><div class="legend">…</div></figure>\n' +
'   node modifiers: "store" (cylinder = a log/store), "pill", "decision" (rounded decision). connector .conn: optional data-label, add "dashed" for async, and "ec-data"/"ec-async"/"ec-err" to colour the arrow. Use .row + .col for parallel branches.\n\n' +
'② dstack — layered architecture / planes / module stacks.\n' +
'<figure class="diagram"><div class="dstack">\n' +
'  <div class="layer cat-control"><div class="ln">Metadata plane — controller quorum</div><div class="ld">owns cluster metadata as a Raft log</div><div class="row"><span class="chip">QuorumController</span><span class="chip">MetadataImage</span></div></div>\n' +
'  <div class="sep">↑ register / heartbeat · replay records ↓</div>\n' +
'  <div class="layer cat-broker"><div class="ln">Data plane — brokers</div><div class="ld">host replicas; serve produce/fetch</div></div>\n' +
'</div><figcaption>…</figcaption><div class="legend">…</div></figure>\n\n' +
'③ dseq — sequence diagrams (protocol message exchange over time). Set --cols to the number of lanes; one <i> per lane in .lifelines; lanes numbered 1..N left→right.\n' +
'<figure class="diagram"><div class="dseq" style="--cols:3">\n' +
'  <div class="lanes"><span class="actor cat-client">Consumer</span><span class="actor cat-coord">Coordinator</span><span class="actor cat-storage">__consumer_offsets</span></div>\n' +
'  <div class="body"><div class="lifelines"><i></i><i></i><i></i></div>\n' +
'    <div class="m r" style="--from:1;--to:2"><span class="ml">ConsumerGroupHeartbeat(epoch=5)</span><span class="ln"></span></div>\n' +
'    <div class="m r ec-async" style="--from:2;--to:3"><span class="ml">append records</span><span class="ln"></span></div>\n' +
'    <div class="m l dashed" style="--from:2;--to:1"><span class="ml">target assignment</span><span class="ln"></span></div>\n' +
'  </div>\n' +
'</div><figcaption>…</figcaption><div class="legend">…</div></figure>\n' +
'   --from/--to are lane numbers of the source/destination. Class "r" = arrow points right (from‹to); "l" = points left (from›to). Add "dashed" for replies; "ec-async"/"ec-err" to colour. Optional <div class="note">…</div>.\n\n' +
'④ dstate — state machines. Add class "v" on .dstate for a vertical layout when there are many states or branches.\n' +
'<figure class="diagram"><div class="dstate">\n' +
'  <span class="term"></span><span class="tr"><span class="tl"></span><span class="ta"></span></span>\n' +
'  <span class="st">Empty</span><span class="tr"><span class="tl">AddPartitionsToTxn</span><span class="ta"></span></span>\n' +
'  <span class="st">Ongoing</span><span class="tr"><span class="tl">EndTxn(commit)</span><span class="ta"></span></span>\n' +
'  <span class="st accent">CompleteCommit</span><span class="tr"><span class="tl"></span><span class="ta"></span></span><span class="term end"></span>\n' +
'</div><figcaption>…</figcaption><div class="legend">…</div></figure>\n' +
'   .st = a state (add "accent" green / "warn" red). .term = initial dot, .term.end = terminal. .tr = a transition (.tl label, .ta arrow). Keep ≤ ~6 states per row, else use class "v".\n\n' +
'⑤ logstrip — the partition commit log / any offset-cell sequence.\n' +
'<figure class="diagram"><div class="logstrip">\n' +
'  <div class="ls-head"><span class="ttl">partition 0 · topic <span class="q">"orders"</span></span><span class="append">producer appends →</span></div>\n' +
'  <div class="cells"><div class="cell committed">0</div><div class="cell committed">5<span class="tag hw">high watermark</span></div><div class="cell pending">6</div><div class="cell next">7<span class="tag leo">LEO</span></div></div>\n' +
'  <div class="marks"><div class="mslot"></div><div class="mslot"><span class="mk">group A<br>@1</span></div></div>\n' +
'</div><figcaption>…</figcaption><div class="legend">…</div></figure>\n' +
'   cell classes: committed (green), pending (amber dashed), next (hatched). .tag.hw / .tag.leo badges. .marks has one .mslot per cell; .mk (or .mk.b) sits under a cell as a consumer marker.\n\n' +
'⑥ bytemap — binary / on-disk / struct field layouts (already used in some chapters; keep these, but ensure each has a legend).\n' +
'<figure class="diagram"><div class="bytemap"><div class="bf" style="--w:8"><span class="bf-name">baseOffset</span><span class="bf-meta">int64 · @0</span></div>…</div><figcaption>…</figcaption><div class="legend">…</div></figure>\n\n' +
'LEGEND — REQUIRED on every <figure class="diagram"> (add it right after the <figcaption>). Explain ONLY the encodings this diagram actually uses — colours, shapes, arrow styles, typography:\n' +
'<div class="legend">\n' +
'  <span class="lgi"><span class="sw cat-client"></span>client</span>\n' +
'  <span class="lgi"><span class="sw cat-broker"></span>broker</span>\n' +
'  <span class="lgi"><span class="sw cat-storage"></span>log / storage</span>\n' +
'  <span class="lgi"><span class="arr"></span>data flow</span>\n' +
'  <span class="lgi"><span class="arr dashed"></span>async / response</span>\n' +
'  <span class="lgi"><span class="mono">Class</span> = source identifier</span>\n' +
'</div>\n' +
'   legend swatches: <span class="sw cat-X"></span> colour chip; add "round" for a circle, "dash" for a dashed outline; <span class="arr"></span> / <span class="arr dashed"></span> arrow samples; <span class="mono">…</span> for typography notes. For dstate also note "pill = state, ◉ = terminal"; for dflow note "cylinder = log/store, rounded = decision" if used.';

const RULES =
'RULES:\n' +
'• Replace EVERY <pre class="mermaid"> … </pre> with the best bespoke component above. ZERO Mermaid may remain (no <pre class="mermaid">, no "flowchart"/"sequenceDiagram"/"stateDiagram" text).\n' +
'• Preserve the enclosing <figure class="diagram"> and its <figcaption> (you may lightly polish the caption wording, never the facts).\n' +
'• EVERY <figure class="diagram"> must end with a <div class="legend"> that matches its content — including existing bytemap figures that currently lack one.\n' +
'• Keep the diagram faithful to what the Mermaid showed (same nodes, edges, direction, labels, states). Do not invent or drop steps. If a diagram is too complex for one component, split it into two <figure>s or simplify for clarity — but keep the meaning.\n' +
'• Use the semantic colours consistently and correctly for what each node represents.\n' +
'• Escape any literal < > & in text as &lt; &gt; &amp;. Produce well-formed HTML (every tag closed, attributes quoted). Do NOT touch prose, tables, callouts, headings, or citations outside the <figure> diagrams.\n' +
'• A handful of "diagrams" may genuinely be better as a small <table> (pure tabular data) — that is allowed (a table needs no legend). But anything showing flow, structure, sequence, state, layout, or a log should become a bespoke component WITH a legend.';

const SCHEMA = {
  type: 'object',
  properties: {
    chapter: { type: 'string' },
    mermaidFound: { type: 'number' },
    mermaidRemaining: { type: 'number', description: 'MUST be 0' },
    components: { type: 'object', properties: {
      dflow: { type: 'number' }, dstack: { type: 'number' }, dseq: { type: 'number' },
      dstate: { type: 'number' }, logstrip: { type: 'number' }, bytemap: { type: 'number' }, table: { type: 'number' }
    } },
    legendsTotal: { type: 'number', description: 'number of <figure class="diagram"> that now have a legend' },
    figuresTotal: { type: 'number' },
    htmlOk: { type: 'boolean' }
  },
  required: ['chapter', 'mermaidRemaining', 'legendsTotal', 'figuresTotal', 'htmlOk']
};

function prompt(key) {
  const frag = OUT + '/_fragments/' + key + '.html';
  return GUIDE + '\n\n' + RULES + '\n\n' +
    'CHAPTER FRAGMENT to edit IN PLACE (Read it fully, then Edit; for many diagrams you may Read then Write the whole file back): ' + frag + '\n' +
    '(If you need to confirm a detail to label a node correctly, the Kafka source is at ' + ROOT + ', but the existing Mermaid already encodes the diagram — your job is to re-express it, not re-derive it.)\n\n' +
    'When done, re-read the fragment and verify: NO mermaid remains; every diagram figure has a legend; the HTML is well-formed. Return the structured summary (mermaidRemaining MUST be 0, and legendsTotal SHOULD equal figuresTotal).';
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

phase('Bespoke');
const results = await withRetry(CHAPTERS, (key) => agent(prompt(key), {
  label: 'bespoke:' + key, phase: 'Bespoke', schema: SCHEMA, agentType: 'general-purpose'
}));

const ok = results.filter(Boolean);
const remaining = ok.reduce((a, r) => a + (r.mermaidRemaining || 0), 0);
const legends = ok.reduce((a, r) => a + (r.legendsTotal || 0), 0);
log('Bespoke conversion: ' + ok.length + '/' + CHAPTERS.length + ' chapters; mermaid remaining=' + remaining + '; legends=' + legends + '.');
return results;
