export const meta = {
  name: 'kafka-design-rationale',
  description: 'Gather authoritative design rationale (KIPs, papers) for Kafka subsystems into a reference file used by the overview synthesis and fact-checking',
  phases: [
    { title: 'Research', detail: 'parallel web research per design area' },
    { title: 'Consolidate', detail: 'merge into a single design-rationale reference file' }
  ]
};

const OUT = '/Users/user/projects/_playground/apache-kafka-architecture';

const TOPICS = [
  { key: 'origins',
    q: 'The origins and core design philosophy of Apache Kafka: the original LinkedIn paper "Kafka: a Distributed Messaging System for Log Processing" (Kreps, Narkhede, Rao, NetDB 2011) and Jay Kreps\'s "The Log: What every software engineer should know about real-time data\'s unifying abstraction" (2013). What problem Kafka was built to solve, the distributed commit log abstraction, log-centric / stream-table duality, and the throughput-oriented design goals (sequential IO, page cache, zero-copy, batching). Cite the paper and key blog posts.' },
  { key: 'storage',
    q: 'Apache Kafka storage and record-format design rationale: log-structured append-only segments, reliance on the OS page cache and sequential IO, zero-copy via sendfile, the v2 message/record batch format introduced in KIP-98 (batching, relative offsets, headers), compression, and log compaction. Which KIPs introduced these. Cite KIP numbers and titles.' },
  { key: 'replication',
    q: 'Apache Kafka replication design: the ISR (in-sync replica) model, high watermark, acks and durability, leader epochs and the log-divergence/truncation fixes (KIP-101 and KIP-279), fetch-from-follower for rack locality (KIP-392), and Eligible Leader Replicas (KIP-966). Explain the design intent and trade-offs vs quorum replication. Cite KIP numbers/titles and status.' },
  { key: 'kraft',
    q: 'KRaft (Kafka Raft) design and the removal of ZooKeeper: KIP-500 (replace ZooKeeper with a self-managed metadata quorum and the motivation), KIP-595 (the Raft protocol for the metadata quorum, pull-based replication), KIP-630 (metadata snapshots), KIP-631 (the quorum controller and metadata record format), KIP-584 (feature versioning / metadata.version), KIP-866 (ZooKeeper to KRaft migration), KIP-853 (dynamic KRaft quorum reconfiguration / dynamic voters), and the completion of ZooKeeper removal in Kafka 4.0. Cite KIP numbers, titles, and which Kafka versions shipped them.' },
  { key: 'eos',
    q: 'Apache Kafka exactly-once semantics design: the idempotent producer and transactions from KIP-98 (producer IDs, epochs, sequence numbers, transaction coordinator, transaction markers, two-phase commit), read_committed isolation and the Last Stable Offset, and KIP-447 (producer scalability for exactly-once with consumer groups, sendOffsetsToTransaction). Explain the protocol intent. Cite KIP numbers and titles.' },
  { key: 'consumer-groups',
    q: 'Apache Kafka consumer group and rebalance protocol evolution: the group coordinator, KIP-429 (incremental cooperative rebalancing), KIP-345 (static membership), and especially KIP-848 (the next generation consumer rebalance protocol — fully server-side, incremental, broker-driven assignment with epochs, replacing the JoinGroup/SyncGroup dance), plus the rewrite of the group coordinator in Java and KIP-1071 (Streams rebalance protocol). Cite KIP numbers, titles, status, and target versions.' },
  { key: 'tiered-storage',
    q: 'Apache Kafka tiered storage design: KIP-405 (tiered storage) — the motivation (decoupling storage from compute, infinite retention to object stores), the RemoteStorageManager and RemoteLogMetadataManager pluggable interfaces, local vs remote retention, and the read path for historical data. Cite KIP-405 and related follow-up KIPs and the version it became production-ready.' },
  { key: 'queues',
    q: 'Apache Kafka KIP-932 "Queues for Kafka" / share groups: the motivation (queue-like cooperative consumption with per-record acknowledgement and redelivery, many consumers per partition), share groups vs consumer groups, the acquisition lock model, delivery counts, the share coordinator and share-group state, and the feature status/maturity (early access vs GA) across Kafka versions. Cite KIP-932 and related KIPs.' }
];

const NOTE_SCHEMA = {
  type: 'object',
  properties: {
    topic: { type: 'string' },
    summary: { type: 'string', description: '4-8 sentence synthesis of the design rationale' },
    kips: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          id: { type: 'string' }, title: { type: 'string' },
          whatItDid: { type: 'string' }, status: { type: 'string' }
        },
        required: ['id', 'title']
      }
    },
    keyDesignDecisions: { type: 'array', items: { type: 'string' }, description: 'decisions WITH their rationale/trade-off' },
    historicalEvolution: { type: 'string' },
    cautions: { type: 'array', items: { type: 'string' }, description: 'commonly-misstated facts or version caveats to avoid errors' },
    sources: { type: 'array', items: { type: 'object', properties: { title: { type: 'string' }, url: { type: 'string' } }, required: ['title'] } }
  },
  required: ['topic', 'summary', 'kips', 'keyDesignDecisions']
};

phase('Research');
const notes = await parallel(TOPICS.map((t) => () => agent(
  'You are researching the authoritative DESIGN RATIONALE behind a part of Apache Kafka, to support an internals documentation project. Use web search and fetch to consult primary sources: the Apache Kafka Improvement Proposals (KIPs) on the cwiki, the official design docs, the original academic paper, and reputable engineering blog posts. Focus on WHY Kafka is built the way it is, the trade-offs, and the historical evolution, plus exact KIP numbers/titles/status and which Kafka versions shipped each change. Be precise and factual; flag anything commonly misstated. Topic:\n\n' + t.q,
  { label: 'research:' + t.key, phase: 'Research', schema: NOTE_SCHEMA, agentType: 'general-purpose' }
).then((r) => r || { topic: t.key, summary: '(research failed)', kips: [], keyDesignDecisions: [] })));

phase('Consolidate');
const consolidated = await agent(
  'You are compiling a single, well-organized Markdown reference titled "Apache Kafka — Design Rationale & KIP Index" from the structured research notes below. This file is an internal reference for documentation authors and fact-checkers (it is NOT a published web page). Produce clean Markdown with: a short intro; one section per topic containing the synthesized rationale, the key design decisions with their trade-offs, and the historical evolution; a consolidated KIP table (KIP, title, what it did, status/version) sorted by KIP number and de-duplicated; and a final "Common pitfalls / version caveats" section aggregating the cautions (things documentation authors frequently get wrong). Write the file to ' + OUT + '/_research/design-rationale-and-kips.md using your Write tool. Then return a one-paragraph confirmation plus the total count of distinct KIPs catalogued.\n\nRESEARCH NOTES (JSON):\n' + JSON.stringify(notes, null, 1),
  { label: 'consolidate-rationale', phase: 'Consolidate', agentType: 'general-purpose' }
);

log('Design-rationale reference written.');
return { consolidated, topicCount: notes.length, notes };
