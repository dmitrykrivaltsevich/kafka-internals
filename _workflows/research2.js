export const meta = {
  name: 'kafka-ops-blueprint-research',
  description: 'Gather the empirical/operational/comparative knowledge (capacity, limits guidance, cost, scaling case studies, comparative architecture, critiques) that the source code cannot provide, for Part II (Operations) and Part III (Blueprint)',
  phases: [
    { title: 'Research', detail: 'parallel web research per operational/architectural angle' },
    { title: 'Consolidate', detail: 'merge into one reference file for authoring + fact-checking' }
  ]
};

const OUT = '/Users/user/projects/_playground/apache-kafka-architecture';

const TOPICS = [
  { key: 'capacity-sizing',
    q: 'Apache Kafka capacity planning and cluster sizing: concrete formulas and rules of thumb for sizing brokers, partitions, disk, memory (page cache & heap) and network from a target throughput; published throughput benchmarks (e.g. the classic LinkedIn "2 million writes/sec" result, Confluent and Redpanda benchmark posts, OpenMessaging benchmark numbers); replication network amplification; per-broker and per-partition realistic throughput ceilings (MB/s). Give numbers with sources and the assumptions behind them.' },
  { key: 'partition-count',
    q: 'How many partitions should a Kafka topic / cluster have? The community and Confluent guidance: per-broker and per-cluster partition limits and why (the old ZooKeeper ~4000/broker and ~200000/cluster guidance vs the much higher limits enabled by KRaft/KIP-500), the costs of too many partitions (leader election & failover time, end-to-end latency, open file descriptors, memory, controller load, rebalance time, replication overhead), heuristics for choosing partition count (max of producer/consumer throughput needs), and the difficulty and approaches for repartitioning a keyed topic. Cite Confluent/Kafka docs, KIP-500, and Jun Rao\'s "how to choose the number of partitions" post.' },
  { key: 'perf-tuning',
    q: 'Apache Kafka performance tuning best practices: the throughput-vs-latency tradeoffs and recommended settings for producers (batch.size, linger.ms, compression type comparisons lz4/zstd/snappy/gzip, acks, max.in.flight, buffer.memory), consumers (fetch.min.bytes, fetch.max.bytes, max.poll.records), and brokers (num.io.threads, num.network.threads, num.replica.fetchers, socket buffers, segment size, OS page cache, swappiness, file descriptors). Why each matters. Cite Confluent tuning guides and reputable engineering blogs.' },
  { key: 'failure-ops',
    q: 'Apache Kafka common failure modes and operational incidents and their runbooks: under-replicated partitions, offline partitions, unclean leader election and data loss, ISR shrink/expand thrash, slow/lagging followers, disk and log-directory failures (JBOD), full disks, GC pauses, consumer group rebalance storms, hot/skewed partitions, request handler/queue saturation, producer fencing, hanging transactions blocking the last-stable-offset. Symptoms, root causes, and remediations. Cite operational guides and post-mortems.' },
  { key: 'metrics-monitoring',
    q: 'Which Apache Kafka metrics matter most for monitoring and the "golden signals": under-replicated partitions, offline partition count, active controller count, request latency broken down into queue/local/remote/response/throttle time, request handler and network processor idle ratio, log flush latency, leader election rate and time, ISR shrink/expand rate, purgatory size, consumer lag, bytes-in/out rate, under-min-ISR partitions. Recommended thresholds/alerts and dashboard practices. Cite Confluent/Datadog/Grafana Kafka monitoring guides.' },
  { key: 'cost',
    q: 'Apache Kafka cost optimization in the cloud: the main cost drivers (storage = throughput x retention x replication factor; inter-AZ/cross-AZ network transfer for replication and consumer fetch; compute/instances) and the levers — compression, fetch-from-follower (KIP-392) to cut cross-AZ read cost, tiered storage (KIP-405) to cut storage cost, replication factor and retention tradeoffs, and newer diskless/object-store designs (WarpStream, KIP-1150). Real cost figures and case studies where available. Cite engineering blogs.' },
  { key: 'scaling-case-studies',
    q: 'Real-world high-throughput Apache Kafka deployments and what breaks at scale: published architectures and numbers from LinkedIn, Uber, Netflix, Cloudflare, Pinterest, Datadog and similar operating millions to hundreds of millions of events per second / trillions of messages per day. What bottlenecks and limits appear at each scale (partition count, controller/metadata limits, replication network, cross-AZ cost, page-cache pressure, multi-cluster federation). Cite the engineering blog posts with concrete numbers.' },
  { key: 'log-pattern',
    q: 'The distributed commit log as an architectural pattern: Jay Kreps "The Log", Martin Kleppmann "Turning the Database Inside Out" and "Designing Data-Intensive Applications", event sourcing and CQRS, stream-table duality, change data capture (Debezium), the outbox pattern, log compaction as materialized state. When the log/event-streaming pattern is the right architectural choice and when it is not (anti-patterns: Kafka as a database, as RPC, as a low-latency task queue). Cite the primary sources.' },
  { key: 'comparative-critique',
    q: 'Apache Kafka compared to alternatives, and its inherent architectural limitations and criticisms: Kafka vs Apache Pulsar (compute/storage separation via BookKeeper), vs Redpanda (C++ thread-per-core, no JVM/page-cache reliance), vs AWS Kinesis / Google Pub/Sub (managed, shard model), vs RabbitMQ/traditional MQ (broker-tracked, flexible routing), vs WarpStream/diskless object-store-native designs. Kafka\'s structural weaknesses and common criticisms: per-partition ordering only, partition-count scaling ceiling, no per-message TTL/priority/routing, rebalance disruption, operational complexity, cross-AZ cost, latency floor. Cite comparative analyses and critiques.' }
];

const NOTE_SCHEMA = {
  type: 'object',
  properties: {
    topic: { type: 'string' },
    summary: { type: 'string', description: '5-10 sentence synthesis' },
    keyFacts: { type: 'array', items: { type: 'string' }, description: 'concrete, citable facts/numbers/heuristics with the source named inline' },
    formulasOrHeuristics: { type: 'array', items: { type: 'string' } },
    caseStudies: { type: 'array', items: { type: 'string' }, description: 'org + numbers + what broke, if any' },
    cautions: { type: 'array', items: { type: 'string' }, description: 'commonly-misstated facts / version caveats' },
    sources: { type: 'array', items: { type: 'object', properties: { title: { type: 'string' }, url: { type: 'string' } }, required: ['title'] } }
  },
  required: ['topic', 'summary', 'keyFacts']
};

phase('Research');
const notes = await parallel(TOPICS.map((t) => () => agent(
  'You are gathering authoritative, CITABLE operational and architectural knowledge about Apache Kafka to support an internals book (Part II Operations Manual, Part III architectural blueprint). The source code is handled separately and is the ground truth for configs/limits/mechanisms — your job is the EMPIRICAL layer the code cannot give: real numbers, benchmarks, capacity formulas, sizing heuristics, scaling case studies, cost figures, comparative-architecture facts, and critiques. Use web search/fetch on primary and reputable sources (Confluent docs & blog, Kafka docs/KIPs, engineering blogs from LinkedIn/Uber/Netflix/Cloudflare/etc., Jun Rao / Jay Kreps / Martin Kleppmann writings, OpenMessaging/Redpanda benchmarks, comparative analyses). Prefer concrete numbers and name the source inline. Flag anything commonly misstated or version-dependent. Topic:\n\n' + t.q,
  { label: 'research2:' + t.key, phase: 'Research', schema: NOTE_SCHEMA, agentType: 'general-purpose' }
).then((r) => r || { topic: t.key, summary: '(research failed)', keyFacts: [] })));

phase('Consolidate');
const consolidated = await agent(
  'Compile a single well-organized Markdown reference titled "Apache Kafka — Operations & Blueprint: Empirical Reference" from the structured research notes below. It is an INTERNAL reference for documentation authors and fact-checkers (not a published page). Structure: a short intro; one section per topic with the synthesized findings, key numbers/facts (each with its source), formulas & heuristics, case studies, and cautions; then a consolidated "Capacity & sizing formulas" quick-reference; a "Partition-count heuristics" box; a "Cost levers" box; a "Comparative systems" table (Kafka vs Pulsar/Redpanda/Kinesis/PubSub/RabbitMQ/WarpStream — model, storage, ordering, key tradeoff); and a final "Common pitfalls / version caveats" section. Write it to ' + OUT + '/_research/ops-blueprint-reference.md with your Write tool. Then return a one-paragraph confirmation and the count of distinct sources cited.\n\nRESEARCH NOTES (JSON):\n' + JSON.stringify(notes, null, 1),
  { label: 'consolidate-ops', phase: 'Consolidate', agentType: 'general-purpose' }
);

log('Ops/blueprint empirical reference written.');
return { consolidated, topicCount: notes.length, notes };
