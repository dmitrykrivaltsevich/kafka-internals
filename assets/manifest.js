/* Single source of truth for the site: every page's metadata + the 3-part nav.
   Loaded as a classic <script> (sets window.KAFKA_DOCS) and require()d by the
   Node build (module.exports). Keep app.js and build.js free of page lists. */
(function () {
  var PAGES = {
    // ---- Part I, Architecture Internals ----
    "00-overview":             { num: "00", title: "Architecture Overview",            desc: "The distributed commit log, broker & cluster anatomy, and the end-to-end data path." },
    "01-record-format":        { num: "01", title: "Record Format & Batches",          desc: "The v2 record batch on disk and on the wire: varints, headers, CRC, control records, compression." },
    "02-wire-protocol":        { num: "02", title: "Wire Protocol & RPC",              desc: "Request/response framing, ApiKeys, the schema generator, flexible versions, tagged fields." },
    "03-storage-log-engine":   { num: "03", title: "The Log Storage Engine",           desc: "UnifiedLog, LogSegment, the offset/time/transaction indexes, append & read paths, recovery." },
    "04-storage-management":   { num: "04", title: "Retention & Compaction",           desc: "LogManager, retention by time/size, the log cleaner, compaction, tombstones, JBOD." },
    "05-tiered-storage":       { num: "05", title: "Tiered Storage",                   desc: "KIP-405 remote log: RemoteLogManager, the metadata topic, RemoteIndexCache, copy/read paths." },
    "06-network-and-threading":{ num: "06", title: "Network & Threading Model",        desc: "SocketServer, the Acceptor/Processor reactor, the handler pool, Selector, purgatory & timing wheels." },
    "07-request-processing":   { num: "07", title: "Request Processing",               desc: "KafkaApis dispatch, the request lifecycle, validation, authorization & throttling." },
    "08-replication":          { num: "08", title: "Replication, ISR & High Watermark",desc: "ReplicaManager, Partition, the ISR, the high watermark, leader epochs, acks, unclean election, ELR." },
    "09-fetch-path":           { num: "09", title: "Fetch Path & Replica Fetchers",    desc: "Follower replication, AbstractFetcherThread, truncation, incremental fetch sessions, DelayedFetch." },
    "10-kraft-consensus":      { num: "10", title: "KRaft Consensus (Raft)",           desc: "KafkaRaftClient, the quorum state machine, elections, pull-based replication, snapshots, voters." },
    "11-kraft-controller":     { num: "11", title: "The KRaft Controller",             desc: "QuorumController, the control managers, the single-threaded event loop, timeline data structures." },
    "12-metadata-propagation": { num: "12", title: "Metadata & Broker Lifecycle",      desc: "MetadataImage/Delta, the loader & publishers, KRaftMetadataCache, registration, heartbeats, fencing." },
    "13-group-coordination":   { num: "13", title: "Group Coordination",               desc: "The group coordinator, classic & KIP-848 rebalance protocols, assignors, offset management." },
    "14-transactions-eos":     { num: "14", title: "Transactions & Exactly-Once",      desc: "Idempotent producer, the transaction coordinator, markers, two-phase commit, read_committed & LSO." },
    "15-share-groups":         { num: "15", title: "Share Groups (Queues)",            desc: "KIP-932 queues: share consumers, the share coordinator, acquisition locks, delivery counts, DLQ." },
    "16-producer-client":      { num: "16", title: "The Producer Client",              desc: "KafkaProducer, the RecordAccumulator, BufferPool, the Sender thread, partitioning, idempotence." },
    "17-consumer-client":      { num: "17", title: "The Consumer Client",              desc: "Classic vs async consumer, SubscriptionState, the fetch pipeline, request managers, the poll loop." },
    "18-security":             { num: "18", title: "Security",                         desc: "SASL/SSL/OAuth/SCRAM/Kerberos, delegation tokens, the Authorizer, KRaft ACLs, principal building." },
    "19-quotas":               { num: "19", title: "Quotas & Throttling",              desc: "ClientQuotaManager, token buckets, quota entities, throttle responses, client metrics (KIP-714)." },
    "20-kafka-streams":        { num: "20", title: "Kafka Streams",                    desc: "Topologies, tasks, StreamThread, state stores & changelogs, the partition assignor, EOS." },
    "21-kafka-connect":        { num: "21", title: "Kafka Connect",                    desc: "Workers, connectors & tasks, the distributed herder, backing stores, MirrorMaker 2." },
    "glossary":                { num: ", ",  title: "Glossary & Concepts",              desc: "Cross-cutting terminology and a quick reference of Kafka's core abstractions." },

    // ---- Part II, Operations Manual ----
    "op00-operator-model":     { num: "II·00", title: "The Operator's Mental Model",   desc: "What you run in KRaft, what fails independently, the control loops, and the SLIs/SLOs of a healthy cluster." },
    "op01-configuration":      { num: "II·01", title: "Configuration: The Tuning Surface", desc: "Static vs dynamic vs per-topic configs, precedence, and the knobs that actually matter, with interactions." },
    "op02-limits":             { num: "II·02", title: "Limits & Boundaries",           desc: "Hard, soft, and emergent limits: partitions, request/message size, the 1 GiB/s-per-topic question, FDs, 2 GB ceilings." },
    "op03-partitioning":       { num: "II·03", title: "Partitioning Strategy",         desc: "How many partitions, per-partition ceilings, the real cost of partitions, when to reshard, the repartitioning trap." },
    "op04-capacity-planning":  { num: "II·04", title: "Capacity Planning & Sizing",    desc: "The throughput, disk, memory, and network formulas; replication amplification; estimating broker count." },
    "op05-performance-tuning": { num: "II·05", title: "Performance Tuning",            desc: "Producer/consumer/broker knobs, the end-to-end latency budget, page cache and zero-copy; throughput ⇄ latency." },
    "op06-durability":         { num: "II·06", title: "Durability, Availability & Consistency", desc: "acks, min.insync.replicas, RF, unclean election, the replication-not-fsync philosophy, ELR." },
    "op07-failure-modes":      { num: "II·07", title: "Failure Modes & The Runbook",   desc: "URP, offline partitions, disk/quorum failure, ISR thrash, rebalance storms, hanging txns, cause→symptom→fix." },
    "op08-metrics-signals":    { num: "II·08", title: "Metrics, Signals & Observability", desc: "The golden signals, where each is emitted in source, alert thresholds, and a dashboard blueprint." },
    "op09-topologies":         { num: "II·09", title: "Topologies & Deployment",       desc: "Rack/multi-AZ, dedicated vs combined KRaft, multi-region replication, tiered-storage topology, tenancy." },
    "op10-cost":               { num: "II·10", title: "Cost Engineering",              desc: "Storage/network/cross-AZ cost drivers and the levers: compression, fetch-from-follower, tiered storage, RF." },
    "op11-scaling-scenarios":  { num: "II·11", title: "Scaling: 1M → 10M → 100M / sec",desc: "Worked capacity tiers, the bottleneck that emerges at each, what to watch, and where Kafka's limits bind." },
    "op12-lifecycle":          { num: "II·12", title: "Lifecycle Operations",          desc: "Rolling upgrades & metadata.version, reassignment & throttling, add/remove brokers, disaster recovery." },
    "op13-multitenancy":       { num: "II·13", title: "Multitenancy & Isolation",      desc: "What is shared vs isolated in a broker, and how different tenants raise each other's latency & error rates, the noisy-neighbour problem." },
    "op14-proactive-monitoring": { num: "II·14", title: "Proactive Monitoring: Leading Indicators", desc: "The leading indicators, trends and capacity-runway metrics that warn you a cluster will suffer, with lead time, before the lagging alerts fire. Includes client-team-side signals." },

    // ---- Part III, The Log as a Blueprint ----
    "bp00-log-pattern":        { num: "III·00", title: "The Distributed Log as a Pattern", desc: "The pattern abstracted from Kafka, its invariants and the universal problems a replicated log solves." },
    "bp01-when-to-use":        { num: "III·01", title: "When to Use the Log, and When Not To", desc: "A decision framework: the forces for and against, and the anti-patterns (log-as-DB / -as-RPC / -as-queue)." },
    "bp02-design-decisions":   { num: "III·02", title: "Design Decisions & Alternatives", desc: "Each Kafka choice as a tradeoff: pull vs push, ISR vs quorum (and why both), page cache vs managed memory, segments vs LSM." },
    "bp03-inherent-limits":    { num: "III·03", title: "Inherent Limitations",         desc: "Where the pattern structurally falls short, ordering, the partition ceiling, no per-message routing/TTL, the latency floor." },
    "bp04-tactics-toolkit":    { num: "III·04", title: "The Tactics Toolkit",          desc: "Reusable engineering tactics generalized: mechanical sympathy, epoch fencing, everything-is-a-log, timing wheels, MVCC timelines." },
    "bp05-evolution":          { num: "III·05", title: "Evolution as Case Studies",    desc: "ZooKeeper→KRaft, the message format, EOS, rebalancing, tiered storage, queues, lessons in evolving a distributed system." },
    "bp06-comparative":        { num: "III·06", title: "Comparative Architecture",     desc: "Kafka vs Pulsar, Redpanda, Kinesis, Pub/Sub, RabbitMQ, and diskless designs, what each tradeoff teaches." },
    "bp07-architect-cheatsheet": { num: "III·07", title: "The Architect's Cheat Sheet", desc: "Consolidated design dials, decision trees, and heuristics for choosing and tuning a log-based architecture." }
  };

  var PARTS = [
    { id: "I", title: "Part I · Architecture Internals", groups: [
      { title: "Getting Started",                 items: ["00-overview"] },
      { title: "Data Format",                     items: ["01-record-format", "02-wire-protocol"] },
      { title: "Storage Engine",                  items: ["03-storage-log-engine", "04-storage-management", "05-tiered-storage"] },
      { title: "Networking & API",                items: ["06-network-and-threading", "07-request-processing"] },
      { title: "Replication & Cluster",           items: ["08-replication", "09-fetch-path"] },
      { title: "KRaft & Metadata",                items: ["10-kraft-consensus", "11-kraft-controller", "12-metadata-propagation"] },
      { title: "Coordination",                    items: ["13-group-coordination", "14-transactions-eos", "15-share-groups"] },
      { title: "Clients",                         items: ["16-producer-client", "17-consumer-client"] },
      { title: "Cross-Cutting",                   items: ["18-security", "19-quotas"] },
      { title: "Streams & Connect",               items: ["20-kafka-streams", "21-kafka-connect"] },
      { title: "Reference",                       items: ["glossary"] }
    ]},
    { id: "II", title: "Part II · Operations Manual", groups: [
      { title: "Foundations",                     items: ["op00-operator-model", "op01-configuration", "op02-limits"] },
      { title: "Sizing & Performance",            items: ["op03-partitioning", "op04-capacity-planning", "op05-performance-tuning", "op06-durability"] },
      { title: "Running in Production",           items: ["op07-failure-modes", "op08-metrics-signals", "op09-topologies"] },
      { title: "Scale & Economics",               items: ["op10-cost", "op11-scaling-scenarios", "op12-lifecycle"] },
      { title: "Advanced Operations",             items: ["op13-multitenancy", "op14-proactive-monitoring"] }
    ]},
    { id: "III", title: "Part III · The Log as a Blueprint", groups: [
      { title: "The Pattern",                     items: ["bp00-log-pattern", "bp01-when-to-use"] },
      { title: "Design & Tradeoffs",              items: ["bp02-design-decisions", "bp03-inherent-limits"] },
      { title: "For the Architect",               items: ["bp04-tactics-toolkit", "bp05-evolution", "bp06-comparative", "bp07-architect-cheatsheet"] }
    ]}
  ];

  var M = { PAGES: PAGES, PARTS: PARTS, PROVENANCE: "Apache Kafka 4.4.0-SNAPSHOT · git 04bfe7d · 2026-06-15" };
  if (typeof window !== "undefined") window.KAFKA_DOCS = M;
  if (typeof module !== "undefined" && module.exports) module.exports = M;
})();
