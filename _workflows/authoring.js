export const meta = {
  name: 'kafka-arch-authoring',
  description: 'Author 21 source-derived Kafka architecture chapters (HTML), each adversarially fact-checked against the source tree',
  phases: [
    { title: 'Author', detail: 'one agent per subsystem reads real source and writes an HTML fragment' },
    { title: 'Verify', detail: 'adversarial fact-check of each chapter against the source code' }
  ]
};

const ROOT = '/Users/user/projects/_playground/apache-kafka-architecture/kafka-source';
const OUT  = '/Users/user/projects/_playground/apache-kafka-architecture';

const CHAPTERS_LIST = [
  '00-overview.html — Architecture Overview',
  '01-record-format.html — Record Format & Batches',
  '02-wire-protocol.html — Wire Protocol & RPC Framework',
  '03-storage-log-engine.html — The Log Storage Engine',
  '04-storage-management.html — Log Management, Retention & Compaction',
  '05-tiered-storage.html — Tiered Storage (Remote Log)',
  '06-network-and-threading.html — Network Layer & Threading Model',
  '07-request-processing.html — Request Processing (KafkaApis)',
  '08-replication.html — Replication, ISR & High Watermark',
  '09-fetch-path.html — The Fetch Path & Replica Fetchers',
  '10-kraft-consensus.html — KRaft Consensus (Raft)',
  '11-kraft-controller.html — The KRaft Controller',
  '12-metadata-propagation.html — Metadata Propagation & Broker Lifecycle',
  '13-group-coordination.html — Group Coordination & Rebalance Protocols',
  '14-transactions-eos.html — Transactions & Exactly-Once Semantics',
  '15-share-groups.html — Share Groups (Queues / KIP-932)',
  '16-producer-client.html — The Producer Client',
  '17-consumer-client.html — The Consumer Client',
  '18-security.html — Security: Authentication & Authorization',
  '19-quotas.html — Quotas, Throttling & Client Metrics',
  '20-kafka-streams.html — Kafka Streams Architecture',
  '21-kafka-connect.html — Kafka Connect Architecture',
  'glossary.html — Glossary & Cross-Cutting Concepts'
].join('\n');

const HOUSE =
'You are a principal distributed-systems engineer and meticulous technical writer producing ORIGINAL, deeply technical architecture documentation for Apache Kafka, version 4.4.0-SNAPSHOT (git commit 04bfe7d, dated 2026-06-15). This Kafka is KRaft-only: ZooKeeper has been fully removed (since 4.0); cluster metadata lives in a Raft-replicated metadata log managed by a controller quorum. Do not describe ZooKeeper as a current dependency; mention it only as historical context if relevant.\n\n' +
'GROUND TRUTH = THE SOURCE CODE. Your single source of truth is the actual Java/Scala source under ' + ROOT + '. READ the real files before writing. Every concrete claim — class names, method names, field names, configuration keys, default values, algorithm steps, byte layouts, state transitions, thread names — must be grounded in code you actually opened and read. If you cannot confirm something from the source, read more or omit it; explicitly mark unavoidable inferences as "(inferred)".\n\n' +
'DO NOT COPY EXISTING DOCS. Do NOT read or paraphrase the prose under ' + ROOT + '/docs or on kafka.apache.org. Derive every explanation from the code itself, in your own words. You MAY run up to TWO targeted web searches ONLY to confirm a KIP number or a one-line design rationale for a "Design rationale" callout; never copy documentation prose; cite KIPs by number.\n\n' +
'CITATIONS. Cite source locations inline as path/File.ext:line relative to the kafka-source root, e.g. core/src/main/scala/kafka/server/KafkaApis.scala:412. Cite a specific file (and line when you can) for every important claim. If a hinted path is stale, use grep/glob/find to locate the correct file — never guess a path.\n\n' +
'DEPTH — top-level to nitty-gritty. Cover: purpose & role; the key classes/interfaces and how they collaborate; the in-memory and on-disk/on-wire data structures and their fields; the precise algorithms step by step; the concurrency & threading model (which thread does what; which locks/queues/atomics guard which state); configuration knobs with exact names, defaults and effects; failure modes, edge cases and recovery; invariants & guarantees; and how this subsystem interacts with the others. Prefer depth and precision over breadth of name-dropping.\n\n' +
'ACCURACY OVER EVERYTHING — above completeness, above length. A smaller set of verified, precise statements beats a long plausible-sounding narrative. Never invent APIs or defaults.\n\n' +
'LENGTH: aim for roughly 3,000–6,000 words of dense, correct, well-structured content. This is a reference chapter; be thorough.\n\n' +
'SECTION SHAPE (adapt sensibly to the subsystem; not every heading applies): an abstract; role & responsibilities; where it lives in the code (a small table of principal classes + file paths); core concepts & terminology; data structures (in-memory + persistent, field-level, with byte layouts where relevant); architecture & control/data flow (with diagrams); detailed mechanics — the actual algorithms with citations; concurrency & threading; configuration reference (table: key, default, effect); failure modes, edge cases & recovery; invariants & guarantees; interactions with other subsystems (cross-link siblings); design rationale & evolution (relevant KIPs, why it is built this way); and gotchas / operational notes.';

const HTML_GUIDE =
'OUTPUT FORMAT — HTML FRAGMENT ONLY. Write ONLY the inner HTML that belongs inside the page\'s <article> element. Do NOT emit <html>, <head>, <body>, <nav>, <aside>, or <script> — a build step wraps your fragment in the site skeleton and auto-generates the left sidebar and the right "on this page" table of contents from your headings.\n\n' +
'Your fragment MUST, in order:\n' +
'1. Start with <h1> whose text is exactly the provided "NN · Title".\n' +
'2. Then a provenance line: <blockquote class="provenance">Source: Apache Kafka 4.4.0-SNAPSHOT (git 04bfe7d, 2026-06-15), KRaft mode. Derived from source code, not copied from official documentation.</blockquote>\n' +
'3. Then a one-paragraph abstract as <p class="lead">…</p>.\n' +
'4. Then the body: <h2> for top-level sections, <h3>/<h4> for nested ones. Do NOT put id attributes on headings (the runtime adds them).\n\n' +
'Building blocks (use standard, well-formed HTML):\n' +
'- Text: <p>, <ul>/<ol>/<li>, <strong>, <em>; inline code/identifiers/config-keys/paths in <code>…</code>.\n' +
'- Source citations inline as <code>path/File.java:123</code> (relative to the kafka-source root). For a lighter visual you may use <span class="cite">path:line</span>.\n' +
'- Tables: <div class="table-wrap"><table><thead><tr><th>…</th></tr></thead><tbody><tr><td>…</td></tr></tbody></table></div>. Use them for config references, field/byte layouts, RPC lists, and state tables.\n' +
'- ASCII/box diagrams: <figure class="diagram"><pre>…</pre><figcaption>caption</figcaption></figure> (or a bare <pre class="diagram">…</pre>). Use diagrams generously for data flow, state machines, threading, request lifecycles, and on-disk/byte layouts.\n' +
'- Short code/structure/pseudocode snippets: <pre><code>…</code></pre>. Synthesize; never paste large source dumps (a few lines max).\n' +
'- Callouts (use them to spotlight the important things):\n' +
'    <div class="callout key"><span class="callout-title">Key idea</span><p>…</p></div>\n' +
'    <div class="callout rationale"><span class="callout-title">Design rationale</span><p>… cite KIP-NNN</p></div>\n' +
'    <div class="callout invariant"><span class="callout-title">Invariant</span><p>…</p></div>\n' +
'    <div class="callout gotcha"><span class="callout-title">Gotcha</span><p>…</p></div>\n' +
'    <div class="callout warning"><span class="callout-title">Caution</span><p>…</p></div>\n' +
'    <div class="callout note"><span class="callout-title">Note</span><p>…</p></div>\n' +
'- Term/value grids: <dl class="kv"><dt>name</dt><dd>meaning</dd></dl>. KIP badges: <span class="pill kip">KIP-848</span>.\n' +
'- Cross-links to sibling chapters: <a href="NN-slug.html">Title</a>. Link generously wherever subsystems interact.\n\n' +
'CRITICAL — DIAGRAM/ENTITY SAFETY. Inside <pre> and <code>, the characters < > & are special. For diagrams, PREFER Unicode box-drawing and arrows so you avoid them entirely: ─ │ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼ ═ ║ ╔ ╗ ╚ ╝ → ← ↑ ↓ ⇄ ▶ ◀ ● ○ ✓ ✗. If you ever must include a literal < > or & (e.g. Java generics like List&lt;Foo&gt; or a "<--" arrow), you MUST escape them as &lt; &gt; &amp;. Never leave a raw < inside text/pre/code — it will break the page. Keep diagram lines under ~92 characters. Produce well-formed HTML: every tag closed, every attribute quoted.';

const CHAPTERS_NOTE = 'The full chapter set (for cross-linking by filename):\n' + CHAPTERS_LIST;

const SUBSYSTEMS = [
  { n: '01', slug: 'record-format', title: 'The Record Format & Batches',
    focus: 'The on-disk and on-wire byte format of Kafka data. The v2 record batch (DefaultRecordBatch) header field-by-field (baseOffset, batchLength, partitionLeaderEpoch, magic, CRC, attributes bitfield incl. compression/timestampType/isTransactional/isControl, lastOffsetDelta, base/maxTimestamp, producerId/producerEpoch/baseSequence, records count); the per-record varint/zigzag encoding (DefaultRecord: length, attributes, timestampDelta, offsetDelta, key, value, headers); legacy v0/v1 records (AbstractLegacyRecordBatch) and why v2 exists; control batches & control records (ControlRecordType, EndTransactionMarker) used for txn markers/leader-change; compression codecs; CRC32C; how MemoryRecords/MemoryRecordsBuilder build batches and FileRecords/FileLogInputStream read them; relationship to idempotence/transactions (PID/epoch/sequence live in the batch header).',
    files: 'clients/src/main/java/org/apache/kafka/common/record/internal/{DefaultRecordBatch.java, DefaultRecord.java, AbstractLegacyRecordBatch.java, LegacyRecord.java, MemoryRecords.java, MemoryRecordsBuilder.java, FileRecords.java, FileLogInputStream.java, RecordBatch.java, ControlRecordType.java, EndTransactionMarker.java, DefaultRecordsSend.java}; clients/src/main/java/org/apache/kafka/common/compress/* (compression); look for the byte-offset constants at the top of DefaultRecordBatch.java.' },

  { n: '02', slug: 'wire-protocol', title: 'The Wire Protocol & RPC Framework',
    focus: 'How Kafka RPCs are framed and evolved. The 4-byte length-prefixed frame; request header (apiKey, apiVersion, correlationId, clientId; header v2 tagged fields) and response header; ApiKeys enum and how each API maps to request/response schemas; the message generator (the *.json schemas under clients common/message compiled to Java ApiMessage/Message classes by the generator module) and the Readable/Writable/ByteBufferAccessor (de)serialization; FLEXIBLE versions, compact arrays/strings, optional tagged fields (KIP-482); version negotiation via ApiVersions (KIP-35) and ApiVersionsResponse; the Errors enum & error codes; how AbstractRequest/AbstractResponse and RequestHeader/ResponseHeader work; throttling fields. Show a worked byte layout of a small request.',
    files: 'clients/src/main/java/org/apache/kafka/common/protocol/{ApiKeys.java, Protocol.java, Message.java, ApiMessage.java, Readable.java, Writable.java, ByteBufferAccessor.java, SendBuilder.java, Errors.java, MessageUtil.java}; clients/src/main/java/org/apache/kafka/common/requests/{AbstractRequest.java, AbstractResponse.java, RequestHeader.java, ResponseHeader.java, ApiVersionsRequest.java, RequestUtils.java}; clients/src/main/resources/common/message/{ProduceRequest.json, FetchRequest.json, ApiVersionsResponse.json, RequestHeader.json, ResponseHeader.json}; generator/src/main/java/org/apache/kafka/message/* (the code generator).' },

  { n: '03', slug: 'storage-log-engine', title: 'The Log Storage Engine',
    focus: 'The per-partition log. UnifiedLog over LocalLog over an ordered set of LogSegments; the active segment; segment file naming by baseOffset; the index files — OffsetIndex (relative offset -> physical position), TimeIndex (timestamp -> offset), TransactionIndex (aborted txns) — all built on AbstractIndex memory-mapped sparse indexes; the append path (LogValidator validation/assigning offsets/timestamps, leader epoch stamping) and the read/fetch-by-offset path (lookup -> segment.read); log start offset, recovery point/flush, LEO, high watermark interplay (cross-ref replication); segment rolling triggers (size/time/index full); recovery & LogLoader on startup (sanity, rebuild indexes, truncate to recovery); ProducerStateManager (idempotency dedup state, snapshots, .snapshot files) and LeaderEpochFileCache (leader-epoch -> start offset checkpoint). On-disk directory layout of a partition.',
    files: 'storage/src/main/java/org/apache/kafka/storage/internals/log/{UnifiedLog.java, LocalLog.java, LogSegment.java, LogSegments.java, AbstractIndex.java, OffsetIndex.java, TimeIndex.java, TransactionIndex.java, LazyIndex.java, LogLoader.java, LogValidator.java, ProducerStateManager.java, LogConfig.java, OffsetPosition.java}; storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java; storage/src/main/java/org/apache/kafka/storage/internals/checkpoint/*.' },

  { n: '04', slug: 'storage-management', title: 'Log Management, Retention & Compaction',
    focus: 'Broker-wide log lifecycle. LogManager: the registry of all UnifiedLogs across log.dirs (JBOD multi-dir), log creation/deletion, the recovery thread pool on startup, the background schedulers (flush-dirty, checkpoint recovery points & log-start-offsets, retention cleanup), LogDirFailureChannel & handling a failed disk. Retention: by time (retention.ms) and size (retention.bytes), deleting whole segments, the async delete (.deleted rename) and file.delete.delay.ms. Log compaction: cleanup.policy=compact; the LogCleaner pool of CleanerThreads; LogCleanerManager choosing the dirtiest log (cleaner checkpoint, dirty ratio); the two-pass Cleaner (build an OffsetMap/SkimpyOffsetMap of key->latest offset over the dirty section, then recopy segments keeping only the latest record per key); tombstones (null value) and delete.retention.ms; the cleaner point / first uncleanable offset; min.cleanable.dirty.ratio, min.compaction.lag.ms, max.compaction.lag.ms; interaction with transactions/markers.',
    files: 'storage/src/main/java/org/apache/kafka/storage/internals/log/{LogManager.java, LogCleaner.java, Cleaner.java, LogCleanerManager.java, LogCleaningState.java, OffsetMap.java, SkimpyOffsetMap.java, CleanerConfig.java, LogDirFailureChannel.java, LogToClean.java}; storage/src/main/java/org/apache/kafka/storage/internals/checkpoint/{OffsetCheckpointFile.java, CleanerCheckpoint*}. Cross-ref 03 for segment internals.' },

  { n: '05', slug: 'tiered-storage', title: 'Tiered Storage (Remote Log)',
    focus: 'KIP-405 tiered storage. The two SPIs: RemoteStorageManager (put/fetch segment + indexes to/from object store) and RemoteLogMetadataManager (track RemoteLogSegmentMetadata). RemoteLogManager on the broker: the copy task (leader copies eligible segments past the local retention to remote, updates metadata, advances log-start/highest-copied), expiration of remote segments by remote.retention, and the remote read path (serving a Fetch whose offset is below the local log start by streaming from remote, RemoteLogInputStream). RemoteIndexCache (caching remote offset/time/txn indexes locally). The default metadata impl TopicBasedRemoteLogMetadataManager backed by the internal __remote_log_metadata topic (ConsumerTask/RemoteLogMetadataCache, ProducerManager). Remote read quotas. enable per topic; local vs remote retention; copier/expiration thread pools.',
    files: 'storage/src/main/java/org/apache/kafka/server/log/remote/storage/{RemoteLogManager.java, RemoteStorageManager.java, RemoteLogMetadataManager.java, RemoteLogSegmentMetadata.java, RemoteLogSegmentId.java, RemoteLogManagerConfig.java}; storage/src/main/java/org/apache/kafka/server/log/remote/metadata/storage/{TopicBasedRemoteLogMetadataManager.java, RemoteLogMetadataCache.java, ConsumerTask.java}; storage/src/main/java/org/apache/kafka/storage/internals/log/RemoteIndexCache.java; storage/src/main/java/org/apache/kafka/server/log/remote/quota/*. Cross-ref 03/04/09.' },

  { n: '06', slug: 'network-and-threading', title: 'Network Layer & Threading Model',
    focus: 'The broker network stack and the thread anatomy of a broker. SocketServer with per-listener Acceptor threads (accept connections, round-robin to Processors) and Processor threads (the NIO reactor: register channels, read requests, enqueue to RequestChannel, write responses); data-plane vs control-plane listeners; the shared common.network.Selector / KafkaChannel / TransportLayer (PlaintextTransportLayer, SslTransportLayer) and Authenticator; NetworkReceive (size-delimited read) and Send/NetworkSend; RequestChannel (the bounded request queue + response queues); the KafkaRequestHandler pool (num.io.threads) pulling from RequestChannel and dispatching to KafkaApis; ConnectionQuotas (max connections per ip/listener/broker, connection-creation rate); the request purgatory — DelayedOperationPurgatory with a hierarchical timing wheel (Timer/TimingWheel) and watcher lists keyed by DelayedOperationKey — used by produce/fetch/etc.; the full request lifecycle timeline (queue time, local time, remote time, response queue time, throttle). Enumerate broker thread types.',
    files: 'core/src/main/scala/kafka/network/{SocketServer.scala, RequestChannel.scala}; core/src/main/scala/kafka/server/KafkaRequestHandler.scala; clients/src/main/java/org/apache/kafka/common/network/{Selector.java, KafkaChannel.java, TransportLayer.java, PlaintextTransportLayer.java, NetworkReceive.java, NetworkSend.java}; server-common/src/main/java/org/apache/kafka/server/purgatory/{DelayedOperationPurgatory.java, DelayedOperation.java, DelayedOperationKey.java}; server-common/src/main/java/org/apache/kafka/server/util/timer/* (TimingWheel/Timer — grep). Cross-ref 07.' },

  { n: '07', slug: 'request-processing', title: 'Request Processing (KafkaApis)',
    focus: 'How a broker turns a request into a response. KafkaApis.handle as the giant dispatch on apiKey; the handler contract (authorize, validate, act, build response, possibly via purgatory for produce/fetch); a few representative handlers walked end-to-end: Produce (-> ReplicaManager.appendRecords -> DelayedProduce for acks=all), Fetch (-> ReplicaManager.fetchMessages -> DelayedFetch), Metadata (-> MetadataCache), InitProducerId/AddPartitionsToTxn/offset APIs delegating to coordinators; ApiVersions handling & the ApiVersionManager (which APIs/versions this node exposes, enabled by features); the split between broker APIs (KafkaApis) and controller APIs (ControllerApis on the KRaft controller node); authorization hooks (Authorizer) and request-context/principal; throttling integration (quota manager -> throttle time in response); error mapping. The RequestHandler->KafkaApis->RequestChannel.sendResponse round trip.',
    files: 'core/src/main/scala/kafka/server/{KafkaApis.scala, ControllerApis.scala, KafkaRequestHandler.scala, ApiVersionManager.scala}; server/src/main/java/org/apache/kafka/server/purgatory/{DelayedProduce.java}; core/src/main/scala/kafka/network/RequestChannel.scala. Cross-ref 06, 08, 09, 13, 14.' },

  { n: '08', slug: 'replication', title: 'Replication, ISR & High Watermark',
    focus: 'The heart of Kafka durability. ReplicaManager (per-broker owner of partitions, applies metadata to become leader/follower via applyDelta/makeLeaders/makeFollowers, drives appends/fetches, manages fetchers and the high-watermark checkpoint); Partition (per-partition state machine: leader/follower, the assignment, the ISR set, leaderEpoch, the local UnifiedLog, remoteReplicas with their LEO/lastFetch); the ISR (in-sync replicas) and how it shrinks (replica.lag.time.max.ms) and expands; the High Watermark = min LEO over ISR, how it advances and is propagated to followers; leader epochs and epoch-based truncation (KIP-101/279, OffsetsForLeaderEpoch); acks=0/1/all and min.insync.replicas durability; the AlterPartition RPC flow to the controller to commit ISR changes (AlterPartitionManager) and why ISR is controller-authoritative in KRaft; unclean leader election; Eligible Leader Replicas (KIP-966, ELR) if present; LogOffsetMetadata (HW/LEO/LSO). The produce path acks=all completion via the high watermark.',
    files: 'core/src/main/scala/kafka/server/ReplicaManager.scala; core/src/main/scala/kafka/cluster/{Partition.scala, Replica.scala}; storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java; metadata/src/main/java/org/apache/kafka/metadata/PartitionRegistration.java; look for AlterPartitionManager (grep core/server). Cross-ref 03, 09, 11, 12.' },

  { n: '09', slug: 'fetch-path', title: 'The Fetch Path & Replica Fetchers',
    focus: 'How data moves to followers and consumers. Follower replication: AbstractFetcherThread / ReplicaFetcherThread pull from the leader via the Fetch API in a loop (build fetch -> process responses -> append to local log -> advance follower HW from leader response); AbstractFetcherManager partitions work across num.replica.fetchers threads keyed by (leader, fetcherId); LocalLeaderEndPoint vs RemoteLeaderEndPoint; truncation handshake on becoming follower (OffsetsForLeaderEpoch -> truncate to leader epoch boundary) and the Tiered/diverging-epoch handling; ReplicaAlterLogDirsThread for intra-broker JBOD moves. Consumer/leader fetch serving: ReplicaManager.fetchMessages, reading from the log, DelayedFetch in purgatory (wait for min.bytes or max.wait.ms), HW vs LSO bound (read_uncommitted vs read_committed), fetch-from-follower for rack locality (KIP-392, replica selector). Incremental fetch sessions (KIP-227): FetchSession/FetchSessionCache, sessionId/epoch, only sending changed partitions. Zero-copy via FileRecords/sendfile.',
    files: 'core/src/main/scala/kafka/server/{AbstractFetcherThread.scala, AbstractFetcherManager.scala, ReplicaFetcherThread.scala, ReplicaFetcherManager.scala, LocalLeaderEndPoint.scala, RemoteLeaderEndPoint.scala, ReplicaAlterLogDirsThread.scala}; server/src/main/java/org/apache/kafka/server/FetchSession.java; look for DelayedFetch + replica selector (grep). Cross-ref 03, 08.' },

  { n: '10', slug: 'kraft-consensus', title: 'KRaft Consensus (Raft)',
    focus: 'Kafka\'s own Raft implementation underpinning the metadata quorum. KafkaRaftClient as an event-driven Raft state machine (poll loop, no blocking); the roles & QuorumState transitions — Unattached, Voted, Prospective (pre-vote KIP-996), Candidate, Follower, Leader, Resigned — and the persisted quorum-state file; leader election (Vote / BeginQuorumEpoch / EndQuorumEpoch RPCs, epoch monotonicity, randomized timeouts); PULL-based log replication (followers/observers send Fetch to the leader; the leader never pushes) and how this differs from classic Raft; the metadata log (KafkaRaftLog over the same segment storage) and BatchAccumulator/BatchBuilder building control+data batches; commit via the high watermark = majority replication; snapshots (KIP-630: FetchSnapshot RPC, generating/loading snapshots, log truncation to a snapshot, KRaftControlRecordStateMachine for control records like LeaderChange/SnapshotHeader); observers (brokers) vs voters (controllers); dynamic quorum reconfiguration (KIP-853: AddVoter/RemoveVoter/UpdateVoter, VoterSet, kraft.version); listener notifications to the controller state machine; RequestManager & connection backoff.',
    files: 'raft/src/main/java/org/apache/kafka/raft/{KafkaRaftClient.java, QuorumState.java, LeaderState.java, FollowerState.java, CandidateState.java, ProspectiveState.java, UnattachedState.java, VotedState.java, VoterSet.java, RequestManager.java, ReplicatedLog.java}; raft/src/main/java/org/apache/kafka/raft/internals/{KafkaRaftLog.java, BatchAccumulator.java, BatchBuilder.java, KRaftControlRecordStateMachine.java, RecordsIterator.java, AddVoterHandler.java}; raft/src/main/resources/common/message/{VoteRequest.json, FetchRequest? (raft uses Fetch), BeginQuorumEpochRequest.json, FetchSnapshotRequest.json}. Cross-ref 11, 12.' },

  { n: '11', slug: 'kraft-controller', title: 'The KRaft Controller',
    focus: 'The active controller as a deterministic replicated state machine over the metadata log. QuorumController: a single-threaded event loop (KafkaEventQueue) processing controller events; every mutation is computed as a list of metadata records, appended to the Raft log via the RaftClient, and only APPLIED to in-memory state once committed (the write->commit->apply discipline) so the active and standby controllers stay identical; the control managers each owning a slice of state — ReplicationControlManager (topics/partitions/ISR/leader election/AlterPartition), ClusterControlManager (broker registrations/fencing), ConfigurationControlManager (dynamic configs), FeatureControlManager (metadata.version & features, KIP-584), ProducerIdControlManager (PID block allocation), AclControlManager, ScramControlManager, DelegationTokenControlManager; the timeline data structures (SnapshotRegistry + TimelineHashMap/TimelineHashSet/TimelineLong) that keep multiple committed offsets\' views so reads are consistent and uncommitted state can be reverted on failover; deferred/uncommitted events & the purgatory of pending records; BrokerHeartbeatManager (liveness, fencing, controlled shutdown) and PartitionChangeBuilder (recomputing leader/ISR on broker up/down); replica placement (StripedReplicaPlacer); periodic tasks (e.g. leader balancing, unclean recovery). Contrast with the old ZK controller.',
    files: 'metadata/src/main/java/org/apache/kafka/controller/{QuorumController.java, Controller.java, ReplicationControlManager.java, ClusterControlManager.java, ConfigurationControlManager.java, FeatureControlManager.java, ProducerIdControlManager.java, BrokerHeartbeatManager.java, PartitionChangeBuilder.java, OffsetControlManager.java}; metadata/src/main/java/org/apache/kafka/metadata/placement/StripedReplicaPlacer.java; server-common/src/main/java/org/apache/kafka/timeline/{SnapshotRegistry.java, TimelineHashMap.java, SnapshottableHashTable.java}; server-common/src/main/java/org/apache/kafka/queue/KafkaEventQueue.java. Cross-ref 10, 12.' },

  { n: '12', slug: 'metadata-propagation', title: 'Metadata Propagation & Broker Lifecycle',
    focus: 'How committed metadata reaches brokers and how brokers join/leave. The metadata log (__cluster_metadata, single partition) as the ordered record stream; MetadataImage (a complete immutable snapshot composed of TopicsImage, ClusterImage, ConfigurationsImage, etc.) and MetadataDelta (incremental changes applied to produce the next image); MetadataLoader consuming committed records/snapshots from the RaftClient and publishing each new image to MetadataPublishers; on the broker, BrokerMetadataPublisher applies image deltas to ReplicaManager/GroupCoordinator/etc., and KRaftMetadataCache serves Metadata requests from the latest image; metadata.version / feature levels (KIP-584) gating record formats and behavior; broker lifecycle: BrokerLifecycleManager registers the broker (BrokerRegistration), sends periodic BrokerHeartbeat to the controller, transitions fenced->active->controlled-shutdown, and how fencing/unfencing affects leadership; AssignmentsManager (directory/JBOD assignment intents to the controller); the broker\'s catch-up on startup (replay metadata to current before serving).',
    files: 'metadata/src/main/java/org/apache/kafka/image/{MetadataImage.java, MetadataDelta.java, TopicsImage.java, TopicDelta.java, ClusterImage.java, loader/MetadataLoader.java, publisher/*}; metadata/src/main/java/org/apache/kafka/metadata/{KRaftMetadataCache.java, PartitionRegistration.java, BrokerRegistration.java}; core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala; server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java; server/src/main/java/org/apache/kafka/server/AssignmentsManager.java; server-common MetadataVersion (grep). Cross-ref 10, 11, 08.' },

  { n: '13', slug: 'group-coordination', title: 'Group Coordination & Rebalance Protocols',
    focus: 'Consumer group membership, assignment and offsets — the modern Java coordinator. The new GroupCoordinator running as a replicated state machine via CoordinatorRuntime over the internal __consumer_offsets partitions (each partition = one coordinator shard; the group/offset records are written to the log and replayed to build in-memory state; __consumer_offsets is the coordinator log for a group hashed by groupId). GroupCoordinatorService -> GroupCoordinatorShard -> GroupMetadataManager (group state, members) + OffsetMetadataManager (committed offsets). The CLASSIC protocol: find-coordinator, JoinGroup (leader picks assignment) / SyncGroup, generations, heartbeats, session.timeout, the rebalance dance, eager vs cooperative-incremental rebalancing (KIP-429) and partition revocation. The NEW consumer group protocol (KIP-848): fully server-side, incremental ConsumerGroupHeartbeat; the broker computes target assignment with a server-side assignor (UniformAssignor / RangeAssignor) and members reconcile toward it via epochs (member epoch, group epoch, assignment epoch) — no global sync barrier; ConsumerGroup/modern records. Offset management: OffsetCommit/OffsetFetch, the __consumer_offsets record schemas (offset commit value, group metadata value), retention. Mention StreamsGroup (KIP-1071) and static membership (group.instance.id, KIP-345). Coordinator failover = replay the partition.',
    files: 'group-coordinator/src/main/java/org/apache/kafka/coordinator/group/{GroupCoordinatorService.java, GroupCoordinatorShard.java, GroupMetadataManager.java, OffsetMetadataManager.java, GroupCoordinatorRecordHelpers.java, GroupConfig.java}; group-coordinator/.../group/classic/ClassicGroup.java; group-coordinator/.../group/modern/consumer/ConsumerGroup.java; group-coordinator/.../group/assignor/* (UniformAssignor etc.); coordinator-common/.../CoordinatorRuntime.java (grep); clients consumer assignors clients/src/main/java/org/apache/kafka/clients/consumer/{RangeAssignor.java, CooperativeStickyAssignor.java}. Cross-ref 17, 14.' },

  { n: '14', slug: 'transactions-eos', title: 'Transactions & Exactly-Once Semantics',
    focus: 'Idempotence and transactions end-to-end. The idempotent producer: a Producer ID (PID) + producer epoch + per-partition monotonic sequence number stamped in each batch header; the broker (ProducerStateManager) dedups/validates sequences to give exactly-once-in-order per partition; InitProducerId allocates a PID (controller AllocateProducerIds block allocation via ProducerIdManager). Transactions (KIP-98): a transactional.id mapped to a PID with epoch fencing of zombies; the TransactionCoordinator + TransactionStateManager backed by the internal __transaction_state log; the TransactionMetadata state machine (Empty -> Ongoing -> PrepareCommit/PrepareAbort -> CompleteCommit/CompleteAbort) persisted as transaction log records; the protocol flow: InitProducerId (fence + abort dangling), AddPartitionsToTxn (register partitions in the txn), produce, AddOffsetsToTxn/TxnOffsetCommit (consume-transform-produce, KIP-447 sendOffsetsToTransaction binds consumer offsets into the txn), EndTxn -> two-phase commit: write PrepareCommit, then the coordinator writes transaction markers (WriteTxnMarkers -> EndTransactionMarker control records) to every involved partition (TransactionMarkerChannelManager), then CompleteCommit. The consumer side: read_committed isolation, the Last Stable Offset (LSO), the aborted-transaction index used to filter aborted records. Fencing, timeouts (transaction.timeout.ms), and recovery on coordinator failover.',
    files: 'core/src/main/scala/kafka/coordinator/transaction/{TransactionCoordinator.scala, TransactionStateManager.scala, TransactionMarkerChannelManager.scala, ProducerIdManager? }; transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/{TransactionMetadata.java, TransactionLog.java, TransactionState.java, RPCProducerIdManager.java}; storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java; clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java. Cross-ref 01, 03, 16, 13.' },

  { n: '15', slug: 'share-groups', title: 'Share Groups (Queues / KIP-932)',
    focus: 'Queues for Kafka — share groups, a queue-like consumption model alongside consumer groups. Concept: many share consumers in a share group can consume from the SAME partitions cooperatively (not one-partition-per-consumer); records are individually ACQUIRED with a time-bounded acquisition lock, then acknowledged as ACCEPTED / RELEASED / REJECTED, with a per-record delivery state and delivery count (redelivery), giving at-least-once queue semantics with per-record acks rather than offset commits. The broker SharePartition tracks in-flight record states and the share-partition start offset (SPSO); ShareFetch/ShareAcknowledge RPCs; the ShareConsumer client (ShareConsumerImpl + ShareConsumeRequestManager). The ShareCoordinator (ShareCoordinatorService/Shard) persists durable share-group state (the share-partition offset & state batches) to the internal __share_group_state topic via the persister SPI (DefaultStatePersister/PersisterStateManager). max delivery attempts and the Dead-Letter-Queue (ShareGroupDLQStateManager). acquisition lock timeout, max in-flight records/bytes. Contrast share groups vs consumer groups vs transactions.',
    files: 'share-coordinator/src/main/java/org/apache/kafka/coordinator/share/{ShareCoordinatorService.java, ShareCoordinatorShard.java, ShareGroupOffset.java, PersisterStateBatchCombiner.java}; server-common/src/main/java/org/apache/kafka/server/share/persister/{PersisterStateManager.java, DefaultStatePersister.java}; server/src/main/java/org/apache/kafka/server/share/dlq/ShareGroupDLQStateManager.java; clients/src/main/java/org/apache/kafka/clients/consumer/internals/{ShareConsumerImpl.java, ShareConsumeRequestManager.java}; look for SharePartition / SharePartitionManager (grep core or share). Cross-ref 13, 17.' },

  { n: '16', slug: 'producer-client', title: 'The Producer Client',
    focus: 'KafkaProducer from send() to broker ack. The send pipeline: serialize key/value, optional interceptors, partition selection (explicit partition, key-hash, or the sticky BuiltInPartitioner / partitioner.adaptive KIP-794/KIP-480), then append to the RecordAccumulator — per-(topic,partition) deque of ProducerBatch objects, backed by the BufferPool (buffer.memory, ready batches when batch.size full or linger.ms elapsed, max.block.ms back-pressure). The single background Sender (io) thread: drain ready batches grouped by leader broker, build ProduceRequests, send via NetworkClient (one in-flight pipeline per broker, max.in.flight.requests.per.connection), handle responses/retries/backoff. Ordering & idempotence: enable.idempotence default true, sequence numbers per partition, why max.in.flight<=5 keeps ordering, epoch bumps on fatal errors; acks (0/1/all) semantics; delivery.timeout.ms vs request.timeout.ms vs retries; compression (compression.type) at the batch level; the TransactionManager hooks for EOS; flush()/close() and how completion callbacks fire. Error classification (retriable vs fatal).',
    files: 'clients/src/main/java/org/apache/kafka/clients/producer/{KafkaProducer.java, ProducerConfig.java, ProducerRecord.java}; clients/src/main/java/org/apache/kafka/clients/producer/internals/{RecordAccumulator.java, ProducerBatch.java, BufferPool.java, Sender.java, BuiltInPartitioner.java, TransactionManager.java}; clients/src/main/java/org/apache/kafka/clients/NetworkClient.java. Cross-ref 01, 14, 02, 08.' },

  { n: '17', slug: 'consumer-client', title: 'The Consumer Client',
    focus: 'KafkaConsumer end-to-end, both implementations. The delegator KafkaConsumer chooses ClassicKafkaConsumer (legacy single-threaded: poll drives coordinator + fetch on the user thread) or the new AsyncKafkaConsumer (KIP-848 threading model: a user-facing application thread and a background ConsumerNetworkThread communicating via ApplicationEvent/BackgroundEvent queues, with RequestManagers — FetchRequestManager, CommitRequestManager, OffsetsRequestManager, HeartbeatRequestManager, the MembershipManager — driven by the network thread). SubscriptionState: subscription type (subscribe/assign/pattern), per-partition position/HW/LSO, paused state, reset strategy. The fetch pipeline: build fetches per node honoring fetch.min/max.bytes, max.partition.fetch.bytes, fetch.max.wait.ms; FetchBuffer/CompletedFetch decompression & iteration; max.poll.records; honoring isolation.level (LSO for read_committed) and skipping aborted batches via the aborted-txn list. Offset management: auto vs manual commit (enable.auto.commit, commitSync/Async), committed-offset fetch, auto.offset.reset; group membership & rebalance callbacks (ConsumerRebalanceListener), max.poll.interval.ms liveness; assign() (no group) vs subscribe(). position vs committed.',
    files: 'clients/src/main/java/org/apache/kafka/clients/consumer/{KafkaConsumer.java, ConsumerConfig.java}; clients/src/main/java/org/apache/kafka/clients/consumer/internals/{AsyncKafkaConsumer.java, ClassicKafkaConsumer.java, ConsumerCoordinator.java, AbstractCoordinator.java, SubscriptionState.java, CommitRequestManager.java, OffsetsRequestManager.java, AbstractMembershipManager.java, ConsumerNetworkThread.java, FetchCollector.java, FetchBuffer.java}; consumer/internals/events/*. Cross-ref 13, 09, 02.' },

  { n: '18', slug: 'security', title: 'Security: Authentication & Authorization',
    focus: 'Wire security and access control. Listeners & security protocols (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL); the pluggable channel: ChannelBuilders -> SaslChannelBuilder/SslChannelBuilder -> KafkaChannel with an SslTransportLayer (TLS handshake, mutual TLS, SslFactory/SslEngineFactory, keystore/truststore) and/or a SASL Authenticator (SaslServerAuthenticator/SaslClientAuthenticator) exchanging SaslHandshake+SaslAuthenticate over the connection. SASL mechanisms: PLAIN, SCRAM-SHA-256/512 (salted challenge-response, ScramCredential stored in KRaft metadata), GSSAPI/Kerberos, OAUTHBEARER (token validation, JWT). Delegation tokens (KIP-48). KafkaPrincipal & KafkaPrincipalBuilder. Authorization: the Authorizer SPI; the default StandardAuthorizer (KRaft) with StandardAuthorizerData holding ACL bindings sourced from metadata-log AccessControlEntryRecord (so ACLs are replicated like all metadata); ResourcePattern (LITERAL/PREFIXED), operations, ALLOW/DENY precedence (deny wins), super.users, allow.everyone.if.no.acl.found; the authorize() evaluation path called from KafkaApis. inter-broker security.',
    files: 'clients/src/main/java/org/apache/kafka/common/security/authenticator/{SaslServerAuthenticator.java, SaslClientAuthenticator.java}; clients/src/main/java/org/apache/kafka/common/security/scram/internals/*; clients/src/main/java/org/apache/kafka/common/security/oauthbearer/*; clients/src/main/java/org/apache/kafka/common/security/ssl/SslFactory.java; clients/src/main/java/org/apache/kafka/common/network/{SaslChannelBuilder.java, ChannelBuilders.java, SslTransportLayer.java}; metadata/src/main/java/org/apache/kafka/metadata/authorizer/{StandardAuthorizer.java, StandardAuthorizerData.java}; clients/src/main/java/org/apache/kafka/common/acl/*. Cross-ref 06, 07, 12.' },

  { n: '19', slug: 'quotas', title: 'Quotas, Throttling & Client Metrics',
    focus: 'Protecting the cluster from overload and pushing client telemetry. ClientQuotaManager and the quota types: produce-rate, fetch-rate, request-rate (request.percentage / IO-thread time), controller-mutation-rate (KIP-599), plus replication and alter-log-dirs throttles; quota entities and resolution order (user+client-id, user, client-id, defaults, IP connection-rate) configurable dynamically via ClientQuotaCallback / configs stored in metadata; the throttling mechanism — a token-bucket / sampled rate (the Sensor/Rate metrics), computing a throttle delay when over quota, holding the channel muted and returning throttle_time_ms so the client backs off (and ThrottledChannelReaper/the delay queue re-enabling the channel); ConnectionQuotas (connection count & creation rate). Client metrics / telemetry push (KIP-714): ClientMetricsManager, the GetTelemetrySubscriptions/PushTelemetry RPCs, broker-side metric subscriptions delivering OTLP from clients. How quota config flows through dynamic config & metadata.',
    files: 'server/src/main/java/org/apache/kafka/server/quota/{ClientQuotaManager.java, ClientRequestQuotaManager.java, ControllerMutationQuotaManager.java, ThrottledChannelReaper? }; core ReplicationQuotaManager (grep core/server); server/src/main/java/org/apache/kafka/server/ClientMetricsManager.java; ConnectionQuotas in core/src/main/scala/kafka/network/SocketServer.scala; common metrics Sensor/Rate (clients common/metrics). Cross-ref 06, 07.' },

  { n: '20', slug: 'kafka-streams', title: 'Kafka Streams Architecture',
    focus: 'The stream-processing library built on the consumer/producer. The topology: a DAG of source/processor/sink ProcessorNodes (InternalTopologyBuilder) built from the Processor API or the DSL (KStream/KTable/GlobalKTable in kstream); how the DSL compiles to processors and inserts repartition & changelog topics; sub-topologies and how the topology is split into tasks where a Task owns one partition-group (one partition per input topic) — StreamTask (processing) and StandbyTask (hot replica of state); the runtime: KafkaStreams -> N StreamThreads, each with its own consumer/producer; the StreamThread loop (poll -> add records to task buffers -> process -> punctuate -> commit); TaskManager assigning/creating/closing tasks; the StreamsPartitionAssignor (a custom consumer assignor doing co-partitioning, sticky assignment, and warmup/probing rebalances for state, KIP-441) and the newer Streams group protocol (KIP-1071 / StreamsGroup); state stores (KeyValue/Window/Session; RocksDB persistent or in-memory; the record cache; suppliers) and their CHANGELOG topics for fault tolerance, with StoreChangelogReader / DefaultStateUpdater restoring state on assignment; time (event/processing/ingestion), windowing, grace; processing guarantees (at_least_once vs exactly_once_v2 using the producer txns); interactive queries (IQv2).',
    files: 'streams/src/main/java/org/apache/kafka/streams/{KafkaStreams.java, Topology.java, StreamsBuilder.java, StreamsConfig.java}; streams/src/main/java/org/apache/kafka/streams/processor/internals/{StreamThread.java, TaskManager.java, StreamTask.java, StandbyTask.java, InternalTopologyBuilder.java, StreamsPartitionAssignor.java, StoreChangelogReader.java, DefaultStateUpdater.java, ProcessorStateManager.java, ProcessorNode.java}; streams/src/main/java/org/apache/kafka/streams/state/internals/* (RocksDBStore, caching). Cross-ref 17, 16, 13.' },

  { n: '21', slug: 'kafka-connect', title: 'Kafka Connect Architecture',
    focus: 'The framework for streaming data in/out of Kafka. The plugin SPIs: SourceConnector/SinkConnector + SourceTask/SinkTask (a Connector splits work into taskConfigs; Tasks do the IO); the Worker process that instantiates and runs Tasks in threads (WorkerSourceTask polls the source and produces; WorkerSinkTask consumes and puts to the sink); converters (key/value Converter, e.g. JSON/Avro via schemas) and Single Message Transforms (SMT) + predicates in the pipeline; standalone vs distributed mode. Distributed mode: the DistributedHerder coordinates a cluster of workers using Kafka\'s group membership with a Connect-specific protocol and the IncrementalCooperativeAssignor (KIP-415) to balance connectors/tasks with minimal disruption; the three internal compacted topics as the source of truth — config (KafkaConfigBackingStore), offsets (KafkaOffsetBackingStore: source-connector offsets), status (KafkaStatusBackingStore); rebalance on config/membership change; the REST API for management; exactly-once source (KIP-618) and SMT chains. MirrorMaker 2 (the connect/mirror module: MirrorSourceConnector/Heartbeat/Checkpoint) for cross-cluster replication.',
    files: 'connect/runtime/src/main/java/org/apache/kafka/connect/runtime/{Worker.java, AbstractHerder.java, WorkerSourceTask.java, WorkerSinkTask.java, AbstractWorkerSourceTask.java}; connect/runtime/.../runtime/distributed/{DistributedHerder.java, IncrementalCooperativeAssignor.java, WorkerCoordinator.java}; connect/runtime/.../storage/{KafkaConfigBackingStore.java, KafkaOffsetBackingStore.java, KafkaStatusBackingStore.java}; connect/api/src/main/java/org/apache/kafka/connect/{connector/*, source/*, sink/*}; connect/mirror/* (MirrorSourceConnector). Cross-ref 13, 16, 17.' }
];

const AUTHOR_SCHEMA = {
  type: 'object',
  properties: {
    fragmentPath: { type: 'string', description: 'absolute path of the HTML fragment you wrote' },
    title: { type: 'string' },
    abstract: { type: 'string', description: '2-4 sentence summary of the chapter' },
    sections: { type: 'array', items: { type: 'string' }, description: 'the h2 section titles in order' },
    keyFacts: { type: 'array', items: { type: 'string' },
      description: '6-12 self-contained, citable facts/claims about this subsystem for the overview synthesizer' },
    sourceFilesRead: { type: 'array', items: { type: 'string' }, description: 'main source files you actually read' },
    crossLinks: { type: 'array', items: { type: 'string' }, description: 'sibling chapter slugs referenced' },
    approxWords: { type: 'number' }
  },
  required: ['fragmentPath', 'title', 'abstract', 'sections', 'keyFacts']
};

const VERIFY_SCHEMA = {
  type: 'object',
  properties: {
    slug: { type: 'string' },
    fragmentExists: { type: 'boolean' },
    htmlWellFormed: { type: 'boolean', description: 'no raw unescaped < inside pre/code; tags balanced' },
    claimsChecked: { type: 'number' },
    accuracyScore: { type: 'number', description: '0-100, fraction of checked claims that are correct' },
    errors: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          severity: { type: 'string', enum: ['critical', 'major', 'minor'] },
          location: { type: 'string', description: 'section/heading or quoted phrase in the doc' },
          claim: { type: 'string', description: 'what the doc says' },
          correction: { type: 'string', description: 'what the source actually says, with path:line' }
        },
        required: ['severity', 'claim', 'correction']
      }
    },
    missingTopics: { type: 'array', items: { type: 'string' }, description: 'important things omitted' },
    verdict: { type: 'string', enum: ['publishable', 'minor-fixes', 'needs-rework'] },
    notes: { type: 'string' }
  },
  required: ['slug', 'accuracyScore', 'errors', 'verdict']
};

function authorPrompt(s) {
  const outPath = OUT + '/_fragments/' + s.n + '-' + s.slug + '.html';
  return HOUSE + '\n\n' + HTML_GUIDE + '\n\n' + CHAPTERS_NOTE +
    '\n\n=== YOUR CHAPTER ===\n' +
    'Number/Title for the <h1>: "' + s.n + ' · ' + s.title + '".\n' +
    'Output file (write the complete HTML fragment here with your Write tool): ' + outPath + '\n' +
    'Repository root to read source from: ' + ROOT + '\n\n' +
    'SCOPE & FOCUS:\n' + s.focus + '\n\n' +
    'START FROM THESE SOURCE FILES (read them; if a path is stale, grep/glob to find the real one — these are hints, the code is truth):\n' + s.files + '\n\n' +
    'Open and read the relevant source thoroughly before writing. Then write the full, dense, accurate HTML fragment to the output path. ' +
    'After writing the file, return ONLY the structured summary (do not echo the document body). Make keyFacts genuinely useful, self-contained, and citable — they feed the top-level overview.';
}

function verifyPrompt(s, authored) {
  const outPath = OUT + '/_fragments/' + s.n + '-' + s.slug + '.html';
  return 'You are an adversarial technical fact-checker for Apache Kafka 4.4.0-SNAPSHOT internals documentation (KRaft-only build). A chapter titled "' + s.n + ' · ' + s.title + '" was just written to:\n' + outPath + '\n\n' +
    'Repository root (the ground truth): ' + ROOT + '\n\n' +
    'YOUR JOB — find factual errors by re-reading the ACTUAL source. Steps:\n' +
    '1. Read the fragment file at the path above.\n' +
    '2. Independently read the relevant source under the repo root. Start from these areas but follow the code: ' + s.files + '\n' +
    '3. Do NOT trust the document. For every significant claim verify against code: that named classes/methods/fields exist as stated; that config keys and especially DEFAULT VALUES are correct; that algorithm and state-machine descriptions match the code; that file:line citations point to roughly the right place; that there are no invented APIs; that nothing describes ZooKeeper as a current dependency (this is KRaft-only); that byte-layout/format claims match the code.\n' +
    '4. Check the HTML is well-formed: no raw unescaped "<" inside <pre>/<code> that would break rendering, tags balanced, attributes quoted.\n\n' +
    'Be skeptical, specific, and grounded — every error you report must include the correction with a path:line from the source. Default to flagging when you cannot confirm a non-trivial claim. Do NOT nitpick prose or style; focus on technical/factual correctness and rendering-breaking HTML. Score accuracy 0-100. Verdict: "publishable" (no critical/major errors), "minor-fixes" (only minor issues), or "needs-rework" (any critical or several major errors). Also list important topics the chapter omitted.';
}

// --- which chapters to run: args may be an array of two-digit numbers; empty => all ---
const filter = Array.isArray(args) ? args.map(String) : [];
const wanted = filter.length ? SUBSYSTEMS.filter((s) => filter.includes(s.n)) : SUBSYSTEMS;

const authorStage = (s) => agent(authorPrompt(s), {
  label: 'author:' + s.n + '-' + s.slug, phase: 'Author',
  schema: AUTHOR_SCHEMA, agentType: 'general-purpose'
});
const verifyStage = (authored, s) => {
  if (!authored) return { slug: s.slug, n: s.n, title: s.title, authored: null, verify: null, failed: true };
  return agent(verifyPrompt(s, authored), {
    label: 'verify:' + s.n + '-' + s.slug, phase: 'Verify',
    schema: VERIFY_SCHEMA, agentType: 'general-purpose'
  }).then((v) => ({ slug: s.slug, n: s.n, title: s.title, authored: authored, verify: v }));
};

// First pass: author each chapter then adversarially verify it (independent per chapter).
let results = await pipeline(wanted, authorStage, verifyStage);

// Self-heal transient failures (e.g. server-side rate limits): retry chapters whose
// author died. Each round runs after the prior fully completes, providing natural spacing.
for (let round = 1; round <= 3; round++) {
  const todo = wanted.filter((s, i) => !results[i] || results[i].failed || !results[i].authored);
  if (!todo.length) break;
  log('Retry round ' + round + ' for ' + todo.length + ' chapter(s): ' + todo.map((s) => s.n).join(', '));
  const redo = await pipeline(todo, authorStage, verifyStage);
  todo.forEach((s, k) => { results[wanted.indexOf(s)] = redo[k]; });
}

const ok = results.filter((r) => r && r.authored).length;
log('Authoring complete: ' + ok + '/' + wanted.length + ' chapters have a fragment.');
return results;
