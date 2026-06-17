# Apache Kafka — Design Rationale & KIP Index

> **Internal reference for documentation authors and fact-checkers.** This file
> synthesizes structured research notes on Apache Kafka's core design decisions,
> their trade-offs, and their historical evolution, with a consolidated index of
> the Kafka Improvement Proposals (KIPs) that shaped them. It is **not** a
> published web page. Where a date, version, or attribution is commonly
> mis-stated, the "Common pitfalls / version caveats" section at the end records
> the correct fact and the usual error.

**Scope of the topics covered below:**

1. Origins & core design philosophy (the log abstraction)
2. Storage & record-format design (segments, page cache, zero-copy, v2 batch, compression, compaction)
3. Replication (ISR, high watermark, acks/durability, leader epochs, fetch-from-follower, ELR)
4. KRaft and the removal of ZooKeeper
5. Exactly-once semantics (idempotent producer + transactions)
6. Consumer group & rebalance protocol evolution
7. Tiered storage

A note on one requested topic: a "queues" topic was included in the source
research set but its research **failed** (no summary, decisions, or KIPs were
produced). It is intentionally omitted here; if share groups / queue semantics
(KIP-932) are needed, that research must be redone.

---

## 1. Origins and core design philosophy: the log abstraction

### Rationale

Kafka was built at LinkedIn (Jay Kreps, Neha Narkhede, Jun Rao) to close a
specific gap. Existing **log aggregators** (Scribe, Cloudera Flume, Yahoo Data
Highway) were batch/offline and pushed to HDFS, while traditional **JMS-style
message brokers** (ActiveMQ, IBM WebSphere MQ, RabbitMQ, TIBCO) were tuned for
rich per-message transactional delivery — not the firehose of "activity stream"
plus operational-metrics *log* data that internet companies generate (data that
is "orders of magnitude larger than the real data"). The 2011 NetDB paper,
*"Kafka: a Distributed Messaging System for Log Processing,"* frames Kafka as a
**unification** of the two: a distributed, partitioned, replayable, pull-based
pub/sub log that is fast enough for online consumption yet durable and scalable
enough for offline ETL.

Jay Kreps's 2013 essay *"The Log"* generalized this into a philosophy: a log is
"an append-only, totally-ordered sequence of records ordered by time" and
"perhaps the simplest possible storage abstraction." It is the mechanism behind
database replication and state-machine replication, and it can serve as the
single central source-of-truth pipeline that replaces an O(N²) tangle of bespoke
point-to-point integrations with an O(N) hub-and-spoke. The essay ties this to
the **State Machine Replication Principle**: identical, deterministic processes
that begin in the same state and consume the same inputs in the same order
produce the same outputs and end in the same state.

### Key design decisions and trade-offs

- **Bridge log aggregators and messaging systems.** Combine "the benefits of
  traditional log aggregators and messaging systems." *Trade-off:* drop rich
  per-message delivery guarantees and queueing features to get throughput.
- **Storage IS a log of segment files.** Each partition is a logical log
  implemented as ~1 GB segment files; publishing appends to the last segment;
  old data is deleted by a time-based retention SLA. *Rationale:* turns all I/O
  into sequential append/scan and makes deletion trivial.
- **Address by logical offset, not message ID; stateless broker.** Each message
  is addressed by its logical offset; the broker keeps no per-consumer state.
  *Rationale:* avoids seek-intensive index structures, reduces consumer position
  to one number per partition, and enables deliberate **rewind/replay**.
  *Trade-off:* violates the usual destroy-on-consume queue contract.
- **Lean on the OS page cache, not an in-JVM cache.** Avoids double-buffering,
  avoids GC pressure on large heaps, and keeps the cache warm across broker
  restarts.
- **Batching via the "message set" abstraction, end to end.** Producers send a
  set of messages per request; consumers fetch many at once. *Rationale:*
  amortize TCP round-trips and per-message overhead; produce large sequential
  writes. *Benchmark impact:* batch size 50 improved producer throughput ~10×
  (50k → ~400k msg/s).
- **Zero-copy transfer with `sendfile`.** Ships bytes straight from page cache to
  socket, eliminating user-space copies. *Rationale:* a multi-subscriber log is
  read many times, so consumption can approach network-link speed.
- **Pull-based consumers, not push.** Consumers retrieve at the maximum rate they
  can sustain. *Rationale:* natural backpressure (a push system can DoS a slow
  consumer), better batching, easy replay. *Trade-off:* a naive pull loop can
  busy-wait when idle (later mitigated by long-poll fetch).
- **Partition = unit of parallelism + ordering; consumer groups for scale-out.**
  Ordering is guaranteed within a partition, not across partitions; each
  partition is consumed by exactly one consumer per group.
- **Decentralized coordination via ZooKeeper, no master broker.** "Adding a
  master can complicate the system." *Trade-off, later revisited:* this
  ZooKeeper-centric model was eventually replaced by a broker-side group
  coordinator and ultimately by KRaft (KIP-500).
- **Trade durability for throughput initially.** The 2011 system had **no**
  intra-cluster replication, producers did not wait for acks, and Kafka
  guaranteed **at-least-once only**. Per-message CRCs guarded corruption.
  *Rationale:* "For many types of log data, it is desirable to trade durability
  for throughput." (This is the single most commonly mis-remembered aspect of
  early Kafka.)

### Historical evolution

- **2010–2011:** Built at LinkedIn to replace point-to-point pipelines and feed
  both real-time services and Hadoop. Open-sourced early 2011.
- **June 12, 2011:** NetDB'11 paper presented (Athens). Benchmarks on a 2-node
  setup: producer 50,000 msg/s (batch 1) and ~400,000 msg/s (batch 50, saturating
  a 1 Gb link), 200-byte messages; consumer 22,000 msg/s (>4× ActiveMQ 5.4 /
  RabbitMQ 2.4); end-to-end latency ~10 s; ~9 bytes/message storage overhead vs
  144 bytes in ActiveMQ.
- **July 2011:** Apache Incubator. **October 23, 2012:** top-level project.
- **Late 2013:** Kafka 0.8.0 added intra-cluster replication (ISR) — the feature
  the paper listed as future work. Around the same time Kreps published *"The
  Log."*
- **2014:** Kreps, Narkhede, Rao founded Confluent (incorporated Sept 25, 2014;
  often cited as "Nov 2014"). Kreps's *"I Heart Logs"* and Kleppmann's *"Turning
  the Database Inside Out"* popularized stream/table thinking.
- **2015:** KIP process introduced; VLDB 2015 paper documented production
  replication.
- **2016:** Kafka 0.10.0 shipped Kafka Streams (KIP-28) with KStream/KTable, plus
  message timestamps and the new format (KIP-31/32) — making stream/table duality
  a concrete API.
- **June 2017:** Kafka 0.11.0 shipped idempotent producer + transactions
  (KIP-98), adding exactly-once semantics.
- **2018:** *"Streams and Tables: Two Sides of the Same Coin"* (BIRTE 2018)
  formalized the duality.
- **2019–2025:** KRaft (KIP-500) removed the ZooKeeper dependency (production-ready
  for new clusters in 3.3, default in 3.5+, removed in Kafka 4.0, 2025); tiered
  storage (KIP-405) extended the log-as-storage idea to object stores.

---

## 2. Storage and record-format design

### Rationale

Kafka's storage is a deliberate bet that the cheapest, fastest durable store is a
simple append-only log on a commodity filesystem, exploiting the fact that
sequential disk I/O is orders of magnitude faster than random I/O (the design doc
cites **~600 MB/s sequential vs. ~100K small random writes/s** on a six-drive
7200 rpm SATA JBOD). Each partition is an ordered, immutable log split into ~1 GB
segments; messages are addressed by a monotonic logical offset (not a stored
physical ID), giving O(1) append and O(1) seek so performance is decoupled from
total data volume and long retention is cheap.

The two efficiency themes are framed explicitly as eliminating **"too many small
I/O operations"** (via the message-set/record-batch as the fundamental unit of
write, replication, and fetch) and **"excessive byte copying"** (via zero-copy
and end-to-end batch compression that keeps a batch compressed from producer
through log to consumer).

### Key design decisions and trade-offs

- **Plain append-only log over a B-tree/random-access DB**, because consumption is
  overwhelmingly sequential; gives O(1) append and O(1) offset seek so throughput
  is independent of retained volume.
- **~1 GB segment files addressed by monotonic offset** with only a sparse offset
  index — retention/deletion is just dropping whole old segments.
- **Delegate ALL caching to the OS page cache**, keeping no in-JVM cache: JVM
  object overhead can ~double the data size, GC cost grows with heap, the page
  cache survives restarts, and an in-app cache would just double-buffer.
- **Message set / record batch is the fundamental unit** of write, replication,
  and fetch — amortizing round-trips and turning I/O into large sequential chunks.
- **Zero-copy reads via `sendfile`** (`FileChannel.transferTo`): a caught-up
  cluster does essentially no disk reads and consumption approaches link speed.
- **Compress whole batches end-to-end** (producer compresses; the batch stays
  compressed in the log and on the wire to the consumer), combined with KIP-31
  relative offsets so the broker need not recompress to assign offsets.
- **In v2, separate batch-level from per-record schema**; write shared fields
  (producer id/epoch, base offset, base timestamp) once per batch, with a single
  batch-level CRC32C and varint per-record deltas — cutting per-record overhead
  from a fixed **34 bytes to ~7 bytes**.
- **Move the checksum to the batch level** (CRC32C over the batch): more robust
  than per-message CRCs once log-append-time stamping and format conversion mean
  the producer's per-message CRC can't be assumed to match the consumer's.
- **Encode idempotence/transaction primitives inline** in the batch header (PID,
  ProducerEpoch, BaseSequence, isTransactional/isControl), so EOS state is carried
  with the data and reconstructable on log reload.
- **Offer log compaction as an alternative retention mode** (keep at least the
  last value per key; null payload = tombstone) for changelog / event-sourcing /
  CDC / state-restore workloads where time/size retention would wrongly discard a
  key's current value.

### Historical evolution

The 2011 paper already states the core storage thesis (partitions as ~1 GB
segment logs, offset addressing, no in-process cache, page-cache reliance,
`sendfile`). Storage features then evolved largely independently of the record
format:

- **Log compaction shipped first, in 0.8.1 (2014), before the KIP process**;
  later refined by KIP-58 and KIP-71 (0.10.1: configurable compaction point,
  compact+delete coexistence) and KIP-280 / KIP-534 (2.6.0: timestamp/header-based
  compaction and safe tombstone retention).
- **Record format generations:** v0 (original, magic 0); **v1** in 0.10.0 via
  KIP-32 (timestamps, magic 1) with KIP-31 (relative offsets in compressed sets);
  **v2** in 0.11.0 via KIP-98 (magic 2) reframing the unit of storage as a record
  batch carrying producer id/epoch/sequence, a single batch CRC32C, and varint
  deltas — alongside KIP-82 (headers) and KIP-87 (explicit tombstone flag), all in
  0.11.0.
- **Compression broadened over time:** GZIP and Snappy early, LZ4 added, ZStandard
  via KIP-110 (2.1.0, 2018), configurable levels via KIP-390 (3.0.0).
- **KIP-724** deprecated legacy v0/v1 with a warning in 3.0 and removed them in
  4.0, because v2 is mandatory for correctness/EOS and down-converting v2 for old
  clients is expensive and defeats zero-copy.

---

## 3. Replication: ISR, high watermark, durability, leader epochs, fetch-from-follower, ELR

### Rationale

Kafka's replication is a leader-based, **pull (fetch)** primary-backup scheme
built around the **In-Sync Replica (ISR)** set rather than majority/quorum
voting. The intent is **durability-per-replica efficiency**: to tolerate `f`
failures, ISR needs only `f+1` replicas (commit when all *current* ISR members
ack), whereas a majority-vote quorum needs `2f+1`. The **high watermark (HW)** —
the offset replicated to all ISR members — is the commit boundary: only data
below the HW is committed and consumer-visible, which makes a partition a
replicated state machine. Over time, Kafka closed a series of correctness gaps in
this model.

### Key design decisions and trade-offs

- **ISR (primary-backup) over majority quorum.** Needs `f+1` vs `2f+1` replicas
  for `f` failures, so Kafka can run usefully with as few as 2 replicas across
  many partitions. *Trade-off:* commit latency is bounded by the **slowest**
  in-sync follower (a quorum's latency is bounded by the *faster* majority); ISR
  membership must be tracked dynamically (`replica.lag.time.max.ms`).
- **High watermark as the commit boundary.** Makes the partition a replicated
  state machine, but the original design propagated the new HW via an extra
  lagging fetch round-trip — the exact gap KIP-101 had to close.
- **Pull/fetch-based replication** (followers fetch from the leader, same path
  consumers use): simplifies the broker and later enabled fetch-from-follower.
- **Durability is `acks` + `min.insync.replicas` together, not `acks` alone.**
  `acks=all` waits for all *current* ISR members; if ISR has shrunk to 1, it
  still acks on a single replica. `min.insync.replicas>=2` makes the leader reject
  writes (`NotEnoughReplicas`) when ISR is too small — trading availability for
  durability. **(Most commonly misunderstood Kafka durability point.)**
- **Leader epochs as the truncation authority** (KIP-101/279): a monotonic
  per-leadership number stamped on records replaces the HW for deciding where a
  follower truncates, eliminating silent log divergence. *Deferred:* KIP-101 did
  **not** cover `unclean.leader.election.enable=true` (the hardest loss case,
  later addressed by KIP-966).
- **Broker-driven (not client-driven) replica selection** for fetch-from-follower
  (KIP-392): the broker tells the consumer which replica to read
  (`PreferredReadReplica`) via a pluggable `ReplicaSelector`. *Trade-off:*
  followers lag the leader's HW, so locality is bought with some added read
  latency and `OFFSET_NOT_AVAILABLE` retry handling.
- **ELR moves the durability guarantee into controller-tracked metadata**
  (KIP-966): a "strict min ISR" rule freezes HW advancement when ISR size <
  `min.insync.replicas`, so out-of-ISR-but-HW-complete replicas (ELR) stay valid
  leader candidates. *Cost:* reduced write availability during degraded states in
  exchange for not silently losing committed data, plus a deterministic Unclean
  Recovery replacing random/first-alive unclean election.

### Historical evolution

1. **Foundational (pre-0.8 → 0.9):** leader-based primary-backup; committed =
   replicated to all ISR; HW as the visible boundary. ISR membership moved from a
   message-lag threshold (`replica.lag.max.messages`) to time-based
   (`replica.lag.time.max.ms`, KIP-16 era).
2. **Truncation correctness (0.11.0.0, June 2017):** KIP-101 introduced leader
   epochs (carried by message format v2) and epoch-based truncation.
3. **Fast-failover fix (2.0.0):** KIP-279 (KAFKA-6361) extended
   `OffsetForLeaderEpoch` to return the largest epoch ≤ requested with its end
   offset. Related **KIP-320** (2.1.0) let clients detect truncation/unclean
   election via leader epochs.
4. **Read-path locality (2.4.0, Dec 2019):** KIP-392 added fetch-from-follower
   with `RackAwareReplicaSelector`, preserving HW-bounded read consistency.
5. **Default consistency:** `unclean.leader.election.enable=false` has been the
   default since 0.11.0.0.
6. **Durability hardening (4.0 opt-in / 4.1 default-on, 2025):** KIP-966 ELR +
   strict-min-ISR + deterministic Unclean Recovery close the "last replica
   standing" loss scenario and make `min.insync.replicas` cluster-level — the
   KRaft-era controller taking over leadership/durability bookkeeping that ISR
   persistence in ZooKeeper used to handle.

---

## 4. KRaft (Kafka Raft) and the removal of ZooKeeper

### Rationale

KRaft replaces Kafka's external ZooKeeper dependency with a self-managed metadata
quorum built on a Kafka-native Raft implementation. KIP-500's goals: run a
**single** system (not two), eliminate controller/ZooKeeper state divergence, and
scale to far more partitions by making metadata an ordered, replayable, fsync'd
**event log** instead of ZooKeeper znodes. ZooKeeper's problems were concrete:
brokers could receive some-but-not-all pushed changes; the controller's in-memory
state often didn't match ZK (multi-second lag); ZK watches don't return current
state and are performance-limited; and running two distributed systems is
operationally/security-costly.

### Key design decisions and trade-offs

- **Pull-based Raft dialect (KIP-595).** Followers *fetch* from the leader
  (reconciliation driven by the replica) instead of standard Raft's push-based
  `AppendEntries`; uses Kafka terminology **offset/leader-epoch** instead of
  index/term; **no leader heartbeats** — fetches are the liveness signal. Chosen
  to reuse Kafka's existing log layer, scale to many non-voting observers, and
  ease a future move to partition-level Raft. The project deliberately **wrote its
  own Raft** (not etcd/Consul/a library) because "log replication is at the core
  of Kafka and the project should own it." *Trade-off:* requires **synchronous
  fsync** of every metadata append to guarantee Raft safety.
- **Brokers PULL metadata** via `MetadataFetch` (which doubles as a heartbeat) and
  persist it locally, instead of being PUSHed `LeaderAndIsr`/`UpdateMetadata`/
  `StopReplica` — making topic create/delete O(1) for the controller.
- **Snapshots (KIP-630), not log compaction**, for the metadata log: the
  controller state is event/delta-based (e.g. `FenceBroker`), not key/value, and
  compaction can produce divergent logs across replicas via missed tombstones.
- **Quorum controller + compact framed record format (KIP-631):** an active
  controller (Raft leader) serves broker RPCs; standbys are hot. Brokers
  proactively register (`BrokerRegistrationRequest`) and are explicitly fenced
  until log recovery completes. Node IDs must be set up front (Raft requirement);
  storage must be pre-formatted with `kafka-storage.sh format`.
- **Feature-level gating via `metadata.version` (KIP-584)** replaces the
  `inter.broker.protocol.version` double-roll, letting KRaft format changes
  upgrade safely. (KIP-584 itself predates KRaft — shipped 2.7, ZK era.)
- **Dual-write migration (KIP-866):** a controller in migration mode mirrors KRaft
  commits back into ZooKeeper, so operators can roll forward broker-by-broker and
  **roll back at any point before finalization**. Chose rolling restart (not an
  in-place switch) to keep the ZK code path as a safety net.
- **Dynamically reconfigurable quorum (KIP-853):** the voter set is persisted in
  the log via `VotersRecord` with `AddVoter`/`RemoveVoter` RPCs and
  `controller.quorum.bootstrap.servers`, replacing the static
  `controller.quorum.voters`. Only one voter change at a time; the leader requires
  majority commit in both old and new voter sets.

### Historical evolution

Kafka used ZooKeeper from the start (2011) for broker registration, controller
election, and metadata. KIP-500 (Accepted 2020-07) proposed removing it, with
KIP-595/630/631 as the core follow-ons and KIP-584 (2.7) as enabling "bridge
work." Timeline: **early access in 2.8.0 (2021-04-19)** → **preview in 3.0
(2021)** → **production-ready for NEW clusters in 3.3** (KIP-833, Oct 2022) → **ZK
deprecated in 3.5 (first bridge release)** → **migration (KIP-866) GA in 3.6 (late
2023)** → **dynamic quorums (KIP-853) + final ZK-mode release in 3.9
(2024-11-06)** → **complete ZooKeeper removal (KRaft-only) in Kafka 4.0
(2025-03-18)**. Upgrade implication: ZK-based clusters must first migrate to KRaft
on a 3.x bridge release (3.9 recommended) before upgrading to 4.0.

---

## 5. Exactly-once semantics: idempotent producer and transactions

### Rationale

Kafka's exactly-once semantics (EOS) is built in two layers introduced by KIP-98
(Kafka 0.11.0.0): the **idempotent producer**, which deduplicates retries within
a single producer session on a per-partition basis, and **transactions**, which
give atomic, all-or-nothing writes across multiple topic-partitions and across
producer sessions. The deliberate philosophy was to **reuse robust existing Kafka
primitives** (the log, leader election, the offsets-topic pattern) and keep
per-transaction overhead tiny.

### Key design decisions and trade-offs

- **Reuse Kafka's own log as the transaction store.** `__transaction_state` is a
  normal replicated, compacted topic, so the coordinator inherits durability,
  replication, and leader-election/failover for free.
- **Separate the two concerns.** Idempotence (cheap, single-session, per-partition
  dedup via PID + sequence numbers) is distinct from transactions (atomic
  multi-partition, cross-session) — so users can get dedup without paying
  transaction cost, and idempotence can become a sane default (KIP-679).
- **Server-assigned PID + user-supplied `transactional.id`, fenced by epoch.** The
  `transactional.id` gives identity continuity across restarts; bumping its epoch
  on re-init guarantees at most one live producer (zombie fencing,
  `ProducerFencedException`).
- **Two-phase commit with control-record markers written into the data partitions
  themselves**, so consumers learn an outcome by reading an in-line COMMIT/ABORT
  record — keeping the hot read path local. Per-transaction cost is roughly one
  marker write per partition plus a few records on the txn log.
- **Introduce the Last Stable Offset (LSO).** `read_committed` consumers stop at
  the LSO (below which every transaction is decided), not the HW, and filter
  aborted records via an aborted-transactions index — bounding visibility without
  unbounded buffering.
- **Default consumers to `read_uncommitted`** and support speculative reads,
  because Streams topologies can be ~10 stages deep; requiring every stage to wait
  for upstream commits would make latency unacceptable. EOS visibility is opt-in.
- **Fold consumer-offset commits into the transaction** via
  `sendOffsetsToTransaction`, making the read-process-write loop one atomic unit —
  what makes Kafka Streams end-to-end exactly-once possible.
- **KIP-447 leans on the consumer group coordinator's generation-based fencing**
  (generationId/memberId/groupInstanceId) instead of inventing new fencing,
  decoupling `transactional.id` from input-partition assignment. *Trade-off:* a
  brief availability loss while a newly assigned consumer waits out pending
  transactions (`PendingTransactionException`) in exchange for one reusable
  producer per process and far better batching.

### Historical evolution

Kafka originally shipped with **at-least-once** producer semantics only. **KIP-98
(0.11.0.0, June 2017)** introduced both the idempotent producer and transactions,
plus the v2 record-batch format. In the same release line, **KIP-129** brought
EOS to Kafka Streams (the original "eos-alpha" mode, one producer per task).
**KIP-185** *proposed* turning the idempotent producer on by default, but that
broad change was not actually completed then. **KIP-447 (2.6.0)** removed the
one-producer-per-input-partition constraint (Streams "eos-beta"). **KIP-679
(3.0.0)** finally made `enable.idempotence=true` and `acks=all` the producer
defaults (acks landed cleanly; the idempotence flag needed follow-up fix
KAFKA-13598). **KIP-732 (3.0.0)** renamed eos-beta to `exactly_once_v2` and
deprecated `exactly_once` (eos-alpha) and `exactly_once_beta`. Related later work:
**KIP-618** added EOS for Connect source connectors; **KIP-854** added separate
producer-ID expiry configuration.

---

## 6. Consumer group and rebalance protocol evolution

### Rationale

The original rebalance protocol was a client-driven, generation-based handshake:
consumers heartbeat to a broker-side group coordinator, and on any membership
change they all execute a `JoinGroup`/`SyncGroup` "dance" in which an elected
**consumer** (the "leader," *not* the broker) computes the assignment and the
coordinator merely relays it. The fundamental flaw: this is a global
synchronization barrier ("stop-the-world") — every member revokes everything and
the whole group stalls whenever one consumer joins/leaves/fails — and the fragile
assignment logic lives in a thick, polyglot, slow-to-patch client. **The unifying
theme of the entire evolution is the migration of complexity from client to
broker.**

### Key design decisions and trade-offs

- **Static membership (KIP-345):** restarting members keep identity
  (`group.instance.id`) and don't send `LeaveGroup`, so rolling bounces don't
  rebalance; removal is driven only by `session.timeout.ms`. Explicit trade-off:
  **"state persistence over liveness."** Fences duplicates with
  `FENCED_INSTANCE_ID` (error code 78). Targets rolling bounces/scale-down, not
  scale-up.
- **Incremental cooperative rebalancing (KIP-429):** replaces eager
  revoke-everything with a **two-rebalance** protocol
  (`CooperativeStickyAssignor`) — the first rebalance only revokes partitions that
  must move; a second assigns them — so unaffected partitions keep being
  processed. *Caveat:* benefits only materialize if the assignor is actually
  sticky; requires a two-rolling-bounce upgrade.
- **Next-gen protocol (KIP-848) — the architectural reset:** deletes
  `JoinGroup`/`SyncGroup`, replaces them with a single long-lived
  `ConsumerGroupHeartbeat` RPC, moves assignment **fully server-side** into a
  group coordinator **rewritten from Scala into Java** as a per-`__consumer_offsets`-partition
  event loop, and drives fully-incremental reconciliation via **three epochs**
  (group, assignment, per-member). *Trade-off / limitation:* **only server-side
  assignors** are supported — client-side/custom assignors were marked "won't do"
  (KAFKA-15282); that use case is handed to Streams (KIP-1071).
- **Streams rebalance protocol (KIP-1071):** applies the KIP-848 model to Kafka
  Streams via a first-class `streams` group type and `StreamsGroupHeartbeat` RPC,
  moving task assignment, topology validation, and internal-topic creation onto
  the broker — precisely **why** client-side assignors for plain consumers were
  abandoned. Makes scaling/warm-up parameters broker-side and runtime-tunable (no
  app redeploy).

### Historical evolution

- **Original (pre-2.4):** client-driven `JoinGroup`/`SyncGroup`; one consumer
  computes the assignment; eager/stop-the-world; Scala coordinator storing state
  in `__consumer_offsets`.
- **2.4.0 (two parallel improvements):** KIP-345 (static membership) and KIP-429
  (incremental cooperative rebalancing) — both reduce rebalances without changing
  the client-driven core. (Connect had an analogous path, KIP-415.)
- **2.5.0:** KIP-429 refinement to allow processing during a rebalance.
- **KIP-848 staged rollout:** **Early Access in 3.7.0** (server-side assignors
  only; no server-side regex; can't describe new-protocol groups via tooling) →
  **Preview in 3.8.0** ("not recommended for production") → **GA in 4.0.0**, where
  the new Java group coordinator also became the **default** on KRaft. KRaft-only.
  Client opt-in via `group.protocol=consumer` (default remains `classic`). 4.2
  added topic-ID support for offset RPCs under the new protocol.
- **KIP-1071:** **Early Access in 4.1.0 (Sept 2025)** — `streams` group type,
  server-side task assignment. Not GA; some features (static membership, certain
  topology updates) not yet covered.

> Note on `group.protocol`: it takes **three** relevant values across these
> KIPs — `classic` (legacy), `consumer` (KIP-848), and `streams` (KIP-1071) — not
> a simple classic/consumer boolean.

---

## 7. Tiered storage

### Rationale

Tiered storage (KIP-405) decouples storage from compute by splitting a topic's
log into a **local** tier (broker disks, serving latency-sensitive tail reads from
page cache) and a **remote** tier (object stores like S3/GCS/Azure Blob, or HDFS)
that holds older closed segments. In classic Kafka, retention is bounded by broker
disk and every stored byte inflates broker count, rebalance/recovery time, and
cost; pushing cold data to cheap, elastic object storage enables near-infinite
retention while keeping clusters small and recovery fast. Because closed segments
are immutable, they can be copied wholesale to an external store and deleted
locally without changing read semantics.

### Key design decisions and trade-offs

- **Two tiers (local + remote), not a replacement.** Tail/latency-sensitive reads
  stay on broker disks via the OS page cache; only data older than
  `local.retention` is offloaded — preserving the low-latency hot path while
  unlocking cheap long retention.
- **Separate DATA storage from METADATA storage.** `RemoteStorageManager` (RSM)
  writes segment data to the (eventually-consistent) object store, while
  `RemoteLogMetadataManager` (RLMM) keeps authoritative segment metadata in a
  **strongly-consistent** store (default: internal `__remote_log_metadata` topic).
  *Rationale:* object stores have weak/eventual consistency and expensive/limited
  LIST semantics; keeping metadata in a consistent Kafka topic avoids relying on
  S3 LIST and lets remote ops be retried idempotently.
- **Two pluggable SPIs, NO built-in cloud implementation** shipped in Apache
  Kafka. *Trade-off:* keeps the broker storage-agnostic, but users must supply/
  configure `remote.log.storage.manager.class.name` (implementations come from
  vendors, e.g. Aiven's open-source S3/GCS/Azure RSM).
- **Dedicated `RemoteLogManager` thread pools** (copier, expiration, reader) plus a
  `RemoteFetchPurgatory`, isolating high-latency remote I/O from broker request/
  replica threads so object-store latency/outages don't stall produce/consume.
- **Transparent to clients.** Consumers need no changes; a fetch below the
  local-log-start-offset is served from remote. Followers behind the local start
  get `OFFSET_MOVED_TO_TIERED_STORAGE` and rebuild leader-epoch/producer-ID state
  from remote metadata (no new leader RPC).
- **Independent retention.** `local.retention.ms/bytes` (≤ overall
  `retention.ms/bytes`) bound the local tier; `retention.ms/bytes` govern remote
  lifetime — cleanly separating "how much on disk" from "how long overall."
- **Staged maturity.** Shipped Early Access first (3.6.0, "not for production"),
  then GA (3.9.0) after follow-up KIPs closed operational gaps — not a big-bang
  release.
- **Deliberately scoped v1 via non-goals** — no compacted topics, no JBOD, no
  secondary remote stores, not an ETL replacement — to bound complexity. (JBOD was
  later supported by GA; disablement added via KIP-950.)

### Historical evolution

`KAFKA-7739` was filed 2018-12-14 (reporter Harsha); KIP-405 went through a long
DISCUSS/VOTE cycle with Uber and others running early production deployments. It
shipped as **Early Access in 3.6.0 (Oct 2023)** — explicitly not for production,
with no compacted topics, no JBOD, no disablement, and admin actions requiring
3.0+ clients. Over 3.7–3.8 the implementation hardened, and in **3.9.0 (Nov 2024,
the final 3.x release)** it was declared **production-ready/GA**, bundling KIP-950
(per-topic disablement, KRaft-first), KIP-956 (upload/download quotas), KIP-1005
(ListOffsets v9 exposing earliest-local and tiered offsets), and KIP-1057
(`kafka-dump-log.sh` support for remote-log-metadata). By GA, JBOD/multiple
log-directories was supported (reversing the original non-goal), though compacted
topics and the one-partition-per-remote-fetch limitation remained. Continuing
work extends the baseline: KIP-1075 (async remote `ListOffsets`), KIP-1176
(tiering the active segment), KIP-1255 (remote read replicas).

---

## Consolidated KIP index

Sorted by KIP number, de-duplicated across topics. "Status / version" gives the
shipped/adopted release where known. Several entries are foundational features
that **predate the KIP process** or are proposals/follow-ups whose status is noted
inline.

> **Pre-KIP foundations (no KIP number):** The **original Kafka design** (NetDB
> 2011 paper) predates the KIP process, which began ~2015 — do not cite a KIP for
> log storage, offsets, page cache, `sendfile`, or pull consumers. **Log
> compaction** also has no founding KIP; it shipped in **0.8.1 (2014)** before the
> process existed. **Intra-cluster replication (ISR)** first shipped in **0.8.0
> (2013)**, likewise pre-KIP.

| KIP | Title | What it did | Status / version |
|---|---|---|---|
| KIP-16 | Replica lag based on time (`replica.lag.time.max.ms`) | Moved ISR membership from a message-count lag threshold to a time-based one. (Referenced as the "KIP-16 era" change.) | Adopted (early; ~0.9 era) |
| KIP-28 | Add a processor client / Kafka Streams | Introduced Kafka Streams with KStream/KTable, making stream-table duality a first-class API. | Adopted; Kafka 0.10.0 (2016) |
| KIP-31 | Move to relative offsets in compressed message sets | Made inner (wrapped) message offsets relative to the batch base so the broker stops decompress→reassign→recompress on ingest; prerequisite for v2 varint inner offsets. | Adopted; Kafka 0.10.0 (v1 era; folded into v2 in 0.11.0) |
| KIP-32 | Add timestamps to Kafka message | Introduced message format **v1** (magic 1) with a per-message timestamp (CreateTime vs LogAppendTime); enabled time-based indexing/retention and stream-time. | Adopted; Kafka 0.10.0 |
| KIP-58 | Make log compaction point configurable | Added `min.compaction.lag.ms` so records stay uncompacted for a guaranteed minimum time. | Adopted; Kafka 0.10.1.0 |
| KIP-71 | Enable log compaction and deletion to co-exist | Allowed `cleanup.policy` to combine `compact` + `delete` (e.g. for windowed Streams state). | Adopted; Kafka 0.10.1.0 |
| KIP-82 | Add Record Headers | Added an ordered array of per-record headers (String key + byte[] value); required the v2 format. | Adopted; Kafka 0.11.0.0 |
| KIP-87 | Add Compaction Tombstone Flag | Added an explicit per-record tombstone attribute bit in v2 (disambiguating deletes); null-payload tombstones predate this. | Adopted; Kafka 0.11.0.0 |
| KIP-98 | Exactly Once Delivery and Transactional Messaging | Introduced the idempotent producer (PID + per-partition sequence numbers) **and** transactions (transaction coordinator, `__transaction_state`, 2PC with COMMIT/ABORT markers, epoch fencing) **and** the **v2 record-batch format** (magic 2, batch CRC32C, varint deltas, ~7-byte per-record overhead). | Adopted; Kafka 0.11.0.0 (June 2017) |
| KIP-101 | Use Leader Epoch rather than High Watermark for truncation | Introduced the leader epoch and replaced HW-based truncation with epoch-based truncation, fixing committed-message loss and log divergence on leader change. Did **not** cover unclean leader election. | Adopted; Kafka 0.11.0.0 |
| KIP-110 | Add Codec for ZStandard Compression | Added `zstd` (compression.type id 4); requires v2/new clients (else `UNSUPPORTED_COMPRESSION_TYPE`). | Adopted; Kafka 2.1.0 (Nov 2018) |
| KIP-129 | Exactly-once for Kafka Streams | Brought EOS to the Streams consume-process-produce loop atop KIP-98 (the original "eos-alpha," one producer per task). | Adopted; Kafka 0.11.0.0 |
| KIP-185 | Make idempotent in-order delivery the default producer setting | **Proposed** enabling the idempotent producer by default; commonly misattributed as having done so in 1.0 — effectively completed later by KIP-679. | Proposal; superseded/completed by KIP-679 (3.0.0) |
| KIP-279 | Fix log divergence after fast leader fail-over | Extended `OffsetForLeaderEpoch` to return the largest epoch ≤ requested with its end offset, fixing residual divergence under rapid successive leader elections. | Adopted; Kafka 2.0.0 (KAFKA-6361) |
| KIP-280 | Enhanced log compaction | Let compaction choose the surviving record per key by timestamp or header value (not only highest offset). | Adopted; Kafka 2.6.0 |
| KIP-320 | Allow fetchers to detect and handle log truncation | Let clients detect truncation / unclean leader election via leader epochs. | Adopted; Kafka 2.1.0 |
| KIP-345 | Static membership protocol to reduce rebalances | Added `group.instance.id`; static members skip `LeaveGroup` and keep their cached assignment across restarts (`FENCED_INSTANCE_ID`, code 78). "State persistence over liveness." | Adopted; Kafka 2.4.0 |
| KIP-390 | Configurable compression level | Added `compression.{gzip,lz4,zstd}.level` (Snappy excluded — no level concept). | Adopted; Kafka 3.0.0 |
| KIP-392 | Allow consumers to fetch from closest replica | Added fetch-from-follower: `client.rack`, broker-side pluggable `ReplicaSelector` (`LeaderSelector` default, `RackAwareReplicaSelector`); reads only up to the follower's HW. | Adopted; Kafka 2.4.0 (Dec 2019) |
| KIP-405 | Kafka Tiered Storage | Introduced the local+remote two-tier model: RSM/RLMM SPIs, broker `RemoteLogManager`, `__remote_log_metadata` topic, `remote.storage.enable`, `local.retention.*`, transparent remote reads, `OFFSET_MOVED_TO_TIERED_STORAGE`. | Accepted (KAFKA-7739); Early Access 3.6.0, GA 3.9.0 |
| KIP-415 | Incremental cooperative rebalancing for Kafka Connect | Connect's analogue of KIP-429 (cooperative rebalancing for connector/task assignment). | Adopted; Kafka 2.3.0 |
| KIP-429 | Kafka Consumer Incremental Rebalance Protocol | Replaced eager (stop-the-world) rebalancing with incremental cooperative rebalancing (`CooperativeStickyAssignor`, two-rebalance handoff, `onPartitionsLost`). | Adopted; Kafka 2.4.0 (refinement 2.5.0) |
| KIP-447 | Producer scalability for exactly-once semantics | Removed one-producer-per-input-partition: ships `ConsumerGroupMetadata` with offsets so the group coordinator's generation-based fencing handles zombies; one thread-safe producer per process. Streams "eos-beta." | Adopted; Kafka 2.6.0 |
| KIP-500 | Replace ZooKeeper with a Self-Managed Metadata Quorum | Umbrella proposal: internal controller quorum stores all metadata as an event log in `__cluster_metadata`; brokers PULL metadata; defines the bridge-release upgrade strategy. | Accepted (2020-07); early access 2.8.0 |
| KIP-534 | Retain tombstones/transaction markers for ~`delete.retention.ms` | Fixed premature removal of tombstones/markers so a consumer that read a key is guaranteed to also see its later deletion. | Adopted; Kafka 2.6.0 |
| KIP-584 | Versioning scheme for features | Cluster-wide feature flags (Supported vs Finalized) via `UpdateFeatures`; replaces the IBP double-roll; basis for KRaft's `metadata.version`. (Predates KRaft.) | Adopted; Kafka 2.7 |
| KIP-595 | A Raft Protocol for the Metadata Quorum | Kafka's custom **pull-based** Raft dialect (offset/leader-epoch, no leader heartbeats, fsync of metadata appends); own implementation rather than a library. | Adopted; early access 2.8.0 |
| KIP-618 | Exactly-once support for Connect source connectors | Extended EOS to Kafka Connect source connectors. | Adopted (post-KIP-447 lineage) |
| KIP-630 | Kafka Raft Snapshot | Added metadata-log snapshots (chosen over compaction because controller state is event/delta-based) with a `FetchSnapshot` RPC. | Adopted; with KRaft core |
| KIP-631 | The Quorum-based Kafka Controller | Specified the KRaft controller and the framed metadata record format (`RegisterBrokerRecord`, `TopicRecord`, `PartitionChangeRecord`, etc.); broker registration/fencing; `node.id`, `kafka-storage.sh format`. | Accepted; with KRaft core |
| KIP-679 | Producer enables the strongest delivery guarantee by default | Made `enable.idempotence=true` and `acks=all` the producer defaults (idempotence flag hit validation bug KAFKA-13598, fixed shortly after). | Adopted; Kafka 3.0.0 |
| KIP-724 | Drop support for message formats v0 and v1 | Phased out legacy v0/v1, leaving v2 the only on-disk/wire format (down-conversion defeats zero-copy). | Adopted; warned in 3.0, removed in 4.0 |
| KIP-732 | Deprecate eos-alpha and replace eos-beta with eos-v2 | Streams `processing.guarantee` cleanup: deprecated `exactly_once` (eos-alpha) and `exactly_once_beta`; introduced `exactly_once_v2` (requires brokers ≥ 2.5). | Adopted; Kafka 3.0.0 |
| KIP-833 | Mark KRaft as Production Ready | Declared KRaft production-ready **for new clusters only**. | Adopted; Kafka 3.3 (Oct 2022) |
| KIP-848 | The Next Generation of the Consumer Rebalance Protocol | Replaced `JoinGroup`/`SyncGroup` with a long-lived `ConsumerGroupHeartbeat`; server-side assignment in a Java event-loop coordinator; three-epoch incremental reconciliation. Server-side assignors only. | Accepted; EA 3.7.0, Preview 3.8.0, **GA 4.0.0** (new coordinator default) |
| KIP-853 | KRaft Controller Membership Changes | Made the controller quorum dynamically reconfigurable (`VotersRecord`, `AddVoter`/`RemoveVoter`, `controller.quorum.bootstrap.servers`); integrates KIP-996 pre-vote. | Accepted; Kafka 3.9 (cannot convert an existing static quorum) |
| KIP-854 | Separate configuration for producer-ID expiry | Added independent configuration of producer-ID expiry. | Adopted (EOS-related follow-up) |
| KIP-866 | ZooKeeper to KRaft Migration | Online ZK→KRaft migration via a dual-write controller (mirrors KRaft commits into ZK); roll forward broker-by-broker, roll back before finalization. | Accepted; preview 3.4/3.5, production-ready 3.6 |
| KIP-932 | (Queues for Kafka / share groups) | *Referenced as the queue-semantics work, but the "queues" research topic failed — details not captured here. Verify independently before citing.* | Not researched in this set |
| KIP-950 | Tiered Storage Disablement | Let operators disable tiering per topic (`remote.log.copy.disable`, `remote.log.delete.on.disable`); split RLM copier/expiration thread pools. | Accepted; Kafka 3.9.0 (KRaft path) |
| KIP-956 | Tiered Storage Quotas | Broker-level quotas capping tiered upload (copy) and fetch (read) byte rates. | Accepted; Kafka 3.9.0 |
| KIP-966 | Eligible Leader Replicas (ELR) | Added the ELR set + "strict min ISR" (HW only advances when ISR ≥ `min.insync.replicas`) + deterministic Unclean Recovery; made `min.insync.replicas` cluster-level; supersedes `unclean.leader.election.enable`. | Accepted (Aug 2025); introduced 4.0 (off by default), default-on for new clusters 4.1 |
| KIP-1005 | Expose EarliestLocalOffset and TieredOffset | Added ListOffsets v9 sentinels to query the earliest-local and highest-remote ("tiered") offsets. | Accepted; Kafka 3.9.0 |
| KIP-1057 | Add remote-log-metadata flag to the dump-log tool | Let `kafka-dump-log.sh` decode `__remote_log_metadata` records. | Accepted; Kafka 3.9.0 |
| KIP-1071 | Streams Rebalance Protocol | Extended KIP-848 to Streams: `streams` group type, `StreamsGroupHeartbeat`, broker-side task assignment, topology validation, internal-topic creation, warm-up tasks. | Accepted; **Early Access 4.1.0** (not GA) |
| KIP-1075 | Async remote `LIST_OFFSETS` via delayed purgatory | Makes remote `ListOffsets` non-blocking through a dedicated purgatory. | Follow-up KIP (post-3.9) |
| KIP-1176 | Tiered Storage for Active Log Segment | Explores tiering the currently-active (not-yet-rolled) segment. | Proposed |
| KIP-1255 | Remote Read Replicas for Tiered Storage | Explores read replicas serving historical data directly from the remote tier. | Proposed |
| KIP-1274 | Deprecate/remove the Classic rebalance protocol in `KafkaConsumer` | Proposes eventually deprecating and removing the classic protocol from `KafkaConsumer`. | Proposed |

**Distinct KIPs catalogued: 48** (49 rows appear in the table above, but KIP-932
is only a cross-reference placeholder for the failed "queues" topic and carries no
researched content, so it is excluded from the count). KIP-31 and KIP-32 are
counted separately, as are KIP-101 and KIP-279, etc. The pre-KIP foundational
"Original Kafka design," and the standalone shipping of log compaction and
replication, are **not** counted as KIPs.

---

## Common pitfalls / version caveats (things documentation authors frequently get wrong)

### Origins, attribution, and the 2011 paper

- **The 2011 paper does NOT describe replication or exactly-once.** It explicitly
  states "Kafka only guarantees at-least-once delivery" and that a permanently
  damaged broker loses unconsumed messages "forever," listing replication as
  future work. Replication arrived in **0.8.0 (2013)**; exactly-once/idempotence/
  transactions in **0.11.0 (2017, KIP-98)**. Do not retroject these into the paper.
- **Stream-table duality and KStream/KTable are NOT contributions of the 2011
  paper.** They appear only in seed form in *"The Log"* (2013) and were formalized
  in Kafka Streams (KIP-28, 0.10.0, 2016), KSQL, Kleppmann's *"Turning the
  Database Inside Out"* (2014), and the BIRTE 2018 paper. Attribute the duality to
  that later lineage, not to Kreps/Narkhede/Rao 2011.
- **Benchmark numbers are widely misquoted.** The paper's figures are 50,000 msg/s
  (batch 1), ~400,000 msg/s (batch 50), and 22,000 msg/s (consumer) on a specific
  2-node / 1 Gb setup with 200-byte messages. "800 MB/s" or "2 million msg/s" are
  **not** from this paper (they come from later Confluent/LinkedIn benchmarks on
  different hardware). The "~600 MB/s sequential vs ~100K/s random, >6000×" JBOD
  figure is from the **official Kafka design docs**, not the NetDB paper, and is
  illustrative of ~2010-era spinning disks — **not** current SSD/NVMe numbers.
- **The KIP process did not exist in 2011** (it began ~2015). Do not cite a KIP for
  foundational concepts; cite the NetDB 2011 paper / design docs.
- **Kafka did NOT originally store consumer offsets in a Kafka topic.** In 2011,
  offsets and coordination lived in ZooKeeper; the `__consumer_offsets` topic and
  broker-side group coordinator came later (~0.8.2/0.9). "Kafka always kept offsets
  in the log" is anachronistic.
- **Confluent's founding date** is often given as "November 2014" but the company
  was incorporated **September 25, 2014**; either may appear depending on source.
- The "Kafka named after Franz Kafka because it's optimized for writing" story is
  anecdotal/blog-sourced (per Kreps), **not** in the paper — flag it as such.

### Storage and record format

- **The "v2 record batch format" was NOT introduced by a single KIP.** It is the
  convergence of KIP-98 (batch container, PID/epoch/sequence, batch CRC), KIP-31
  (relative offsets), KIP-32 (timestamps), and KIP-82 (headers). **KIP-98** is the
  one that bumped the magic byte to 2; all landed together in **0.11.0** except the
  timestamp/relative-offset mechanics, which first appeared as **v1 in 0.10.0**.
- **KIP-31 is frequently mis-cited** as introducing v2 or "relative offsets in
  v2." It shipped in **0.10.0 (v1 era)**; its relative-offset idea was a
  prerequisite later folded into v2. v1 (KIP-32) is the magic-byte-1 format that
  added timestamps.
- **Log compaction has no founding KIP** — it shipped in **0.8.1 (2014)** before
  the process existed. KIP-58/71/87/280/534 are later refinements, not its origin.
- **Null-payload tombstones predate KIP-87.** A null value has meant "delete" since
  compaction's introduction; **KIP-87 (0.11.0)** added an explicit tombstone *flag*
  bit in v2 — it did not invent tombstones.
- **"Zero-copy" does not mean literally zero copies.** `sendfile` still copies from
  page cache to the NIC (and DMA into page cache on read); what's eliminated are
  the redundant user-space copies and context switches. Zero-copy does **NOT**
  apply when **TLS/SSL** is enabled or when the broker must **down-convert** v2 to
  an older format for old clients — both force data through user space.
- **Idempotence and transactions are distinct features** that both rely on v2. Do
  not conflate "idempotent producer" with "transactions."
- **ZStandard (KIP-110, 2.1.0) requires v2** and returns
  `UNSUPPORTED_COMPRESSION_TYPE` to old clients. Configurable compression **levels**
  (KIP-390, 3.0.0) exist for gzip/lz4/zstd but **not Snappy**.
- **Kafka 4.0 removes v0/v1 entirely (KIP-724).** 3.0 only deprecates them with a
  warning. Do not claim v0/v1 are still selectable on 4.x.
- **cwiki "last modified" dates on KIP pages can show recent timestamps** (e.g.
  2026) due to wiki edits and do **not** indicate when a KIP was adopted/shipped —
  rely on release notes/plans for versions.

### Replication and durability

- **`acks=all` does NOT by itself guarantee no data loss.** It waits for all
  *current* ISR members; if ISR has shrunk to just the leader, it acks on one
  replica. Durability needs `acks=all` **AND** `min.insync.replicas>=2`. The
  classic loss case is `replication.factor=3, min.insync.replicas=1, acks=all`.
- **"`acks=all` waits for all replicas" is wrong** — it waits for all members of
  the **current ISR**, which may be fewer than the replication factor.
- **State the ISR efficiency claim carefully:** ISR needs `f+1` replicas vs `2f+1`
  for a quorum — but **both wait for the same number of acks**. The win is fewer
  total replicas, **not** fewer acks; the cost is that ISR latency tracks the
  *slowest* in-sync follower while quorum latency tracks the *faster* majority.
- **KIP-101 vs KIP-279 versions are commonly conflated.** Leader-epoch
  infrastructure (KIP-101) shipped in **0.11.0.0**; the fast-failover divergence
  fix (KIP-279 / KAFKA-6361) shipped later, in **2.0.0**.
- **KIP-392 shipped in 2.4.0, not 2.3.** The default `replica.selector.class` is
  `LeaderSelector` (unchanged behavior); `RackAwareReplicaSelector` is opt-in and
  needs `client.rack` set on consumers. Fetch-from-follower reduces **network
  cost, not consistency** — a follower serves only up to its own (lagging) HW, and
  offsets in the gap return `OFFSET_NOT_AVAILABLE`; it can add end-to-end latency.
- **KIP-966 ELR is NOT on by default in 4.0.** It is *introduced* in 4.0 but
  enabled by default only for **new clusters in 4.1**, gated by the
  `eligible.leader.replicas.version` feature level.
- **`unclean.leader.election.enable` default has been `false`** (favor consistency)
  since **0.11.0.0**; under KIP-966 this knob is superseded by
  `unclean.recovery.strategy`.
- **High watermark ≠ log-end-offset (LEO).** Consumers see up to the HW; the LEO
  can be ahead. Truncation/availability errors concern the region between HW and
  LEO.
- **Kafka commits to memory across ISR, not necessarily to disk.** By default Kafka
  does **not** fsync on every write; durability against simultaneous power loss
  relies on replica count + retention, not per-write fsync.

### KRaft / ZooKeeper removal

- **Kafka 4.0 released 2025-03-18.** Some blogs say "October 2024" or "2024" —
  wrong. (3.9, the **final ZK release**, shipped November 2024.)
- **"KRaft production-ready" has three distinct dates** often conflated: protocol
  early access in **2.8 (2021)**; production-ready **for new clusters only** in
  **3.3** (KIP-833, Oct 2022); ZK→KRaft **migration** (KIP-866) production-ready in
  **3.6** (late 2023). "Production-ready since 3.3" without the "new clusters only"
  qualifier is a common error.
- **KIP-595 is a Raft DIALECT, not textbook Raft.** It is **pull-based** (followers
  fetch) vs the paper's push-based `AppendEntries`, uses **offset/leader-epoch**
  instead of index/term, and has **no leader heartbeats** (fetches are the liveness
  signal). Calling KRaft "standard Raft" is incorrect.
- **The metadata log uses SNAPSHOTS (KIP-630), not ordinary log compaction.** The
  `__cluster_metadata` topic is **not** "just a compacted topic" — compaction was
  explicitly rejected (event/delta state; missed-tombstone divergence).
- **Dynamic KRaft quorums (KIP-853, 3.9) cannot convert an existing static
  quorum** (`controller.quorum.voters`) to dynamic — new dynamic quorums must be
  formatted as such. Static uses `controller.quorum.voters`; dynamic uses
  `controller.quorum.bootstrap.servers` (which need not list all voters).
- **KIP-631's wiki page still shows "under discussion"/experimental language** that
  is stale — the quorum controller shipped long ago and is the default in 4.0.
- **KRaft replaced ZooKeeper's metadata role only.** It did **not** change Kafka's
  per-partition ISR replication for normal topic data (that remains leader/follower
  replication, separate from the metadata Raft quorum).
- **The internal metadata topic** is usually `__cluster_metadata`; the very early
  2.8 preview called it `@metadata`. Both appear depending on era.
- **`metadata.version` is built on KIP-584's general feature-versioning mechanism,
  which predates KRaft** (shipped 2.7, ZK era) — don't attribute the whole
  feature-flag system to KRaft.

### Exactly-once semantics

- **Idempotent producer guarantees are per-partition and per-session only.** A
  producer restart (new PID, unless transactional) does not carry dedup state, and
  it provides no cross-partition atomicity (that's the transactions layer).
- **EOS is NOT magically global.** Consumers get exactly-once read visibility only
  with `isolation.level=read_committed` (the historical default was
  `read_uncommitted`), and even then KIP-98 notes consumers cannot guarantee
  reading an entire committed transaction atomically (compaction, segment deletion,
  seeks, partial subscription).
- **`read_committed` reads up to the Last Stable Offset (LSO), not the HW.** A
  single long-running open transaction blocks the LSO and stalls `read_committed`
  consumers on that partition. `read_uncommitted` consumers still read up to the HW.
- **Transaction markers are control records in the partition logs**, not stored
  only in the coordinator. They are never delivered to app code but **do occupy
  offsets** — so consumed offsets can appear to "skip" with no missing data.
- **The idempotent producer was NOT on by default from 0.11/1.0.** KIP-185 only
  *proposed* it; the actual default flip (`enable.idempotence=true` + `acks=all`)
  came with **KIP-679 in 3.0.0** (idempotence flag hit validation bug KAFKA-13598).
  `acks=all` became default cleanly in 3.0.
- **Don't confuse the three Streams EOS names:** `exactly_once` (eos-alpha, KIP-98,
  one producer per task); `exactly_once_beta` (eos-beta, KIP-447, 2.6,
  thread-producer); `exactly_once_v2` (eos-v2, same thing renamed by KIP-732 in
  3.0). eos-v2/beta requires brokers ≥ 2.5; eos-alpha and eos-beta were deprecated
  in 3.0.
- **RPC naming:** the shipped protocol uses `InitProducerId`; KIP-98's text used
  the older `InitPidRequest`. Also, `AddOffsetsToTxn` (coordinator-facing) and
  `TxnOffsetCommit` (group-coordinator-facing) are two **distinct** RPCs that
  together implement `sendOffsetsToTransaction`.
- **KIP-447 did not deprecate the old `sendOffsetsToTransaction(Map, String)`
  overload** — it *added* a new `ConsumerGroupMetadata` overload. Standalone
  (non-consumer-group) EOS still uses the String form.

### Consumer groups / rebalancing

- **KIP-345 AND KIP-429 both shipped in 2.4.0, not 2.3.** (The Streams
  `UPGRADE_FROM=2.3` setting is the source of confusion.) The KIP-429
  "process-during-rebalance" refinement was **2.5.0**.
- **KIP-848 GA = Kafka 4.0, NOT 3.7 or 3.8.** 3.7.0 = Early Access, 3.8.0 = Preview
  (both explicitly "not recommended for production"). Saying KIP-848 "shipped" in
  3.7 is a common error.
- **KIP-848 in 4.0 supports ONLY server-side assignors.** Client-side/custom
  pluggable assignors are **not** implemented (KAFKA-15282 closed "won't do"). For
  custom assignment logic, stay on the classic protocol or use Streams (KIP-1071).
- **The new protocol is opt-in on the client** (`group.protocol=consumer`); the
  default consumer value remains `classic` even in 4.0. What changed by default in
  4.0 is the **broker's** group coordinator implementation (the new Java
  coordinator becomes default on KRaft), not the per-consumer protocol.
- **"New group coordinator" ≠ "new rebalance protocol."** The Java coordinator can
  serve classic groups too; the KIP-848 protocol *requires* the new coordinator.
  Don't conflate "the coordinator was rewritten" with "every consumer now uses
  KIP-848."
- **KIP-848 is KRaft-only** (the new coordinator does not run under ZooKeeper) —
  moot in 4.0 but relevant for 3.7/3.8.
- **`group.protocol` has three values:** `classic`, `consumer` (KIP-848),
  `streams` (KIP-1071) — not a classic/consumer boolean.
- **KIP-1071 is Early Access in 4.1.0 only** — not GA, not production-recommended,
  and missing pieces (e.g. static membership, some topology updates).
- **Eager vs cooperative is a property of the assignor + negotiated
  `RebalanceProtocol` in the CLASSIC protocol (KIP-429).** The KIP-848 "consumer"
  protocol is incremental by construction and does not use EAGER/COOPERATIVE
  assignor negotiation — so KIP-429 cooperative rebalancing and the KIP-848
  next-gen protocol are **different mechanisms**.
- **The pre-KIP-848 "leader" that computed assignments is a CONSUMER (client), not
  the broker coordinator** — the coordinator only relayed. "The broker computed
  assignments pre-KIP-848" is a frequent misstatement.
- **KIP-345 fencing error is `FENCED_INSTANCE_ID` (error code 78)** — a duplicate
  `group.instance.id` fences the stale member rather than silently taking over.
- **Server-side regex (RE2/J) was NOT in the 3.7/3.8 stages** (which supported only
  client-evaluated regex) — be careful dating this capability.

### Tiered storage

- **Do NOT say tiered storage was production-ready in 3.6.** 3.6.0 was **Early
  Access** ("not recommended for use in production environments"). Production-ready/
  GA is **3.9.0**. The `KAFKA-7739` "Fix Version" 3.6.0 marks when it first
  *shipped* (Early Access), not GA.
- **JBOD/multiple-log-directories** was an explicit **non-goal** in the original
  KIP-405 text but **works by GA (3.9)** — state the version context.
- **"Disable once enabled" evolved.** Under KIP-405 alone you could not disable
  tiering without deleting the topic; **KIP-950 (3.9, KRaft-first)** added
  `remote.log.copy.disable` and `remote.log.delete.on.disable`.
- **Compacted topics are still NOT supported** (true at EA and GA 3.9) — tiering is
  for delete-retention topics only.
- **Only ONE partition per fetch request is served from remote storage** (a
  persistent limitation through/after GA) — don't describe remote reads as fully
  parallel per fetch.
- **RSM and RLMM are INTERFACES.** Apache Kafka ships the default RLMM (the
  `__remote_log_metadata` topic) but **no production RSM for a cloud** —
  implementations come from vendors (e.g. Aiven's open-source S3/GCS/Azure RSM).
  Don't imply an S3 implementation is built in.
- **Two switches are easy to confuse:** `remote.log.storage.system.enable` is the
  cluster/broker-level master switch; `remote.storage.enable` is the per-topic
  switch. Both default to `false`.
- **The remote tier can also be HDFS** (or any RSM target); object storage is the
  common case, not the only one. "Infinite retention to object stores" is a partial
  description.
- Keep KIP-405's mechanics (`EARLIEST_LOCAL_TIMESTAMP` sentinel, value −4;
  `OFFSET_MOVED_TO_TIERED_STORAGE`) distinct from **KIP-1005**'s later additions
  (ListOffsets v9 exposing the tiered offset).
