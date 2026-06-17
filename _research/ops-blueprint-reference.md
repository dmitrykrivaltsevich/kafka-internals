# Apache Kafka — Operations & Blueprint: Empirical Reference

> **Internal reference for documentation authors and fact-checkers.** This document synthesizes structured research notes across six topics: capacity planning, performance tuning, failure modes & runbooks, monitoring, cloud cost optimization, large-scale deployments, the distributed-commit-log pattern, and competitive comparisons. Every load-bearing number carries an inline source tag; full source URLs are listed per section. Treat vendor benchmarks as directional, and pin every version-dependent claim to its Kafka version (especially ZooKeeper-era vs. KRaft).
>
> **How to use this doc:** Each topic section gives synthesized findings, key facts (with sources), formulas/heuristics, case studies, and cautions. Consolidated quick-reference boxes (sizing formulas, partition heuristics, cost levers, comparative-systems table, common pitfalls) follow at the end.

---

## Table of contents

1. [Capacity planning & cluster sizing](#1-capacity-planning--cluster-sizing)
2. [Partition count: limits, costs, repartitioning](#2-partition-count-limits-costs-repartitioning)
3. [Performance tuning: throughput / latency / durability](#3-performance-tuning-throughput--latency--durability)
4. [Failure modes, incidents & runbooks](#4-failure-modes-incidents--runbooks)
5. [Monitoring: golden signals, JMX, alert thresholds](#5-monitoring-golden-signals-jmx-alert-thresholds)
6. [Cloud cost optimization](#6-cloud-cost-optimization)
7. [Real-world high-throughput deployments & what breaks at scale](#7-real-world-high-throughput-deployments--what-breaks-at-scale)
8. [The distributed commit log as an architectural pattern](#8-the-distributed-commit-log-as-an-architectural-pattern)
9. [Kafka vs. alternatives & inherent limitations](#9-kafka-vs-alternatives--inherent-limitations)

**Quick-reference appendices**

- [A. Capacity & sizing formulas (quick-reference)](#appendix-a-capacity--sizing-formulas-quick-reference)
- [B. Partition-count heuristics (box)](#appendix-b-partition-count-heuristics-box)
- [C. Cost levers (box)](#appendix-c-cost-levers-box)
- [D. Comparative systems table](#appendix-d-comparative-systems-table)
- [E. Common pitfalls / version caveats](#appendix-e-common-pitfalls--version-caveats)

---

## 1. Capacity planning & cluster sizing

### Synthesized findings

Sizing a Kafka cluster starts from target throughput and works outward. Partition count is set by `N ≥ max(t/p, t/c)` (target throughput over single-partition producer/consumer rates); one partition sustains "tens of MB/s." Broker count is the maximum of three constraints — peak throughput × replication factor against a per-broker ceiling, partitions divided by the per-broker partition cap, and a floor of 3 for availability. Plan egress at **≥ 2× ingress** (replication plus consumer fan-out). Keep the JVM heap small (~6 GB) and leave the rest of RAM to the OS page cache, which is the real read/write accelerator via zero-copy/`sendfile`.

### Key facts (with sources)

- **LinkedIn 2014 benchmark:** 3 producers with 3× async replication = **2,024,032 rec/s (193 MB/s)** at 100-byte messages; single producer SYNC (`acks=-1`) = **421,823 rec/s (40.2 MB/s)**; end-to-end **p99 3 ms**. — *Jay Kreps, LinkedIn "2 Million Writes/sec"*
- **Confluent 2020 OpenMessaging benchmark** (`i3en.2xlarge`, RF=3, `acks=all`, fsync OFF): **605 MB/s peak**, **p99 5 ms at 200 MB/s**; vs. Pulsar 305 MB/s, RabbitMQ ~38 MB/s. — *Confluent "Kafka Performance"*
- **Metadata ceiling, ZooKeeper:** ~**4,000 partitions/broker** and ~**200,000/cluster** (Kafka 1.1.0). KRaft targets millions (Confluent lab: **2M**; Instaclustr: ~**600k/broker**, bounded by `vm.max_map_count`). — *Confluent 200K post; Instaclustr*
- **Vendor-benchmark caution:** Redpanda's "10×/3×" claims used a crippled Kafka (per-batch fsync, Java 11); on equal hardware Kafka matched or beat it (~**1900 vs 1400 MB/s**). — *Jack Vanlightly*

### Formulas & heuristics

- **Partitions:** `N ≥ max(t/p, t/c)` — target throughput `t` over single-partition producer `p` and consumer `c` rates.
- **Per-partition planning rate:** ~**10 MB/s** (conservative; can be tens of MB/s).
- **Brokers:** `max(peak × RF / per-broker-ceiling, partitions / max_per_broker, 3)`.
- **Egress:** plan for **≥ 2× ingress**.
- **Memory split:** heap ~6 GB; remainder → page cache.

### Cautions

- The 605 MB/s and "p99 5 ms at 200 MB/s" figures are from a *specific* test (1 KB messages, fsync off, low-load reads from cache) — illustrative baselines, not guarantees.
- "2 million writes/sec" was three producers combined with async replication; durable single-producer throughput was ~422K rec/s.

### Sources

- Jay Kreps / LinkedIn — *Benchmarking Apache Kafka: 2 Million Writes Per Second on Three Cheap Machines* — https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines
- Confluent — *Kafka Performance, Latency, Throughput and Test Results* — https://developer.confluent.io/learn/kafka-performance/
- Confluent — *Apache Kafka Supports 200K Partitions Per Cluster* — https://www.confluent.io/blog/apache-kafka-supports-200k-partitions-per-cluster/
- Instaclustr — *KRaft Part 3: Maximum Partitions and Conclusions* — https://www.instaclustr.com/blog/apache-kafka-kraft-abandons-the-zookeeper-part-3-maximum-partitions-and-conclusions/

---

## 2. Partition count: limits, costs, repartitioning

### Synthesized findings

Partition count is a trade-off between **parallelism** (throughput) and **overhead** (failover time, latency, file descriptors, memory, controller load). The canonical heuristic (Jun Rao, 2015) is `partitions = max(t/p, t/c)`, with a single partition able to sustain "10s of MB/sec." ZooKeeper-era guidance caps ~4,000/broker and ~200,000/cluster, driven by `O(partitions)` controller failover and metadata-reload time. Kafka 1.1.0 (KIP-227 + batched/async controller writes) actually unlocked the 200K number. KRaft/KIP-500 removes ZooKeeper and targets "a million partitions or more" via an in-memory, log-replicated metadata quorum with near-instant failover (Confluent lab: 2M partitions).

The two hardest operational realities: **you cannot decrease partitions**, and **increasing them on a keyed topic breaks per-key ordering and co-partitioned state** (`hash(key) % N` remaps keys). The accepted fix is to create a new topic at the target count and migrate.

### Key facts (with sources)

- **Sizing formula (Jun Rao, Confluent, Mar 2015):** `Partitions = max(t/p, t/c)` — `t` = target throughput, `p`/`c` = measured single-partition producer/consumer throughput.
- **Single-partition baseline:** "one can produce at 10s of MB/sec on just a single partition"; conservative planning number ~**10 MB/s**. — *Jun Rao*
- **ZooKeeper-era limits:** "up to 4,000 partitions per broker and up to 200,000 partitions per cluster." — *Confluent 200K post*
- **What enabled 200K (Kafka 1.1.0, KIP-227):** controlled shutdown of 5 brokers / 50,000 partitions dropped from **6.5 minutes (1.0.0) → 3 seconds (1.1.0)** (logging fix → 30 s; async API → 3 s). — *Confluent 200K post*
- **Controller failover (ZK era):** metadata reload for 100,000 partitions dropped **28 s (1.0.0) → 14 s (1.1.0)** — still `O(partitions)`, the structural reason ZK capped cluster partition counts. — *Confluent 200K post*
- **Leader-election / failover cost (Jun Rao):** ~**5 ms to elect a leader for one partition**; a broker holding ~1,000 leaderships takes up to ~5 s on unclean loss. Controller failover adds ZK metadata init at ~**2 ms/partition** → 10,000 partitions ≈ +20 s unavailability.
- **KIP-500 / KRaft target:** "a million partitions or more"; hot standby controllers mean "controller failover will not require a lengthy reloading period." — *KIP-500 wiki; Confluent "Kafka Needs No Keeper"*
- **KRaft 2M-partition lab:** Confluent ran 2,000,000 partitions (10× ZK max); controlled-shutdown and recovery times "greatly improved." — *Confluent KRaft docs*
- **KRaft max-partition benchmark:** ~**600,000 partitions on a single KRaft broker** (Kafka 3.2.1, 64 GB EC2, RF=1) vs. ~80,000 max on a whole ZK cluster; extrapolated 3-broker KRaft ≈ **1.9M**. Hit Linux `vm.max_map_count` default **65,530** (2 mmap areas/partition → ~32,765 default ceiling/broker); KAFKA-14204 (fixed 3.3.0) made creation "painfully slow." — *Instaclustr Part 3*
- **Throughput does NOT scale linearly:** peak producer rate ~**2,000,000 msg/s at 100 partitions**; latency rises sharply past ~**1,000 partitions** (3× r6g.large, Kafka 3.1.1, `acks=all`, 8-byte msgs); ZooKeeper vs. KRaft throughput identical in this test. — *Instaclustr Part 1*
- **File descriptors:** set broker `nofile` ≥ **100,000**; need ≈ (partitions × partition_size / segment_size) + connections. Production clusters run "more than 30 thousand open file handles per broker." — *Jun Rao / Confluent*
- **Client memory:** producer should allocate "at least a few tens of KB per partition being produced" (`buffer.memory`). — *Jun Rao*
- **Replication latency:** "replicating 1000 partitions from one broker to another can add about 20 ms latency"; derived per-broker cap ≈ `100 × b × r`. — *Jun Rao*
- **Keyed-topic repartitioning:** keys map via `hash(key) % numPartitions` (e.g., `7654321 % 4 = 1` but `7654321 % 6 = 3`); per-key ordering breaks across the resize boundary, and Kafka Streams state stores are partitioned so "the associated state does not follow them." Kafka does **not** support decreasing partitions. — *Confluent docs; Arpit Bhayani*
- **Migration pattern:** create a NEW topic at the target count, copy/replay old → new, then switch producers/consumers (dual-write + drain). Confluent advises **over-partitioning** up front. — *Confluent docs*
- **Cost:** cloud Kafka is partition-metered — commonly cited at ~**$13/partition/year** on Confluent Cloud, with a per-(Basic/Standard)-cluster cap around **4,096** partitions. *(Secondary/community source; verify against live pricing.)*

### Formulas & heuristics

- `Partition count = max(t/p, t/c)` — take the larger of producer-side and consumer-side requirements; use the **slowest consumer path** for `c`, not broker disk ceiling.
- Worked example: 100 MB/s topic ÷ 10 MB/s/partition ≈ **10 partitions** as a starting point.
- Per-broker cap to bound replication latency: `≤ 100 × (#brokers) × (RF)`.
- File descriptors: `open files ≥ (#partitions) × (partition size / segment size) + connections`; `ulimit nofile ≥ 100,000`.
- Failover mental model: unclean broker loss ≈ `5 ms × #leader-partitions-on-broker`; controller loss adds ≈ `2 ms × total partitions` on ZooKeeper (KRaft removes the second term).
- KRaft per-broker hard ceiling on Linux: `vm.max_map_count` default 65,530 ÷ 2 mmaps/partition ≈ **32,765 partitions/broker** unless raised.

### Case studies

- **LinkedIn:** 100+ clusters / 4,000+ brokers / 100,000+ topics / ~7,000,000 partitions / **7 trillion msgs/day**; largest clusters >140 brokers and ~1M replicas. Pain: ZK-era controllers under memory pressure → cascading controller failures; slow broker start/stop bottlenecked rolling deploys. Mitigations: custom LinkedIn Kafka branch, broker "maintenance mode."
- **Uber:** trillions of messages + multiple PB/day. Per-partition consumer fan-out and head-of-line blocking drove uForwarder (push-based consumer proxy, 1,000+ services). Tiered storage cuts broker recovery time (no cold-data re-replication on rejoin).
- **Confluent KRaft lab:** 2,000,000 partitions (10× ZK max) ran successfully with greatly reduced shutdown/recovery times.
- **Instaclustr KRaft test:** ~600,000 partitions on one broker (vs. ~80,000 on a whole ZK cluster); but throughput peaked at 100 partitions and degraded past ~1,000 — *scalability ≠ performance*.

### Cautions

- **Version-dependent:** the ~4,000/broker and ~200,000/cluster numbers are ZooKeeper-era and only safe in Kafka 1.1.0+ (KIP-227 + batched controller writes). Pre-1.1.0, 50K partitions = 6.5 min controlled shutdown. Don't present 200K as a universal constant.
- **Commonly misstated:** KRaft's "millions of partitions" is a **target + lab result (2M)**, not a per-broker production SLA. Real per-broker counts are gated by memory, FDs, `vm.max_map_count` (~32k/broker default), fetcher overhead, and rebalance time. "KRaft means partitions are free" is wrong.
- The "2M partitions: X s vs Y s" controlled-shutdown numbers circulating online (e.g., OSO's ~120 s vs 20–30 s; "8.2× throughput") come from a **secondary vendor blog (OSO)**, not Confluent's primary chart. Cite the 200K post's 1.1.0 numbers (6.5 min→3 s; 28 s→14 s) as authoritative.
- Throughput does **not** increase monotonically with partitions (peak ~100, degrades past ~1,000). Over-partitioning to chase throughput is a documented anti-pattern.
- You **cannot reduce** partition count, and increasing it on a keyed topic silently breaks per-key ordering and co-partitioned Streams state — the most under-appreciated constraint. Treat partition count as near-immutable for keyed topics.
- Jun Rao's 5 ms/2 ms figures are 2015-era ZooKeeper/hardware numbers — teach the mechanism, don't predict 2026 behavior.
- Cloud per-partition pricing (~$13/partition/year) and per-cluster caps (~4,096) change frequently — verify against live docs.

### Sources

- Jun Rao — *How to Choose the Number of Topics/Partitions in a Kafka Cluster?* — https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/
- Confluent — *Apache Kafka Supports 200K Partitions Per Cluster* — https://www.confluent.io/blog/apache-kafka-supports-200k-partitions-per-cluster/
- *KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum
- Confluent — *Kafka Needs No Keeper* — https://www.confluent.io/blog/removing-zookeeper-dependency-in-kafka/
- Confluent Documentation — *KRaft Overview* — https://docs.confluent.io/platform/current/kafka-metadata/kraft.html
- Confluent Documentation — *Choose and Change the Partition Count in Kafka* — https://docs.confluent.io/kafka/operations-tools/partition-determination.html
- Instaclustr — *KRaft Part 3: Maximum Partitions and Conclusions* — https://www.instaclustr.com/blog/apache-kafka-kraft-abandons-the-zookeeper-part-3-maximum-partitions-and-conclusions/
- Instaclustr — *KRaft Part 1: Partitions and Data Performance* — https://instaclustr.medium.com/apache-kafka-kraft-abandons-the-zoo-keeper-part-1-partitions-and-data-performance-5b0af26184ca
- LinkedIn Engineering — *How LinkedIn customizes Apache Kafka for 7 trillion messages per day* — https://www.linkedin.com/blog/engineering/open-source/apache-kafka-trillion-messages
- Uber Engineering — *Kafka Async Queuing with Consumer Proxy (uForwarder)* — https://www.uber.com/blog/kafka-async-queuing-with-consumer-proxy/
- Arpit Bhayani — *When You Increase Kafka Partitions* — https://arpitbhayani.me/blogs/kafka-partitions/
- OSO — *Apache Kafka's KRaft Protocol* (secondary 2M seconds) — https://oso.sh/blog/apache-kafkas-kraft-protocol-how-to-eliminate-zookeeper-and-boost-performance-by-8x/
- Confluent Documentation — *Running Kafka in Production* — https://docs.confluent.io/platform/current/kafka/deployment.html

---

## 3. Performance tuning: throughput / latency / durability

### Synthesized findings

Kafka tuning is a **three-way tradeoff** between throughput, latency, and durability — you generally improve one by sacrificing another. On the **producer**, the dominant lever is batching (`batch.size` + `linger.ms`): larger batches and a non-zero linger amortize per-request overhead (reported 10–50× throughput gains) at the cost of latency equal to the linger window. Confluent's throughput recipe: `batch.size` ~64 KB–1 MB, `linger.ms` 5–100 ms, `compression.type=lz4` (or `zstd` for storage), `acks=all` with idempotence; the latency recipe keeps linger near 0–1 ms.

**Compression** is a CPU-vs-ratio tradeoff: snappy/lz4 fastest (lowest CPU), zstd ~20–30% better ratio for moderately more CPU, gzip highest ratio but highest CPU (usually avoided). On the **consumer**, `fetch.min.bytes` + `fetch.max.wait.ms` control read batching; `max.poll.records` governs processing-vs-rebalance-risk. On **brokers**, key knobs are `num.network.threads`, `num.io.threads` (~8/disk), `num.replica.fetchers`, socket buffers, and `log.segment.bytes`. **OS-level tuning** is critical because Kafka leans entirely on the page cache and zero-copy/`sendfile`: `vm.swappiness=1`, FDs ≥ 100,000, JVM heap ~6 GB, raise `vm.max_map_count`.

### Key facts (with sources)

**Producer**

- `batch.size` default **16384 bytes (16 KB)**; `linger.ms` default **0** in classic Kafka but **raised to 5 ms** in newer Apache Kafka. Recommend `batch.size` 32–64 KB and `linger.ms` 5–100 ms for throughput. — *Confluent/Strimzi*
- **Batching tradeoff (quantified):** with `linger.ms=0`, batches averaged only ~**1,215 bytes** despite a 16 KB limit; `linger=1500ms` produced ~**275 KB** batches. For a low-rate workload (200 rec/s of 1000 B), increasing linger **decreased** throughput and **increased** latency — defaults were optimal. — *Confluent producer-hands-on course*
- **Throughput recipe:** `batch.size=1MB`, `linger.ms=10`, `acks=all`, `min.insync.replicas=2`. — *Intel / Confluent learn page*
- **Baseline numbers:** peak ~**605 MB/s**; **p99 5 ms at 200 MB/s** (1 KB msgs, ~200K msg/s); low load served from page cache. — *Confluent*
- **acks tradeoff:** `acks=0` (lowest latency/durability), `acks=1` (leader-only, balanced), `acks=all/-1` (highest durability + latency). Recommend `acks=all` + `enable.idempotence=true` + `min.insync.replicas=2` with RF 3. — *Confluent Cloud / Strimzi*
- `max.in.flight.requests.per.connection` default **5**; with idempotence enabled you keep 5 and preserve ordering; without idempotence, ordering on retry needs `max.in.flight=1`. — *Strimzi/Confluent*
- `buffer.memory` default **33554432 (32 MB)**; must be ≥ `batch.size`; if it fills, the producer blocks (or throws after `max.block.ms`). — *Strimzi*

**Compression (Cloudflare production data)**

- **Speed (lzbench, ~1 MB/600-record batches):** LZ4 = 594 MB/s compress, 2,428 MB/s decompress; Snappy = 446 / 1,344; Zstd lvl -1 = 409 / 844. LZ4 is the fastest decompressor by a wide margin.
- **Ratio (HTTP-request data):** Zstd lvl 6 = **4.5×**; Gzip = 3.58×; Snappy = 2.35×; LZ4 = 1.81×. Cloudflare chose Zstandard, saving "hundreds of gigabits of internal traffic and terabytes of flash storage" and cancelling a hardware expansion. Avg msg 1,594 B; peak 100 Gbps / 7.5M msg/s.
- **General guidance:** throughput example — snappy/lz4 ~3400 msg/s vs zstd ~2180 vs gzip ~830. zstd often a strong default in Kafka 3.0+; level tuning via KIP-390/KIP-780 (~Kafka 3.8). — *Confluent, Conduktor, Trendyol*
- **Trendyol:** chose zstd level 3, achieved ~**70% reduction** in message size *(throughput table image-only — cite qualitatively)*.

**Consumer**

- `fetch.min.bytes` default **1 byte** (optimized for ms latency, NOT cost); `fetch.max.wait.ms` default **500 ms**; `fetch.max.bytes` ~**52428800 (50 MB)**; `max.partition.fetch.bytes` default **1 MB**. — *Strimzi*
- **New Relic case study (high-value):** raising `fetch.min.bytes` on consumers of low-throughput topics (and tuning `fetch.max.wait.ms`) reduced **cluster CPU by 15%** from a change in ONE application, enabling broker scale-down. Mechanism: apps polling too frequently send many tiny fetch requests → more broker CPU.
- **Consumer memory:** fetch memory ≈ `#brokers × fetch.max.bytes`, and ≈ `#partitions × max.partition.fetch.bytes`. — *Strimzi*
- `max.poll.records` default **500** — caps records returned per `poll()` (does NOT change fetch). Larger (1000–5000) improves batch throughput but risks exceeding `max.poll.interval.ms` (default **300000 ms / 5 min**) → rebalance.
- **Liveness:** `session.timeout.ms` default ~**10000 ms**; `heartbeat.interval.ms` default ~**3000 ms** (~⅓ of session timeout). `enable.auto.commit` default **true** but recommended **false** for reliability.

**Broker & OS**

- **Threads:** `num.network.threads` default 3 (recommend 8–12); `num.io.threads` default 8 (~8/disk, e.g. 16–24); `num.replica.fetchers` default 1 (recommend 4–8); `num.recovery.threads.per.data.dir` default 1 (raise to speed startup).
- **Socket buffers:** `socket.send/receive.buffer.bytes` default **102400 (100 KB)**; raise to **1 MB** on high-BDP links. `socket.request.max.bytes` default 100 MB; `queued.max.requests` default 500.
- **Log segments:** `log.segment.bytes` default **1073741824 (1 GB)**; `message.max.bytes` default 1 MB.
- **File descriptors:** raise to ≥ **100,000** per broker process. — *Confluent/Cloudera*
- **Swappiness:** `vm.swappiness=1` (NOT 0 — 0 forbids swap and removes the OOM safety net). — *Cloudera/Confluent*
- **JVM heap & page cache:** Kafka does NOT need heap >6 GB. Example flags: `-Xms6g -Xmx6g -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35`. On a 64 GB box with 6 GB heap, ~28–30 GB serves page cache. Recommended hardware: 64 GB RAM (32 GB min), 24 cores, 12×1TB disks RAID10 or JBOD. — *Confluent*
- **mmap & dirty pages:** `vm.max_map_count` default 65536 → raise to ≥ **262144**; `vm.dirty_background_ratio=5`.
- **TCP/network:** `net.core.rmem_max`/`wmem_max` ~16 MB+; XFS with `noatime`; NVMe SSD preferred.
- **Canonical benchmark (Jay Kreps, 2014):** single producer no replication = **821,557 rec/s (78.3 MB/s)**; 3× async = 786,980 (75.1 MB/s); 3× SYNC `acks=-1` = **421,823 (40.2 MB/s)**; three producers 3× async = **2,024,032 (193 MB/s)**. Single consumer 940,521 (89.7 MB/s); three consumers 2,615,968 (249.5 MB/s). E2E latency: median 2 ms, p99 3 ms, p99.9 14 ms. **Key insight: sync replication roughly HALVED throughput.**
- **Durability-vs-latency (Redpanda OMB, context for critiques):** Kafka's ISR replication does NOT fsync by default; forcing per-message fsync degrades tail latency under load. With `acks=all`, one comparison: Kafka ~0.92 ms avg / 3 ms p99 vs Redpanda 2.09 ms avg / 6 ms p99, but Kafka's tail worsened at sustained load. *Treat vendor benchmarks skeptically.*

### Formulas & heuristics

- `num.io.threads ≈ 8 × (#data disks)` — bounded by CPU cores and disk bandwidth.
- `Broker FDs ≈ (#partitions × partition_size) / segment_size + 1 per connection`; `ulimit nofile ≥ 100,000`.
- `Socket buffer = bandwidth-delay product = link_bandwidth × round-trip latency`.
- `Consumer fetch memory ≈ #brokers × fetch.max.bytes`, and ≈ `#partitions × max.partition.fetch.bytes`.
- `heartbeat.interval.ms ≈ session.timeout.ms / 3`.
- `buffer.memory ≥ batch.size` (+ headroom for compression + in-flight).
- `vm.max_map_count ≥ 262144` (above the number of `.index` files).
- JVM heap small (~6 GB); provision page cache for roughly `write_throughput × 30 seconds`.
- Set `linger.ms > 0` **only** when produce rate is high enough to fill `batch.size` within the window.
- **Tradeoff triangle:** `linger.ms↑` trades latency for throughput; compression trades producer CPU for network/disk; `acks=all` trades latency for durability. Optimize for at most two.

### Case studies

- **Cloudflare:** zstd-6 = 4.5× vs snappy 2.35×, lz4 1.81×, gzip 3.58× on ~1 MB/600-record batches; switched to Zstandard, saved "hundreds of gigabits … terabytes of flash," cancelled a hardware expansion. A deliberate ratio-over-speed choice.
- **New Relic:** raised `fetch.min.bytes` on low-throughput-topic consumers; a single-app change cut whole-cluster broker CPU **15%**, enabling scale-down.
- **Jay Kreps / LinkedIn (2014):** 2,024,032 rec/s (193 MB/s) with three producers + async; sync replication halved single-producer throughput to 421,823 rec/s (40.2 MB/s).
- **Trendyol:** zstd level 3 → ~70% message-size reduction.
- **Confluent reference test:** ~605 MB/s peak; p99 5 ms at 200 MB/s — the canonical "high throughput AND low latency, but not maxed simultaneously" demonstration.

### Cautions

- **`linger.ms` default:** classic/older Kafka = 0; newer builds = 5. State the version — "linger defaults to 0" is now frequently misstated.
- **Compression default:** "zstd is the default" is WRONG — `compression.type` defaults to `none`/`producer`. zstd arrived in Kafka 2.1 (KIP-110); fine-grained level tuning is recent (KIP-390/KIP-780, ~3.8+).
- Compression is **per batch** — tiny batches compress poorly regardless of algorithm. "zstd gives 4.5×" is data- and batch-size-dependent.
- `max.poll.records` does NOT change how much is fetched — it caps cached records per `poll()`; tuning it for throughput is a common misconception (it protects against rebalances).
- `fetch.min.bytes=1` is tuned for latency, NOT cost — Kafka does **not** batch reads by default.
- `vm.swappiness=1`, NOT 0.
- **`acks=all` alone does NOT guarantee data on disk** — Kafka acks once replicas have it in page cache; no per-message fsync by default. Durability claims must specify fsync (`flush.messages`/`flush.ms`). Central critique in Redpanda/AutoMQ comparisons.
- "2M writes/sec" requires three producers + async; with `acks=-1` a single producer drops to ~422K rec/s.
- "10–50×" and "67% from 3× compression" figures are vendor/community roll-ups — directional; prefer primary sources (Cloudflare, Kreps, Confluent, New Relic).

### Sources

- Confluent — *Kafka Performance* — https://developer.confluent.io/learn/kafka-performance/
- Confluent — *Hands On: Tuning the Apache Kafka Producer Client* — https://developer.confluent.io/courses/architecture/producer-hands-on/
- Confluent Cloud — *Optimize Clients for Throughput* — https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html
- Confluent — *Running Kafka in Production* — https://docs.confluent.io/platform/current/kafka/deployment.html
- Confluent — *Kafka Producer Configuration Reference* — https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
- Strimzi — *Optimizing Kafka producers* — https://strimzi.io/blog/2020/10/15/producer-tuning/
- Strimzi — *Optimizing Kafka consumers* — https://strimzi.io/blog/2021/01/07/consumer-tuning/
- Strimzi — *Optimizing Kafka broker configuration* — https://strimzi.io/blog/2021/06/08/broker-tuning/
- Cloudflare — *Squeezing the firehose: getting the most from Kafka compression* — https://blog.cloudflare.com/squeezing-the-firehose/
- New Relic — *Tuning Apache Kafka Consumers to maximize throughput and reduce costs* — https://newrelic.com/blog/how-to-relic/tuning-apache-kafka-consumers
- Jay Kreps / LinkedIn — *Benchmarking Apache Kafka: 2 Million Writes Per Second* — https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines
- LinkedIn Engineering — *How LinkedIn customizes Apache Kafka for 7 trillion messages per day* — https://www.linkedin.com/blog/engineering/open-source/apache-kafka-trillion-messages
- Cloudera — *Kafka Performance: Virtual Memory Tuning* — https://docs.cloudera.com/runtime/7.2.6/kafka-performance-tuning/topics/kafka-tune-broker-syslevel-virtual-memory.html
- Red Hat — *Streams for Apache Kafka: Broker Configuration Tuning* — https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/2.9/html/kafka_configuration_tuning/con-broker-config-properties-str
- Trendyol Tech — *Optimizing Kafka Performance Through Data Compression* — https://medium.com/trendyol-tech/optimizing-kafka-performance-through-data-compression-330fb31a0827
- Conduktor — *Kafka Performance Tuning Cheatsheet* — https://www.conduktor.io/glossary/kafka-performance-tuning-guide
- Redpanda — *Redpanda vs. Kafka with KRaft: Performance update* — https://www.redpanda.com/blog/kafka-kraft-vs-redpanda-performance-2023
- Apache Kafka — *Producer & Consumer Configs reference* — https://kafka.apache.org/41/configuration/producer-configs/

---

## 4. Failure modes, incidents & runbooks

### Synthesized findings

Most Kafka incidents are **downstream symptoms** of a small set of root causes: a broker/disk going away, a follower falling behind (network, disk I/O, GC pause), skewed load, or a misbehaving transactional/consumer client. The highest-priority alerts are `UnderReplicatedPartitions` (>0 sustained), `UnderMinIsrPartitionCount`, `OfflinePartitionsCount` (>0 = unavailability), `UncleanLeaderElectionsPerSec` (any non-zero = data loss), `ActiveControllerCount` (must equal 1 cluster-wide), `RequestHandlerAvgIdlePercent`/`NetworkProcessorAvgIdlePercent` (keep >30%), and the LSO lag gap (hanging transactions).

**Durability baseline:** `replication.factor=3`, `min.insync.replicas=2`, `acks=all`, `unclean.leader.election.enable=false` — makes data loss require multiple simultaneous failures. The two biggest data-loss footguns are **unclean leader election** (out-of-sync replica becomes leader and truncates committed data) and a **silent `min.insync.replicas`/RF mismatch** (`effectiveMinIsr` caps the configured value, so RF=1 still accepts writes even with `min.insync.replicas=2`).

### Key facts (with sources)

- **UnderReplicatedPartitions:** constant non-zero usually = a broker down; fluctuating with all brokers up = a performance bottleneck (slow follower, network, disk, GC). URP is a symptom, not a root cause. — *oneuptime/drdroid/VCloudLabs*
- **OfflinePartitionsCount > 0** = partitions have NO leader, unavailable; alert immediately. Causes: leader loss, network partition, loss of sole in-sync replica, leader-disk corruption. — *Ahmed Abouzied; Shoreline*
- **Unclean leader election:** an out-of-sync replica is elected leader, Kafka resets ISR to a singleton and sets `LeaderRecoveryState=RECOVERING`; messages beyond the new leader's log are lost forever. Any `UncleanLeaderElectionsPerSec > 0` is a data-loss event. — *Conduktor*
- **`unclean.leader.election.enable` default is FALSE** in modern Kafka/KRaft, but was **TRUE before 0.11.0** — a version-dependent footgun. Datadog had temporary data loss on a pre-0.11.0 cluster, recovered only because they dual-wrote to a secondary cluster. — *Datadog*
- **Silent `min.insync.replicas` trap:** `effectiveMinIsr()` caps configured `min.insync.replicas` to the actual replica count, so RF=1 accepts writes even when `min.insync.replicas=2`. Verify `min.insync.replicas ≤ replication.factor`. — *Conduktor*
- **`replica.lag.time.max.ms` default 30000 ms (30 s):** if a follower hasn't fetched or reached the leader's LEO within this window it's dropped from ISR. Set `replica.fetch.wait.max.ms < replica.lag.time.max.ms`. Introduced by KIP-16.
- **ISR thrash root causes:** network latency spikes, high follower load, GC pauses, disk I/O bottlenecks. One documented case: hours of thrash after deleting a high-lag consumer, fixed by restarting the controller broker. — *meshIQ; Cloudera (KAFKA-4674)*
- **GC pauses:** a 500 ms pause can trigger rebalances and (in ZK mode) session timeouts; multi-second Full GC (2 s, 12.5 s cited) causes ISR drops/timeouts. Use G1GC: `-Xms6g -Xmx6g -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M`. — *Conduktor/Confluent*
- **Heap ≤ 6 GB** so remaining RAM is page cache. LinkedIn's busy clusters: ~21 ms p90 GC pause, <1 young GC/sec. A 32 GB G1GC heap can incur 100–200 ms pauses. GC alert thresholds: P99 >50 ms = warning, >200 ms = critical. Switch to ZGC only for large heaps (>16 GB, Java 17+/21+) at ~5–10% CPU / ~20% memory overhead. — *Conduktor*
- **Rebalance storm:** exceeding `max.poll.interval.ms` (default 300000 ms / 5 min) during slow processing evicts the consumer → it rejoins → another rebalance (self-sustaining). Fixes: increase `max.poll.interval.ms`, reduce `max.poll.records`, adopt `CooperativeStickyAssignor` (default-capable since 2.4). — *Conduktor/Confluent/AWS*
- **Static membership (KIP-345):** set a stable `group.instance.id` and a large enough `session.timeout.ms` to preserve assignments across restarts/deploys.
- **Hot/skewed partition:** adding partitions does NOT fix a hot key (it still hashes to one partition). Fixes: redesign key, salt hot keys (only when ordering not required), custom partitioner. Cloudflare hit this when a client-library abstraction funneled most messages to one partition. — *Confluent/AutoMQ/Cloudflare*
- **Request-handler saturation:** `RequestHandlerAvgIdlePercent` <20% = potential problem, <10% = active problem; keep >30% (healthy 60–80%). `NetworkProcessorAvgIdlePercent` <0.30 = network threads saturated. — *Confluent/Instaclustr/meshIQ*
- **Latency breakdown:** `TotalTimeMs = RequestQueueTimeMs + LocalTimeMs + RemoteTimeMs + ResponseQueueTimeMs + ResponseSendTimeMs`. — *Confluent "Debug Apache Kafka Pt.2"*
- **Hanging transaction:** a crashed transactional producer pins the LSO → `read_committed` consumers stall, ALL later messages on the partition become invisible, and on compacted topics the cleaner cannot advance (unbounded growth). — *Conduktor/KIP-664*
- **`transaction.timeout.ms`** (producer, default 60000 ms) is capped by broker `transaction.max.timeout.ms` (default **900000 ms / 15 min**) — a misconfigured producer can freeze a partition for `read_committed` consumers up to 15 minutes. Detect via `LastStableOffsetLag` / `PartitionsWithLateTransactionsCount`.
- **Hanging-txn recovery (KIP-664):** `kafka-transactions.sh --describe-producers`, `--find-hanging` (requires `--max-transaction-timeout`), `--abort` (by start offset OR producerId+epoch+coordinatorEpoch). Shipped in **Kafka 3.0**.
- **Producer fencing:** each `transactional.id` has a broker epoch; a new producer with the same id bumps the epoch and the old one gets `ProducerFencedException`. "Fencing avalanche": two pods sharing one `transactional.id` repeatedly fence each other — give each a unique id. KIP-588 made some epoch errors recoverable. — *KIP-588; dev.to*
- **JBOD disk failure:** Kafka marks a log dir bad on first IOException; one bad dir → broker stops serving that dir's replicas; ALL dirs bad → broker offline. KRaft (KIP-858) adds `AssignReplicasToDirs` RPC for reconciliation. Mechanism originally KIP-112.
- **Full disk:** broker crashes with `No space left on device` and `Exit.halt(1)` (no graceful shutdown). Recoverable if RF≥2. Emergency: stop broker → delete only OLD segments (never the newest `.log`) or dynamically lower retention → restart at 10–20% free. Alert at 70%/85% disk and `OfflineLogDirectoryCount>0`. — *Conduktor*
- **Scale references:** LinkedIn >100 clusters / ~4000 brokers / ~7M partitions / >7T msgs/day (earlier: ~1100 brokers, 800B msgs/day, 13M msgs/s peak, 2.75 GB/s). Cloudflare: 1T+ msgs, ~330 nodes, 14 clusters, RF≥3.

### Formulas & heuristics

- **Durability triad:** `replication.factor=3` + `min.insync.replicas=2` + `acks=all` (+ `unclean.leader.election.enable=false`) → single failure is non-data-losing; loss needs ≥2 simultaneous failures.
- **Slow-follower triage order:** (1) broker down? (2) shrink concentrated on one broker? → its disk/network; (3) GC pauses? (4) leader write rate > follower fetch rate? → raise `num.replica.fetchers` / fix disk.
- **Latency root-cause map:** high `RequestQueueTimeMs` → too few io threads; high `LocalTimeMs` → leader disk/page-cache/GC; high `RemoteTimeMs` → slow followers / `min.insync.replicas` waits; high `ResponseQueueTimeMs` → too few network threads.
- **Heap-vs-page-cache:** cap heap ~6 GB (8 GB only for >10k partitions); never give Kafka a huge heap "to go faster."
- **Rebalance-storm recipe:** `CooperativeStickyAssignor` + static membership (unique `group.instance.id`) + `session.timeout.ms` sized to cover deploys + `max.poll.interval.ms ≥` worst-case batch time + reduce `max.poll.records`; idempotent processing + commit in `onPartitionsRevoked`.
- **Hot-partition rule:** do NOT add partitions for a hot KEY; redesign or salt the key.
- **Full-disk emergency:** stop → delete only OLD segments (keep newest `.log`/`.index`/`.timeindex` per partition) OR lower retention via `kafka-configs.sh` → restart at 10–20% free → verify `--under-replicated-partitions` returns empty.
- **Capacity sanity (Cloudflare):** one consumer max per partition — you cannot scale consumption beyond partition count during an incident; size partitions up front.

### Case studies

- **Datadog:** pre-0.11.0 unclean leader election (old default TRUE) → temporary data loss; recovered only via dual-write to a secondary cluster. Lesson: verify durability defaults against your version.
- **Cloudflare:** 1T+ msgs over ~8 years across 14 clusters / ~330 nodes, RF≥3. Worst recurring incident = partition skew (a client-library abstraction funneled most messages to one partition), plus the hard "one consumer per partition" ceiling.
- **LinkedIn:** >100 clusters, ~4000 brokers, ~7M partitions. Ran Cruise Control for rebalancing, built a Kafka Audit system, emits custom per-partition throughput metrics (Kafka doesn't expose them).
- **Cloudera (KAFKA-4674-class):** hours of ISR thrash + broker disconnects after deleting a high-lag consumer; resolved by restarting the controller broker.
- **Confluent KRaft lab:** ~2,000,000 partitions/cluster and ~600,000/broker — demonstrating the 4k/200k limits are ZK-era.

### Cautions

- `unclean.leader.election.enable` default is version-dependent: TRUE before 0.11.0, FALSE after (and KRaft). Pin it to the version. Any `UncleanLeaderElectionsPerSec > 0` is actual data loss.
- `min.insync.replicas` alone guarantees nothing if RF is lower (`effectiveMinIsr` caps silently). Verify per topic.
- `RequestHandlerAvgIdlePercent` has historically been reported inconsistently (rate vs gauge — Datadog integrations-core #516, KAFKA-7354); confirm 0–1 fraction before trusting thresholds.
- Adding partitions is the most common WRONG fix for hot partitions, can't be undone, and breaks ordering/compaction.
- The 4,000/broker and 200,000/cluster numbers are conservative ZK-mode rules, largely obsolete under KRaft. ZooKeeper mode is **deprecated in 3.5, removed in 4.0** — ZK-specific runbook steps apply only to older clusters.
- `transaction.timeout.ms` is bounded by broker `transaction.max.timeout.ms` (15 min) — don't assume the 60 s producer default is the worst case.
- `kafka-transactions.sh` tooling (KIP-664) shipped in 3.0; older clusters have no clean abort path. KAFKA-18957 reports hanging-txn recovery still hard under EOS in KRaft 3.9.
- Manually deleting the active/newest `.log` per partition corrupts the partition — prefer lowering retention via `kafka-configs.sh`.
- Constant URP ≈ broker down; fluctuating URP ≈ performance problem — don't treat them the same.

### Sources

- Conduktor — *Kafka in Production: 11 Pitfalls You Should Avoid* — https://www.conduktor.io/blog/kafka-production-pitfalls
- Conduktor — *Disk Full: Emergency Recovery* — https://www.conduktor.io/blog/disk-full-emergency-recovery
- Conduktor — *JVM Tuning for Kafka Brokers: G1GC vs ZGC* — https://www.conduktor.io/blog/kafka-jvm-tuning-g1gc-vs-zgc-production
- Confluent — *Diagnose and Debug Apache Kafka Issues Pt.2* — https://www.confluent.io/blog/debug-apache-kafka-pt-2/
- Confluent — *Apache Kafka Supports 200K Partitions Per Cluster* — https://www.confluent.io/blog/apache-kafka-supports-200k-partitions-per-cluster/
- Datadog — *Lessons learned from running Kafka at Datadog* — https://www.datadoghq.com/blog/kafka-at-datadog/
- LinkedIn Engineering — *Running Kafka At Scale* — https://engineering.linkedin.com/kafka/running-kafka-scale
- LinkedIn Engineering — *How LinkedIn customizes Apache Kafka for 7 trillion messages per day* — https://www.linkedin.com/blog/engineering/open-source/apache-kafka-trillion-messages
- Cloudflare — *Using Apache Kafka to process 1 trillion inter-service messages* — https://blog.cloudflare.com/using-apache-kafka-to-process-1-trillion-messages/
- *KIP-664: Provide tooling to detect and abort hanging transactions* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-664:+Provide+tooling+to+detect+and+abort+hanging+transactions
- *KIP-112: Handle disk failure for JBOD* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-112:+Handle+disk+failure+for+JBOD
- *KIP-858: Handle JBOD broker disk failure in KRaft* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-858:+Handle+JBOD+broker+disk+failure+in+KRaft
- *KIP-928: Making Kafka resilient to log directories becoming full* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-928:+Making+Kafka+resilient+to+log+directories+becoming+full
- *KIP-16: Automated Replica Lag Tuning* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-16+-+Automated+Replica+Lag+Tuning
- *KIP-345: Introduce static membership protocol* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-345:+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
- *KIP-588: Allow producers to recover gracefully from transaction timeouts* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-588:+Allow+producers+to+recover+gracefully+from+transaction+timeouts
- Ahmed Abouzied — *Kafka Offline Partitions: 4 incident scenarios* — https://medium.com/@ahmedaabouzied/kafka-offline-partitions-4incident-scenarios-and-data-loss-possibilities-bb90577622df
- meshIQ — *Common Apache Kafka Performance Issues & How to Fix* — https://www.meshiq.com/blog/common-kafka-performance-issues-and-how-to-fix-them/
- Redpanda — *Kafka Rebalancing: Triggers, Effects, and Mitigation* — https://www.redpanda.com/guides/kafka-performance-kafka-rebalancing
- AWS re:Post — *Troubleshoot continuous rebalancing of your Amazon MSK consumer group* — https://repost.aws/knowledge-center/msk-consumer-group-rebalance
- AutoMQ — *Hot Partitions in Kafka* — https://www.automq.com/blog/hot-partitions-in-kafka-detection-mitigation-architecture-choices
- Instaclustr — *Network and Request Handler Capacity* — https://www.instaclustr.com/support/documentation/kafka/monitoring-information/network-and-request-handler-capacity/
- OneUptime — *How to Troubleshoot Kafka Under-Replicated Partitions* — https://oneuptime.com/blog/post/2026-01-21-kafka-under-replicated-partitions/view
- DEV Community — *How Kafka applies zombie fencing* — https://dev.to/oleg_potapov/how-kafka-applies-zombie-fencing-1o6e
- Instaclustr — *KRaft Part 3: Maximum Partitions* — https://www.instaclustr.com/blog/apache-kafka-kraft-abandons-the-zookeeper-part-3-maximum-partitions-and-conclusions/

---

## 5. Monitoring: golden signals, JMX, alert thresholds

### Synthesized findings

Kafka monitoring maps onto Google SRE's **four golden signals** (latency, traffic, errors, saturation), but the highest-value broker metrics are a small set of binary/near-binary health gauges plus a latency breakdown. The **page-immediately** tier: `ActiveControllerCount` (cluster sum must equal exactly 1), `OfflinePartitionsCount` (=0), `UnderReplicatedPartitions` (=0), `UnderMinIsrPartitionCount` (=0), `UncleanLeaderElectionsPerSec` (=0). The **investigate** tier covers saturation gauges (`RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent`) and the request-latency decomposition. Consumer lag should be alerted on **trend** (growing over a sustained window), not a single static threshold.

### Key facts (with sources)

- **Golden-signal mapping:** Traffic = `BytesIn/OutPerSec`, `MessagesInPerSec`; Latency = `RequestMetrics TotalTimeMs` (p99); Errors = `FailedProduce/FetchRequestsPerSec` + `UncleanLeaderElectionsPerSec`; Saturation = `RequestHandlerAvgIdlePercent`/`NetworkProcessorAvgIdlePercent` + disk/CPU/heap. — *Grafana; Datadog*
- **`ActiveControllerCount`** (`kafka.controller:type=KafkaController,name=ActiveControllerCount`): per-broker 0 or 1; alert if **SUM across cluster ≠ 1**. Sum=0 = no controller (critical); sum>1 = split-brain. Datadog: "alert on any other value that lasts longer than one second." — *Confluent; Datadog*
- **`OfflinePartitionsCount`** (`kafka.controller:type=KafkaController`): alert if **> 0**. Controller-level metric, read from the active controller. — *Confluent*
- **`UnderReplicatedPartitions`** (`kafka.server:type=ReplicaManager`): `|ISR| < |replicas|`; alert if **> 0**. Replicas being added via reassignment do NOT count. — *Confluent; AWS MSK*
- **`UnderMinIsrPartitionCount`** (`kafka.server:type=ReplicaManager`): partitions with in-sync count < `min.insync.replicas`; when >0 with `acks=all`, producers get `NotEnoughReplicas`. Alert **> 0**. — *Confluent; AWS MSK*
- **`UncleanLeaderElectionsPerSec`** (`kafka.controller:type=ControllerStats`): Confluent "should be 0"; Datadog "signals data loss." Grafana ships a critical alert. — *Confluent; Datadog; Grafana*
- **Request-latency decomposition** (`kafka.network:type=RequestMetrics,name=TotalTimeMs,request={Produce|FetchConsumer|FetchFollower}`): `= RequestQueueTimeMs + LocalTimeMs + RemoteTimeMs + ResponseQueueTimeMs + ResponseSendTimeMs (+ ThrottleTimeMs)`. — *Confluent; Datadog*
- **`RequestHandlerAvgIdlePercent`** (`kafka.server:type=KafkaRequestHandlerPool`): 0 = busy, 1 = idle. Instaclustr: "constantly below 0.2 (20%)" → cluster overloaded, add capacity. Rule of thumb: <20% potential / <10% active problem. — *Confluent; Instaclustr*
- **`NetworkProcessorAvgIdlePercent`** (`kafka.network:type=SocketServer`): Confluent "ideally greater than 0.4"; below ~0.3 → raise `num.network.threads`. — *Confluent*
- **Purgatory size** (`kafka.server:type=DelayedOperationPurgatory,delayedOperation={Produce|Fetch}`): produce purgatory "should be non-zero when `acks=all` is used." Use as a latency diagnostic, not an alert. — *Confluent; Datadog*
- **ISR shrink/expand** (`kafka.server:type=ReplicaManager,name=IsrShrinksPerSec`/`IsrExpandsPerSec`): expected value for both = 0 in steady state. Investigate any shrink WITHOUT a matching expand. Grafana ships warning alerts. — *Confluent; Datadog; Grafana*
- **Leader election** (`kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs`): rate + total time without a leader; "non-zero when there are broker failures." — *Confluent*
- **Consumer lag (client-side)** (`kafka.consumer:type=consumer-fetch-manager-metrics,...,name=records-lag-max`): max messages behind. Alert on growing **trend** over a sustained window. — *Datadog*
- **Consumer lag (external):** LinkedIn's **Burrow** classifies each consumer OK/WARNING/ERR by evaluating the lag **trend** over a sliding window (no per-topic threshold tuning). `kafka_exporter` is the common Prometheus alternative. — *LinkedIn Burrow*
- **Bytes throughput** (`kafka.server:type=BrokerTopicMetrics,name=BytesIn/OutPerSec`): primary capacity-planning input.
- **Log flush latency** (`kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs`): rising flush time correlates with high `LocalTimeMs` and falling `RequestHandlerAvgIdlePercent`.
- **Grafana Cloud Kafka integration:** ships 7 dashboards and 14 alerts, including `KafkaNoActiveController` (critical), `KafkaOfflinePartitonCount` (critical), `KafkaUnderReplicatedPartitionCount` (critical), `KafkaUncleanLeaderElection` (critical), `KafkaISRShrinkRate`/`ExpandRate` (warning), `KafkaLagIsTooHigh` (critical) + `KafkaLagKeepsIncreasing` (warning), `KafkaBrokerCount` (critical), `KafkaZookeeperSyncConnect` (critical).
- **AWS MSK production-ready CloudWatch thresholds:** `ActiveControllerCount ≠ 1`; `OfflinePartitionsCount > 0`; `UnderReplicatedPartitions > 0`; `UnderMinIsrPartitionCount > 0`; CPU (User+System) **> 60%** avg 5+ min; `HeapMemoryAfterGC > 60%`; `KafkaDataLogsDiskUsed ≥ 85%`; consumer lag > SLA threshold; `NetworkRx/TxErrors > 0`. AWS advises testing thresholds on one test cluster first.

### Formulas & heuristics

- **Controller health:** alert if `SUM(ActiveControllerCount) ≠ 1` (0 = no controller; >1 = split-brain). Always aggregate with SUM.
- **Unavailability/data-loss tier (alert if > 0):** `OfflinePartitionsCount`, `UnderReplicatedPartitions`, `UnderMinIsrPartitionCount`, `UncleanLeaderElectionsPerSec`.
- **Request-handler saturation:** `RequestHandlerAvgIdlePercent < 0.2` → add brokers; `< 0.1` → active problem.
- **Network saturation:** `NetworkProcessorAvgIdlePercent < 0.4` below ideal; `< 0.3` → raise `num.network.threads`.
- **Latency localization:** high `RemoteTimeMs` → slow follower replication (check `acks=all` / URP); high `LocalTimeMs` → disk/flush/GC; high `RequestQueueTimeMs` → handler starvation.
- **Consumer lag:** alert on **trend** (growing over 5+ min), not a static count. `recovery time ≈ current_lag / net_drain_rate`.
- **Paging gate:** require both impact AND a sustained window (commonly 5 min / 3 datapoints); use dynamic thresholds (p95 baseline, moving average) for noisy signals.
- **Disk safety:** `KafkaDataLogsDiskUsed ≥ 85%` → alert; keep CPU < 60% and `HeapMemoryAfterGC < 60%`.

### Case studies

- **LinkedIn:** 100+ clusters, 4,000+ brokers, 7M+ partitions, 7T+ msgs/day (2023); largest clusters >140 brokers / ~1M replicas. Hit slow-controller / controller-failure problems under memory pressure, motivating heavy controller-health monitoring; built **Burrow** for trend-based lag.
- **Cloudflare:** ~14 clusters / ~330 nodes / ~1T msgs. Operational pain = partition skew, where per-partition `BytesInPerSec` and leader-skew dashboards matter more than aggregate throughput; built lag-driven automatic restarts.
- **AWS MSK:** published a production-ready CloudWatch alarm set with concrete thresholds and explicitly recommends validating on a single test cluster first ("tune before rollout").

### Cautions

- `RequestHandlerAvgIdlePercent` is commonly MIS-REPORTED: historically mis-calculated (KAFKA-7295), reported as a rate by some integrations (Datadog integrations-core #516), anomalous in KRaft combined mode (KIP-1207). Confirm 0–1 gauge.
- `OfflinePartitionsCount` and `ActiveControllerCount` are CONTROLLER-level — only the active controller emits meaningful values; aggregate with SUM and alert on the cluster aggregate.
- `UnderReplicatedPartitions` excludes replicas being added during reassignment.
- ZooKeeper-specific metrics (`outstanding_requests`, `pending_syncs`, `KafkaZookeeperSyncConnect`) apply only to ZK-mode clusters; KRaft's controller quorum replaces them. Much published guidance (Datadog 2015-era) predates KRaft.
- Produce purgatory being non-zero is NORMAL under `acks=all` — don't alert on raw purgatory size.
- Idle-ratio thresholds are heuristics — Confluent gives NO official number for `RequestHandlerAvgIdlePercent`; the 0.2/20% figure is Instaclustr/community practice.
- Client-side `records-lag-max` only covers running consumers; a dead consumer reports nothing — external monitors (Burrow, `kafka_exporter`) catch stalled groups.
- Confluent's `monitoring.html` landing page is now only a TOC — definitions/thresholds live in `broker-metrics.html` and `log-network-metrics.html`.

### Sources

- Confluent Platform — *Broker and Controller Metrics* — https://docs.confluent.io/platform/current/kafka/broker-metrics.html
- Confluent Platform — *Log and Network Metrics* — https://docs.confluent.io/platform/current/kafka/log-network-metrics.html
- Confluent Platform — *Monitoring Kafka with JMX (index)* — https://docs.confluent.io/platform/current/kafka/monitoring.html
- Datadog — *Monitoring Kafka performance metrics (source)* — https://github.com/DataDog/the-monitor/blob/master/kafka/monitoring-kafka-performance-metrics.md
- Datadog — *Monitoring Kafka performance metrics (blog)* — https://www.datadoghq.com/blog/monitoring-kafka-performance-metrics/
- Grafana — *Explore Kafka metrics (learning path)* — https://grafana.com/docs/learning-paths/kafka-monitoring/explore-kafka-metrics/
- Grafana Cloud — *Kafka integration reference (dashboards & alerts)* — https://grafana.com/docs/grafana-cloud/monitor-infrastructure/integrations/integration-reference/integration-kafka/
- Instaclustr — *Network and Request Handler Capacity* — https://www.instaclustr.com/support/documentation/kafka/monitoring-information/network-and-request-handler-capacity/
- AWS Big Data Blog — *Set up production-ready monitoring for Amazon MSK using CloudWatch alarms* — https://aws.amazon.com/blogs/big-data/set-up-production-ready-monitoring-for-amazon-msk-using-cloudwatch-alarms
- LinkedIn Engineering — *How LinkedIn customizes Apache Kafka for 7 trillion messages per day* — https://www.linkedin.com/blog/engineering/open-source/apache-kafka-trillion-messages
- LinkedIn Burrow — *Kafka consumer lag checking* — https://github.com/linkedin/Burrow
- Cloudflare — *Using Apache Kafka to process 1 trillion inter-service messages* — https://blog.cloudflare.com/using-apache-kafka-to-process-1-trillion-messages/
- *KAFKA-7295 — Fix RequestHandlerAvgIdlePercent metric calculation* — https://issues.apache.org/jira/browse/KAFKA-7295
- *KIP-1207 — Fix anomaly of RequestHandlerAvgIdlePercent in KRaft combined mode* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-1207
- Factor House — *Kafka monitoring: a complete guide for platform engineers* — https://factorhouse.io/articles/kafka-monitoring

---

## 6. Cloud cost optimization

### Synthesized findings

In the cloud, Kafka's bill is dominated **not by compute** but by **cross-AZ network transfer and storage**. Confluent's teardown: a 100 MBps-ingress cluster splits roughly **$2.3k compute / $14.5k storage / $24.2k networking** per month, with networking "likely over 50%" of infra cost — rising to ~90% once tiered storage cuts storage. Cross-AZ is structurally large: with brokers spread over 3 AZs, ~⅔ of produce, ~⅔ of consumer-fetch, and ALL inter-broker replication crosses zone boundaries, charged both ways (~$0.02/GB effective on AWS). **Replication (RF=3 ⇒ ~2× ingress copied cross-AZ) is the single largest component.**

The lever ladder: (1) **compression** (lz4/zstd) cuts storage + replication + fetch bytes simultaneously, near-free; (2) **KIP-392 fetch-from-follower** eliminates consumer-side cross-AZ (~+500 ms tail) but leaves a produce+replication floor; (3) tune **RF and retention**; (4) **tiered storage (KIP-405)** cuts storage 30–90% but does NOT touch networking; (5) **diskless/object-store designs** (WarpStream, AutoMQ, KIP-1150) attack the networking floor directly (+200–400 ms latency).

### Key facts (with sources)

- **Cost split (Confluent):** 100 MBps self-managed ≈ **$2,281 compute + $14,515 storage + $24,192 networking**/mo; 20 MBps ≈ $1,037 + $2,903 + $4,838. Networking "likely over 50%"; after tiered storage "networking alone can comprise ~90%."
- **Storage formula (Confluent):** `storage = (MB ingress/sec) × 86,400 × (retention days) × 0.001 GB/MB × RF × ($/GB-month)`. EBS ≈ $0.08/GB-month.
- **Cross-AZ throughput formula (Confluent):** `cross-AZ GB/s = (ingress × 2/3) + (egress × 2/3) + (ingress × 2)`. The `ingress × 2` term is RF=3 replication and dominates.
- **Cross-AZ rate nuance:** AWS lists **$0.01/GB in EACH direction** → effective ~**$0.02/GB**. GCP ≈ $0.01/GB once at source; Azure inter-AZ historically free. State per-direction vs effective. — *2minutestreaming; AutoMQ; MSK*
- **Why cross-AZ is huge:** clients talk only to the partition LEADER (~⅔ probability in a different AZ across 3 AZs) plus mandatory cross-AZ replication. Confluent: "cross-AZ traffic costs can account for over 50% of a Kafka bill." — *AutoMQ; 2minutestreaming*
- **Cross-AZ worked example (AutoMQ):** 3-node, 100 MiB/s write, 3 consumer groups → ~$14k–$24k/mo. Producer writes ~173 TB/mo ≈ $3,460; replication ≈ $10,360; 3× consumer read ≈ $10,360. Even with fetch-from-follower, produce+replication floor ≈ **$13,800/mo**.
- **Second AutoMQ example:** 3× r6i.large at 30 MiB/s → cross-AZ = `30×60×60×24×30/1024×(2/3+2)×0.02` = **$4,050/mo** vs $272/mo VM (~15× the compute).
- **KIP-392 fetch-from-follower (Kafka 2.3+):** set `broker.rack`, `replica.selector.class=...RackAwareReplicaSelector`, `client.rack`. Can "basically eliminate ALL consumer networking costs." Does NOT reduce produce or replication. — *2minutestreaming*
- **KIP-392 latency tradeoff:** followers serve only up to the high-watermark → Grab measured "up to 500 ms" added latency; also causes broker load skew.
- **Grab case study (InfoQ 2023):** cross-AZ was "half the cost of their Kafka platform"; rack-aware fetch-from-follower drove reconfigured-consumer cross-AZ cost to **zero**. Tradeoff: +500 ms latency, broker skew.
- **Tiered storage (KIP-405):** GA in **Kafka 3.6.0**; also MSK/Confluent. Confluent: "can decrease storage costs by over 90%"; commonly 30–40% in moderate cases. **Does NOT cut cross-AZ networking** — Aiven: "With Tiered Storage, Networking is 83%+ of cost ($882k/yr out of $1.05M/yr)."
- **Tiered storage economics:** S3 ≈ $0.02/GiB-mo vs EBS gp2/gp3 ≈ $0.08–0.10/GiB-mo (~4–5× cheaper); WarpStream cites up to 24× vs local NVMe ($0.02 vs up to $0.48/GiB). Decouples storage from compute. — *Uber; WarpStream*
- **Diskless/object-store designs:** write batches directly to S3 (leaderless, no inter-broker replication) → cross-AZ replication → ~0. **KIP-1150 "Diskless Topics"** (Aiven, Apr 2025) was **ACCEPTED ~March 2, 2026** (acceptance ≠ production-ready). WarpStream/AutoMQ are commercial implementations.
- **Diskless cost claims:** Aiven — storage ~20× less, cross-zone ~$0; up to ~80–90% TCO cut; a 1 GiB/s, 3×-fanout, tiered-storage AWS deployment costs >$3.4M/yr as baseline. 2minutestreaming: ~11.2× lower than Confluent Freight ($100k vs ~$2M/yr); a 500 MB/s 7-day cluster ~$882k→~$200k/yr (~5×).
- **WarpStream benchmark (own blog, S3EOZ TCO):** 3-AZ OSS Kafka **$20,252/mo** (inter-zone networking alone $14,765) vs WarpStream **$2,961/mo**; even vs optimized single-zone Kafka + fetch-from-follower ($8,223/mo), WarpStream is $2,961/mo with multi-AZ durability. Workload: 268 MiB/s on 5× m7g.xl across 3 AZs.
- **Diskless latency tradeoff:** commit interval (~250 ms default, or 8 MiB batch) + S3 PUT (~200–400 ms p99 for 2–8 MB) → ~200–400 ms typical, up to ~2.4 s e2e. WarpStream w/ S3 Express One Zone cuts produce p99 to ~169 ms (median ~105 ms), ~3× faster than S3 standard. KIP-1150 supports BOTH classic sub-100 ms and diskless 200–400 ms topics in one cluster.
- **Compression as a free multiplier:** Kafka replicates and stores batches COMPRESSED. lz4 recommended for performance; zstd ~gzip ratio at less CPU; gzip not recommended. lz4 ~40% of original at ~594 MB/s; zstd-1 ~24% (~12× on text); lz4 can be faster end-to-end than no compression.
- **RF & retention levers:** RF multiplies BOTH storage and cross-AZ replication; dropping RF 3→2 on non-critical topics cuts both; shorter retention cuts storage linearly. Simplest dials before new architectures.
- **KIP-1150 design factions (Vanlightly):** Revolutionary (Rev1 leaderless + Batch Coordinator; Aiven Inkless uses PostgreSQL as sequencer) vs Evolutionary (KIP-1176/Slack keeps leaders, per-broker S3 WAL, acks after WAL write → lower produce latency). New bottleneck shifts to the metadata/sequencing service; Vanlightly flags PostgreSQL row-lock "convoy effect" risk.

### Formulas & heuristics

- `Storage cost/mo = (MB ingress/sec) × 86,400 × (retention days) × 0.001 GB/MB × RF × ($/GB-month, EBS ≈ $0.08)`.
- `Cross-AZ throughput (3 AZ) = (ingress × 2/3) + (egress × 2/3) + (ingress × [RF-1])`; for RF=3 the replication term = `ingress × 2` and dominates. Multiply GB by ~$0.02/GB.
- **Quick cross-AZ monthly cost (AWS, RF=3):** `GB/mo = ingress_MiB/s × 60 × 60 × 24 × 30 / 1024`; `cost ≈ GB/mo × (2/3 + (RF-1) + fanout × 2/3) × $0.02`.
- **Rule of thumb:** networking ≥ 50% of infra cost; after tiered storage 80–90%+. Compute is usually smallest.
- **Lever ordering by effort/impact:** (1) compression — cuts storage+replication+fetch, near-free; (2) fetch-from-follower — kills consumer cross-AZ, +~500 ms tail; (3) tune RF/retention — linear savings; (4) tiered storage — cuts storage 30–90%, no network effect; (5) diskless/object-store — kills replication+produce cross-AZ, +200–400 ms latency.
- **Replication amplification:** each 1 GB produced → `(RF-1)` GB cross-AZ replication; RF=3 → 2 GB cross-AZ per 1 GB written.
- **Object storage as durability+balancing:** S3 ≈ $0.02/GiB-mo replaces RF=3 EBS (~$0.24–0.30/GiB-mo effective) AND eliminates cross-AZ replication.

### Case studies

- **Grab (InfoQ 2023):** cross-AZ ~50% of total cost; rack-aware fetch-from-follower → reconfigured-consumer cross-AZ → zero. Side effects: +500 ms latency, broker skew, maintenance complexity.
- **Uber:** built/adopted tiered storage (KIP-405) to fix storage-compute coupling; local retention days→hours, remote days/months; in production ~1–2 years.
- **WarpStream (S3EOZ TCO):** 3-AZ OSS Kafka $20,252/mo (inter-zone $14,765) vs WarpStream $2,961/mo; vs optimized single-zone + FFF $8,223/mo. 268 MiB/s on 5× m7g.xl; S3EOZ produce p99 ~169 ms.
- **Aiven Diskless / KIP-1150:** baseline 1 GiB/s 3×-fanout tiered-storage >$3.4M/yr; diskless claims storage ~20× cheaper, cross-zone ~$0; ~11.2× lower than Confluent Freight; KIP-1150 accepted ~March 2026.
- **AutoMQ:** models 3× r6i.large 30 MiB/s at $4,050/mo cross-AZ vs $272/mo VM, and 100 MiB/s 3-consumer-group at $14k–$24k/mo. S3-WAL design (buffer → write to S3 at 8 MB or 250 ms, no inter-broker replication) claims ~100% cross-zone elimination while staying protocol-compatible.

### Cautions

- Cross-AZ rate is frequently misstated: AWS = $0.01/GB **each direction** → effective ~$0.02/GB. Some blogs (incl. Confluent) quote "$0.02/GB" as the rate. GCP (~$0.01/GB once) and Azure (historically free) differ — cross-AZ optimization ROI is AWS/GCP-centric, near-irrelevant on Azure.
- **Tiered storage (KIP-405) does NOT reduce cross-AZ networking** — a very common misconception. Fetch-from-follower / diskless are the networking levers.
- Fetch-from-follower (KIP-392) only addresses CONSUMER cross-AZ; produce and replication remain, leaving a hard floor (~$13.8k/mo of the $24k AutoMQ example). Adds ~up to 500 ms latency and broker skew.
- Vendor cost figures (WarpStream 80–85%, Aiven 80%, AutoMQ ~100%) use favorable assumptions (high fanout, retail pricing, RF=3) — directional, not audited; the mechanism (skip inter-broker cross-AZ replication via object storage) is real, the % is workload-dependent.
- KIP-1150 status mid-2026: ACCEPTED (~March 2026) but acceptance ≠ production-ready OSS implementation. Earlier 2025 sources call it "in DISCUSS." Design fragmented into Rev1/Rev2/Rev3 + competing KIP-1176.
- Diskless trades latency for cost (~200–400 ms typical, up to ~2.4 s e2e) — NOT a drop-in for sub-100 ms; KIP-1150 coexists with classic topics, not replaces them.
- Compression savings are data-dependent (~10–12× for text/JSON; little for binary/pre-compressed). Larger batches improve the ratio.
- The Confluent "Part 2 / density" blog URL returned 404 in research — density/instance-type guidance unverified; confirm Graviton/storage-optimized recommendations against a live source.

### Sources

- Confluent — *Uncovering Kafka's Hidden Infrastructure Costs (Part 1)* — https://www.confluent.io/blog/understanding-and-optimizing-your-kafka-costs-part-1-infrastructure/
- AutoMQ — *The Hidden Cloud Cost You Never Noticed in Your Kafka Bill* — https://www.automq.com/blog/kafka-cross-az-hidden-cost
- 2-Minute Streaming — *Kafka KIP-392: Fetch From Follower* — https://blog.2minutestreaming.com/p/kafka-kip-392-follower-fetching
- 2-Minute Streaming — *How KIP-1150 Diskless Topics makes Kafka stateless* — https://blog.2minutestreaming.com/p/diskless-kafka-topics-kip-1150
- Aiven — *Diskless Apache Kafka: 80% Leaner, 100% Open* — https://aiven.io/blog/diskless-apache-kafka-kip-1150
- WarpStream — *S3 Express One Zone Benchmark and Total Cost of Ownership* — https://www.warpstream.com/blog/warpstream-s3-express-one-zone-benchmark-and-total-cost-of-ownership
- WarpStream — *Pricing* — https://www.warpstream.com/pricing
- Jack Vanlightly — *A Fork in the Road: Deciding Kafka's Diskless Future* — https://jack-vanlightly.com/blog/2025/10/22/a-fork-in-the-road-deciding-kafkas-diskless-future
- Uber Engineering — *Kafka Tiered Storage* — https://www.uber.com/blog/kafka-tiered-storage/
- InfoQ — *How Grab Reduced Apache Kafka AWS Costs* — https://www.infoq.com/news/2023/07/grab-apache-kafka-aws-cost/
- Confluent — *Apache Kafka Message Compression* — https://www.confluent.io/blog/apache-kafka-message-compression/
- *KIP-405: Kafka Tiered Storage* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-405:+Kafka+Tiered+Storage
- *KIP-1150: Diskless Topics* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics
- AutoMQ via vutr Substack — *How AutoMQ Reduces Nearly 100% of Kafka Cross-Zone Data Transfer Cost* — https://vutr.substack.com/p/how-automq-reduces-nearly-100-of
- Trendyol Tech — *Optimizing Kafka Performance Through Data Compression* — https://medium.com/trendyol-tech/optimizing-kafka-performance-through-data-compression-330fb31a0827

---

## 7. Real-world high-throughput deployments & what breaks at scale

### Synthesized findings

The largest public Kafka fleets operate at **trillions of messages/day** and tens of millions of messages/second. LinkedIn (the creator) ran ~7T msgs/day across 100+ clusters / 4,000+ brokers (2019), reaching **32T records/day, 17 PB/day, 400K topics, 150 clusters** by 2025 — at which point it built a Kafka replacement (Northguard). Uber, Netflix Keystone (~2T+ events/day), Cloudflare (1T+/day), Pinterest (800B/day), and Datadog (millions of partitions, TB/s) sit in the same tier.

The **recurring bottlenecks** are remarkably consistent: (1) the single-controller/metadata limit (~200K partitions/cluster on ZK); (2) replication network saturation / re-replication storms on broker failure; (3) page-cache eviction / the "catch-up tax" (historical reads spike p99 produce latency ~2 ms → ~250 ms); (4) cross-AZ network cost (>50% of a cloud bill at RF=3); and (5) multi-cluster federation, where vanilla MirrorMaker's rebalancing caused 5–10 min stalls. **KRaft (KIP-500)** removes the metadata bottleneck (2M+ partitions, near-instant failover) and **tiered storage (KIP-405)** decouples retention from disk.

### Key facts (with sources)

- **LinkedIn (2019):** >7 trillion msgs/day across 100+ clusters / 4,000+ brokers / 100,000+ topics / 7M partitions; peak ~13M msgs/s and 2.75 GB/s; ~2 PB/week. Individual clusters 140+ brokers, up to 1M replicas.
- **LinkedIn (2025, Northguard):** 32 trillion records/day, 17 PB/day, 400,000 topics, 150 clusters. Built **Northguard** (sharded Raft state machines / vnodes) + **Xinfra** migration layer (90%+ of apps migrated) because three Kafka limits became hard blockers: single-controller metadata, multi-TB partition rebalance skew, partition-based scaling. — *InfoQ / SiliconANGLE, June 2025*
- **Pinterest:** 2,000+ brokers, 800B msgs/day, 1.2 PB/day, 15M msgs/s peak; max 200 brokers/cluster to bound blast radius; RF=3 across 3 AZs; default `d2.2xlarge`. DoctorKafka cut Kafka alerts >95%. Enabling TLS raised per-SSL-channel memory to ~122 KB and forced heap 4 GB → 8 GB.
- **Netflix Keystone (2016):** 36 clusters / 4,000+ broker instances; <200 brokers and <10,000 partitions per cluster; 700B+ msgs/day; producers tuned for availability (`acks=1`, `unclean.leader.election.enable=true`) accepting <0.01% daily loss because lossless was "cost prohibitive in AWS EC2." Failover to standby <5 min. Later: ~2T+ events/day, ~3 PB in / 7 PB out, ~100 clusters, 20,000+ Flink jobs.
- **Cloudflare:** 1T+ inter-service msgs across 14 clusters / ~330 nodes; RF≥3; Protobuf (one type/topic). HTTP analytics pipeline peaked at 100 Gbps / 7.5M req/s. Documented failure: partition skew from a Messagebus-Client config change drove most traffic to one partition.
- **Datadog:** 40+ clusters (2018) → hundreds of clusters, millions of partitions, TB/s, petabytes of NVMe even for short retention (Feb 2025). Built a custom Rust client, a Streaming Platform control plane, and kafka-kit (topicmappr + autothrottle).
- **Uber:** "one of the largest Kafka deployments"; throughput grew ~1M → ~12M msgs/s over 5 years; federated into ~150-node clusters; two-tier regional + aggregate clusters; supports active/active and active/passive.
- **ZooKeeper-era limits:** ~4,000/broker and ~200,000/cluster; the cap bounds controller hard-failure recovery. Apache's 100,000-partition test: controller state reload **28 s (1.0.0) → 14 s (1.1.0)**; older ZK controllers could exceed 20 min at large scale.
- **KRaft (KIP-500):** Confluent lab at **2 million partitions** (10× ZK max). Controlled shutdown ~120 s (ZK) → ~20–30 s (KRaft, ~6× faster); uncontrolled recovery near-instant vs minutes. ~100× partition scaling.
- **Cross-AZ networking:** AWS $0.01/GB both directions (effective $0.02/GB); GCP $0.01/GB. RF=3 → ~2 GB cross-AZ per 1 GB written. Confluent: cross-AZ can exceed 50% of a bill. AutoMQ (100 MiB/s, 3 AZs, 3 groups): ~$24,000/mo unoptimized, ~$14,000/mo even with fetch-from-follower.
- **Fetch-from-follower (KIP-392, Kafka 2.4):** cuts cross-AZ consumer traffic by up to two-thirds; overall cross-AZ by ~60–80% (Conduktor); cannot remove producer-write or replication traffic.
- **Page-cache "catch-up tax":** historical reads evict hot tail data, spiking p99 produce ~2 ms → ~250 ms, disk I/O to 100%. KIP-405 tests: historical consumers caused ~43% producer-throughput drop without tiered storage; tiered storage improved p99 produce ~30%. Pinterest offloads ~200 TB/day to object store; MemQ up to 90% more cost-efficient for some workloads.
- **Replication network = dominant recovery bottleneck:** RF=3 broker death → re-replicating multi-TB partitions saturates NICs. Netflix documented the cascade (slow outlier → replication lag → leaders read from disk → buffer exhaustion → message drops). Datadog autothrottle paces replication; Pinterest enforces single-broker-per-period replacement.
- **Multi-cluster federation:** vanilla MirrorMaker (high-level consumer) triggered cluster-wide rebalances on any broker hiccup → 5–10 min of replication inactivity, and after 32 failed rebalances permanently stuck consumers — "an outage almost every week" at Uber. uReplicator replaced it (Apache Helix + DynamicKafkaConsumer).

### Formulas & heuristics

- **Partition sizing (ZK-era):** ≤4,000/broker and ≤200,000/cluster; conservative baseline ~100–200/broker. KRaft raises to millions, but per-broker limits (FDs, fetcher threads, leader-election work) still apply.
- **Per-partition throughput:** older ~10–50 MB/s; modern Kafka 3.0+ can do 50–100+ MB/s with tuning. Size from `target throughput / per-partition throughput`.
- **Cross-AZ replication:** `(RF-1) × ingress` cross-AZ per GB under naive placement; RF=3 → ~2 GB. Add producer (~⅔ cross-AZ) and consumer reads (~⅔ × #groups, before FFF).
- **Cap cluster size to bound blast radius:** Pinterest/Netflix ~200 brokers/cluster; Uber ~150-node clusters. Prefer many bounded clusters + an aggregate/federation tier over one giant cluster.
- **RF 3 across 3 AZs** tolerates 2 broker (or 1 AZ) failures; enforce min RF at topic creation; replace ≤1 broker per window.
- **Controller failover on ZK scales ~linearly with partition count**; budget partition count against acceptable failover unavailability, or move to KRaft.
- **Local + aggregate topology** for multi-DC: produce locally, mirror forward, so each message crosses the WAN minimally (LinkedIn/Netflix pattern).

### Case studies

- **LinkedIn — outgrew Kafka:** at 32T records/day / 17 PB/day, single-controller metadata, multi-TB rebalance skew (some brokers 100% CPU while others idle), and partition-based scaling became hard blockers → built Northguard + Xinfra (90%+ apps migrated).
- **Pinterest:** broker failures "almost every day" at 2,000+ brokers required manual intervention until DoctorKafka (>95% alert reduction). Jan 2018 incident: partition reassignment alone insufficient when machines degrade → rate-limited single-broker replacement. TLS leaked unclosed SSL channels (~122 KB each), forcing heap 4 → 8 GB.
- **Netflix Keystone:** a ZooKeeper incident caused producers to drop a significant volume of messages with "little we could do"; cascading replication slowdowns → capped clusters at <200 brokers and accepted <0.01% daily loss.
- **Uber:** MirrorMaker rebalance storms (5–10 min stalls, consumers stuck after 32 rebalances, ~weekly outages) → built uReplicator. Cross-region offset divergence required a dedicated offset-management service.
- **Cloudflare:** a small Messagebus-Client config change caused severe partition skew across a 14-cluster / ~330-node fleet; consumer-lag alerting was the primary early warning.
- **Datadog:** off-the-shelf clients hit limits across 40+ (later hundreds of) clusters; built kafka-kit autothrottle + topicmappr, then a Streaming Platform control plane to decouple clients from topology.

### Cautions

- The ~200,000/cluster / ~4,000/broker figures are ZK-era; KRaft (GA 3.3+, ZK removed in 4.0) runs 2M+ with near-instant failover. Always state the control plane.
- "Controller failover takes 20+ minutes" is also a ZK-at-extreme-scale figure; cite the 2M-partition lab numbers (~120 s → ~20–30 s controlled shutdown).
- Headline org numbers are point-in-time. LinkedIn is variously 800B → 1.1T → 1.4T → 7T → 32T/day (2015→2025) — pin the year. Netflix "4,000 brokers / 36 clusters / 700B/day" is 2016; "2T+ events/day" is later.
- Per-partition throughput is version/hardware-dependent — don't quote a single universal number.
- Cross-AZ cost % depends on RF, AZ count, group count, co-location. Fetch-from-follower reduces consumer-side only; a hard floor remains.
- Cloud transfer prices change; re-check $0.01/GB cross-AZ before publication.
- uReplicator's critique applies to legacy MirrorMaker (MM1); **MirrorMaker 2 (KIP-382, Connect-based)** addresses much of it — don't present MM1's flaws as current behavior.
- Tiered-storage benefit figures (~43% drop avoided, ~30% p99, "90% cheaper" MemQ) are workload-specific/vendor-reported. KIP-405 GA only from 3.6+.

### Sources

- LinkedIn Engineering — *How LinkedIn customizes Apache Kafka for 7 trillion messages per day* — https://www.linkedin.com/blog/engineering/open-source/apache-kafka-trillion-messages
- LinkedIn Engineering — *Running Kafka At Scale* — https://engineering.linkedin.com/kafka/running-kafka-scale
- InfoQ — *LinkedIn Announces Northguard and Xinfra (June 2025)* — https://www.infoq.com/news/2025/06/linkedin-northguard-xinfra/
- SiliconANGLE — *LinkedIn introduces Northguard and Xinfra to replace Kafka* — https://siliconangle.com/2025/06/25/linkedin-introduces-northguard-xinfra-replace-kafka-scalable-log-storage/
- Pinterest Engineering — *How Pinterest runs Kafka at scale* — https://medium.com/pinterest-engineering/how-pinterest-runs-kafka-at-scale-ff9c6f735be
- Pinterest Engineering — *Pinterest Tiered Storage for Apache Kafka* — https://medium.com/pinterest-engineering/pinterest-tiered-storage-for-apache-kafka-%EF%B8%8F-a-broker-decoupled-approach-c33c69e9958b
- Pinterest Engineering — *MemQ: An efficient, scalable cloud native PubSub system* — https://medium.com/pinterest-engineering/memq-an-efficient-scalable-cloud-native-pubsub-system-4402695dd4e7
- Netflix TechBlog — *Kafka Inside Keystone Pipeline* — https://medium.com/netflix-techblog/kafka-inside-keystone-pipeline-dd5aeabaf6bb
- Cloudflare — *Using Apache Kafka to process 1 trillion inter-service messages* — https://blog.cloudflare.com/using-apache-kafka-to-process-1-trillion-messages/
- Datadog — *Lessons learned from running Kafka at Datadog* — https://www.datadoghq.com/blog/kafka-at-datadog/
- Datadog Engineering — *Introducing Kafka-Kit* — https://www.datadoghq.com/blog/engineering/introducing-kafka-kit-tools-for-scaling-kafka/
- Datadog Engineering — *Achieving relentless Kafka reliability at scale with the Streaming Platform* — https://www.datadoghq.com/blog/engineering/streaming-platform-kafka-custom-abstractions/
- Uber Blog — *Disaster Recovery for Multi-Region Kafka at Uber* — https://www.uber.com/blog/kafka/
- Uber Blog — *uReplicator: Uber Engineering's Robust Apache Kafka Replicator* — https://www.uber.com/blog/ureplicator-apache-kafka-replicator/
- Confluent — *Apache Kafka Supports 200K Partitions Per Cluster* — https://www.confluent.io/blog/apache-kafka-supports-200k-partitions-per-cluster/
- Apache Kafka blog archive — *Apache Kafka Supports More Partitions* — https://blogsarchive.apache.org/kafka/entry/apache-kafka-supports-more-partitions
- Confluent — *Kafka Needs No Keeper* — https://www.confluent.io/blog/removing-zookeeper-dependency-in-kafka/
- AutoMQ — *The Hidden Cloud Cost You Never Noticed in Your Kafka Bill* — https://www.automq.com/blog/kafka-cross-az-hidden-cost
- AWS Big Data Blog — *Reduce network traffic costs of your Amazon MSK consumers with rack awareness* — https://aws.amazon.com/blogs/big-data/reduce-network-traffic-costs-of-your-amazon-msk-consumers-with-rack-awareness/
- azguards — *The Catch-Up Tax: Preventing Page Cache Eviction during Kafka Historical Reads* — https://azguards.com/lowlatency/the-catch-up-tax-preventing-page-cache-eviction-during-kafka-historical-reads/
- Aiven — *16 Ways Tiered Storage Improves Apache Kafka* — https://aiven.io/blog/16-ways-tiered-storage-makes-kafka-better
- Confluent / LinkedIn — *Apache Kafka Hits 1.1 Trillion Messages Per Day* — https://www.confluent.io/blog/apache-kafka-hits-1-1-trillion-messages-per-day-joins-the-4-comma-club/

---

## 8. The distributed commit log as an architectural pattern

### Synthesized findings

The distributed commit log reframes the database **"inside out"**: instead of treating mutable state as primary and the redo log as a hidden detail, the log of immutable, totally-ordered events becomes the **system of record**, and all queryable state (indexes, caches, materialized views) becomes a **derived, replayable projection**. Jay Kreps' 2013 essay "The Log" is canonical: a log is "an append-only, totally-ordered sequence of records ordered by time," and the **State Machine Replication Principle** reduces distributed consistency to building one consistent log. Kleppmann's "Turning the Database Inside Out" (2014) unbundles a database into four functions (replication, secondary indexing, caching, materialized views), each a deterministic derivation from a log that "squeezes the non-determinism of concurrency out of the stream of writes."

**Event sourcing** stores state as the event sequence and rebuilds by replay; **CQRS** separates write and read models (the event store = write model, any number of read models = projections). **Stream-table duality** (Matthias Sax) formalizes that a table is the aggregate of a changelog stream and a stream is the changelog of a table — the basis of KStream/KTable and CDC. **CDC + the outbox pattern** solve the dual-write problem by writing business data + an outbox row in one local ACID transaction, then tailing the DB transaction log to Kafka (at-least-once, eventually consistent, no unsafe 2PC). **Log compaction** turns a topic into a table by retaining the last value per key. The pattern is **wrong** — a recognized anti-pattern — as a general-purpose database, as synchronous RPC/request-reply, or as a low-latency per-message task queue (per-partition ordering causes head-of-line blocking).

### Key facts (with sources)

- **Kreps, "The Log" (2013):** "A log is perhaps the simplest possible storage abstraction. It is an append-only, totally-ordered sequence of records ordered by time."
- **State Machine Replication Principle (verbatim):** "If two identical, deterministic processes begin in the same state and get the same inputs in the same order, they will produce the same output and end in the same state." → reduces consistency to "implementing a distributed consistent log."
- **Physical vs logical logging (Kreps):** physical = logging changed-row contents; logical = logging the SQL commands. The log originated in DBs (IBM System R) as the WAL/redo log.
- **Three roles of the log (Kreps):** (1) Data Integration; (2) Real-time data processing; (3) Distributed system design. Prefers "log" over "pub/sub" — "more specific about semantics."
- **Scale (Kreps 2013):** LinkedIn was running "over 60 billion unique message writes through Kafka per day." Partitioning lets "log appends occur without co-ordination between shards" so "throughput scales linearly."
- **Kleppmann (2014):** take the DB replication stream and "make it a first-class citizen"; unbundle into replication, secondary indexing, caching, materialized views. A secondary index "doesn't add any new information"; a materialized view is the ideal "cache that magically keeps itself up-to-date." Application-managed caches are "a complete mess."
- **Kleppmann on consistency:** "the log defines the order in which writes are applied … The log squeezes the non-determinism of concurrency out of the stream of writes." Writes become "super fast and scalable" (append-only).
- **Event sourcing (Fowler 2005):** store state as a chronological sequence of immutable events; current state = replay. Kleppmann (2015): record every write "as an immutable event"; events "are immutable facts … the source of truth."
- **CQRS:** separate write/read models; orthogonal to events but composes well — event store = write model, read models = projections, independently scalable and rebuildable.
- **Stream-table duality (Sax, BIRTE @ VLDB 2018):** "a stream can be viewed as a table, and a table can be viewed as a stream." Table = aggregation of a changelog; stream = changelog of a table. KStream/KTable shipped in Kafka 0.10.
- **Outbox / dual-write (Morling, Debezium, Feb 2019):** a dual write = changing two systems without a shared transaction → inconsistency on partial failure. Fix: write business entity + outbox row in one local ACID transaction; Debezium tails the DB log and publishes to Kafka.
- **CDC mechanics (Debezium):** hooks the DB transaction log (Postgres WAL, MySQL binlog) directly — "zero polling overhead." Yields at-least-once + eventual consistency (NOT cross-system 2PC/XA, deliberately avoided).
- **Log compaction (Confluent):** "retains at least the last known value for each message key." Guarantees: offsets never change, compaction never reorders (only removes), a caught-up consumer sees every message, a consumer from offset 0 "will see at least the final state of all records in the order they were written."
- **Compaction knobs:** `cleanup.policy=compact`; `min.cleanable.dirty.ratio`; `min.compaction.lag.ms`/`max.compaction.lag.ms`; tombstone = key + null value, purged after `delete.retention.ms` (commonly 24h). Does NOT guarantee a single record per key at any instant.
- **Kafka-as-storage (Kreps, 2017):** valid long-term uses — event sourcing change logs, in-memory cache feeds, stream-processing replay, CDC distribution. "Data in Kafka is persisted to disk, checksummed, and replicated … Accumulating more stored data doesn't make it slower."
- **Anti-pattern — Kafka as a database (Kreps):** "I don't think Kafka really benefits from trying to add any kind of random access lookups directly against the log." Kafka replicates INTO specialized systems, not replaces them.
- **Anti-pattern — Kafka as a database (Waehner, 2020):** "Kafka will not replace other databases. It is complementary." Lacks efficient key lookups, complex ad-hoc SQL, multi-key ACID — materialize views into purpose-built stores.
- **Anti-pattern — Kafka as RPC:** designed as an event log "optimizing for throughput over latency"; no native point-to-point or on-the-fly reply topics. Request-reply over Kafka "returns the coupling we were trying to avoid."
- **Anti-pattern — Kafka as a task queue:** per-partition order + one consumer per partition per group → "partition throughput collapses to the speed of the slowest message" (head-of-line blocking; a poison pill stalls every later message). Parallelism capped at partition count. Mitigations: more partitions, or the Confluent Parallel Consumer.
- **Kappa (Kreps 2014):** Lambda runs the same logic twice; Kappa reprocesses by replaying a retained log (start a second job from the start, write to a new output, swap, delete old).
- **Latency context (Confluent OMB, i3en.2xlarge, 1KB msgs):** peak Kafka 605 MB/s vs Pulsar 305 vs RabbitMQ ~38; at ~200K msg/s p99 e2e ~5 ms (Kafka) vs ~25 ms (Pulsar); RabbitMQ ~1 ms at low load but degrades >30 MB/s. *Vendor benchmark — directional.*

### Formulas & heuristics

- **State reconstruction:** `current_state = fold over the ordered event log from offset 0`; same log + same logic → same state and views.
- **Stream-table duality:** `KTable = aggregate(changelog stream)`; `stream = changelog(KTable)`. CDC is the DB-side case.
- **Pattern fit:** use the log for ordered durable history + decoupled fan-out + replay/CDC; avoid Kafka as primary store for point-lookup-by-key / ad-hoc query / synchronous request-reply.
- **Parallelism ceiling:** `max consumers in a group = partition count` — provision for peak fan-out.
- **Outbox safety rule:** atomically write `{business row + outbox row}` in one local transaction, propagate via CDC; accept at-least-once + eventual consistency; make consumers idempotent; never DB+broker 2PC.
- **Compacted-topic sizing:** `steady-state size ≈ (distinct live keys) × (record size)`, independent of total event throughput.

### Cautions

- **Commonly misstated:** "event sourcing == CQRS == Kafka." Distinct: event sourcing is a storage technique, CQRS is read/write model separation (orthogonal, usable without events), Kafka is a log implementation. Fowler warns CQRS adds risk — use narrowly.
- **Commonly misstated:** that Kleppmann's 2015 essay gives the sharp "command (may be rejected) vs event (immutable fact)" distinction — in that text he uses them largely interchangeably. The crisp distinction comes from DDD/CQRS literature.
- **Version-dependent:** log compaction does NOT guarantee exactly one record per key at any moment — multiple values/tombstones can coexist; timing is non-deterministic. Only "at least the last value per key" holds.
- **Commonly misstated:** that outbox/CDC gives exactly-once or distributed-transaction semantics across DB and Kafka. It deliberately avoids 2PC/XA → at-least-once + eventual consistency; consumers must be idempotent. Kafka EOS applies within Kafka read-process-write, not across an external DB.
- **Vendor bias:** "Kafka 15× RabbitMQ / 2× Pulsar" (605/305/38; p99 5 ms vs 25 ms) is Confluent's OMB and was contested by Pulsar/StreamNative — directional, instance/config-specific.
- **Version note:** tiered storage / "infinite retention" (underpinning "Kafka as long-term store") are recent — KIP-405 GA in 3.6 (2023); Confluent Cloud Infinite Retention 2020. Pre-3.6 self-managed stored everything on broker disk.
- **Commonly misstated scale:** "7 trillion msgs/day" is the ~2019 LinkedIn figure, NOT from Kreps' 2013 "The Log" (which cites 60 billion writes/day). Don't attribute trillion-scale to the original essay.
- **Kafka does NOT push to consumers** — it's pull/poll-based, part of why per-message latency exceeds push brokers at low load. A deliberate throughput-over-latency choice.

### Sources

- Jay Kreps — *The Log: What every software engineer should know about real-time data's unifying abstraction* — https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
- Martin Kleppmann — *Turning the database inside-out with Apache Samza* — https://martin.kleppmann.com/2015/03/04/turning-the-database-inside-out.html
- Martin Kleppmann — *Turning the database inside out (Strange Loop 2014)* — https://martin.kleppmann.com/2014/09/18/turning-database-inside-out-at-strange-loop.html
- Martin Kleppmann — *Stream processing, Event sourcing, Reactive, CEP…* — https://martin.kleppmann.com/2015/01/29/stream-processing-event-sourcing-reactive-cep.html
- Matthias J. Sax — *Streams and Tables: Two Sides of the Same Coin (blog)* — https://www.confluent.io/blog/streams-tables-two-sides-same-coin/
- Sax & Wang — *Streams and Tables: Two Sides of the Same Coin (BIRTE @ VLDB 2018, PDF)* — https://www.confluent.io/wp-content/uploads/streams-tables-two-sides-same-coin.pdf
- Gunnar Morling — *Reliable Microservices Data Exchange With the Outbox Pattern* — https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/
- Confluent Documentation — *Kafka Log Compaction* — https://docs.confluent.io/kafka/design/log_compaction.html
- Jay Kreps — *It's Okay To Store Data In Apache Kafka* — https://www.confluent.io/blog/okay-store-data-apache-kafka/
- Jay Kreps — *Questioning the Lambda Architecture* — https://www.oreilly.com/radar/questioning-the-lambda-architecture/
- Kai Waehner — *Can Apache Kafka Replace a Database?* — https://www.kai-waehner.de/blog/2020/03/12/can-apache-kafka-replace-database-acid-storage-transactions-sql-nosql-data-lake/
- Kai Waehner — *When to use Request-Response with Apache Kafka?* — https://www.kai-waehner.de/blog/2022/06/03/apache-kafka-request-response-vs-cqrs-event-sourcing/
- Martin Fowler — *What do you mean by "Event-Driven"?* — https://martinfowler.com/articles/201701-event-driven.html
- Confluent — *Benchmarking Kafka vs Pulsar vs RabbitMQ (OpenMessaging)* — https://www.confluent.io/blog/kafka-fastest-messaging-system/
- LinkedIn Engineering — *How LinkedIn customizes Apache Kafka for 7 trillion messages per day* — https://www.linkedin.com/blog/engineering/open-source/apache-kafka-trillion-messages
- Uber Engineering — *Disaster recovery for multi-region Kafka at Uber* — https://eng.uber.com/kafka/
- Confluent — *Parallel Consumer* — https://github.com/confluentinc/parallel-consumer
- Microsoft Azure Architecture Center — *Event Sourcing pattern* — https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing

---

## 9. Kafka vs. alternatives & inherent limitations

### Synthesized findings

Kafka's defining choice — **brokers co-owning compute and storage**, persisting a partitioned, replicated log on local disk via the OS page cache — is both its performance strength and the root of its most-cited limitations. Competitors attack this on three fronts: **(1) compute/storage separation** (Pulsar via BookKeeper; WarpStream/AutoMQ/KIP-1150 via object storage) for elasticity and to eliminate cross-AZ replication cost (70–90% of self-managed spend at high throughput); **(2) runtime efficiency** (Redpanda's C++ thread-per-core, claiming 10× lower tail latency — a claim Vanlightly showed is workload-dependent and often reversed on identical hardware); **(3) managed simplicity** (Kinesis/Pub-Sub trading throughput-per-partition for a metered shard model with hard caps like 1 MB/s per shard).

Kafka's structural weaknesses are well-documented: ordering is **per-partition only** (never global), there is **no per-message TTL, priority, or content-based routing** (where RabbitMQ excels), and partition count has a practical ceiling (raised ~100× by KRaft). Rebalances were historically "stop-the-world," progressively fixed by **KIP-429** (Kafka 2.4) and **KIP-848** (GA in Kafka 4.0). Cross-AZ replication cost and a real (but tunable) latency floor are the two most material operational critiques. Most vendor benchmarks are self-serving — scrutinize fsync/durability, JVM version, and Coordinated Omission.

### Key facts (with sources)

- **Partition ceiling (ZooKeeper):** ~4,000/broker, ~200,000/cluster; the cluster limit exists "to accommodate the rare event of a hard failure of the controller," which reloads all partition state from ZooKeeper on failover. — *Confluent*
- **Controller failover cost:** Kafka 1.0.0 took **6.5 min** for controlled shutdown of 5 brokers / 50,000 partitions; 1.1.0 cut it to **3 s** (async ZK writes + batched leader comms). State reload of 100,000 partitions: **28 s → 14 s**. — *Confluent*
- **KRaft raises the ceiling ~100×:** Instaclustr created ~600,000 partitions on a single KRaft broker, stable lab operation at ~2 million/cluster (real-world: hundreds of thousands). — *Instaclustr*
- **KRaft practical limits remain:** bounded by per-broker FDs, replica-fetcher threads, metadata RAM. Linux `vm.max_map_count` default **65,530** is a hard ceiling at modest density — raise to 1,000,000+.
- **Cross-AZ networking dominates cost:** Confluent models networking at **70%** of self-managed infra for 20 MBps ($4,838/$8,778/mo) and **87%** for 100 MBps ($24,192/$40,988/mo); after tiered storage "networking alone can comprise ~90%." Modeled rate: $0.02/GB on AWS.
- **Replication amplifies cross-AZ:** 3-zone RF=3 → every 1 GB ingress = ~2 GB cross-AZ replication; producers ~⅔ cross-zone, consumers ~⅔ cross-zone absent optimization.
- **Fetch-from-follower (KIP-392 + rack-awareness KIP-881):** consumers read from a same-AZ replica; aligning fetch traffic can cut total cluster cost ~50%. — *getkafkanated / Apache cwiki*
- **WarpStream diskless:** stateless Agents write directly to S3 (no local disk, no cross-AZ replication); claims 5–10× TCO reduction. Latency: **P99 ~400 ms produce**, ~**1 s P99 producer-to-consumer e2e**. — *WarpStream*
- **WarpStream storage economics:** triple-replicated local SSD ~10–20× the per-GiB cost of S3 (i3en.xlarge ~$0.13/GiB/mo raw → ~$0.39/GiB at RF=3 vs ~$0.021/GiB on S3). Attributes 70–90% of high-throughput cost to inter-zone bandwidth.
- **AutoMQ diskless-with-WAL:** stateless brokers + data on S3, but a tiny (~10 GB) EBS WAL for write acceleration → single-digit-ms latency (unlike pure-S3), ~26× storage reduction vs 3-replica EBS gp3 (~$0.60/GiB/mo), ~10× overall TCO, no cross-AZ.
- **Pulsar separates compute/storage:** stateless brokers, data in Apache BookKeeper (distributed WAL across bookies); independent scaling, seamless add without partition reassignment. BookKeeper uses its own caches, not the OS page cache.
- **Pulsar benchmark (StreamNative — advocacy):** 3× i3en.6xlarge, single-partition max: Pulsar 700 MB/s (journaled) / 580 (no journal) vs Kafka 280; 100-partition: Pulsar 1,600 (no journal) vs Kafka 1,087. Publish p99 @500MB/s: Pulsar 1.58 ms (no journal) / 7.89 ms (journaled) vs Kafka 3.46 ms; p99.9 Kafka 54.56 ms. Catch-up read: Pulsar 3.2 GB/s vs Kafka 2.0 GB/s.
- **Redpanda claim:** "10× faster tail latencies with up to 3× fewer nodes" (OMB, 4× m5n.8xlarge, Kafka 3.2.0 vs Redpanda 22.2.2).
- **Redpanda claim refuted on identical hardware (Vanlightly):** on 3× i3en.6xlarge, Kafka often beat Redpanda — at 50 producers/500 MB/s Redpanda topped out at 330 MB/s (Kafka hit target); NVMe saturation Kafka **1,900** vs Redpanda **1,400** MB/s; under TLS+50 producers Redpanda e2e latency hit **24 s** vs sub-second Kafka; after 12h Redpanda p99 reached 3.5 s / p99.99 26 s while Kafka improved. Verdict: claims "greatly exaggerated" and "not generalizable."
- **Benchmark methodology traps (Vanlightly):** OMB had Kafka misconfigured with `log.flush.interval.messages=1` (fsync per batch), used Java 11 instead of 17 (hurts Kafka, esp. with TLS), inconsistent offset-commit (Redpanda hard-coded 5 s vs Kafka per-poll). Redpanda's "Kafka is unsafe because it doesn't fsync" claim is FALSE — Kafka relies on replication+recovery.
- **Kinesis shard model & caps:** each shard = 1 MB/s (1,000 records/s) write, 2 MB/s read; default 500 shards/account/region. A Kafka partition on NVMe absorbs 10–100+ MB/s. Provisioned Kinesis: $0.015/shard-hr + $0.014/M PUT; on-demand $0.04/GB in + $0.04/GB out.
- **RabbitMQ vs Kafka semantics:** RabbitMQ = "smart broker / dumb consumer" with exchange routing (direct, topic, fanout, headers), per-message priority, per-message/queue TTL. Kafka = "dumb broker / smart consumer": no per-message TTL (topic-level/time-based only), no priority, no content-based routing.
- **Per-partition ordering only:** Kafka guarantees order within a single partition, never globally (true since the 2011 Kreps/Narkhede/Rao paper). Global ordering requires a single partition (sacrificing throughput). Keep related events ordered by partitioning on a key.
- **Rebalance evolution:** original "stop-the-world" → **KIP-429** (Kafka 2.4) incremental cooperative rebalancing → **KIP-848** (GA Kafka 4.0) moves assignment to the broker coordinator via `ConsumerGroupHeartbeat`, removes the global sync barrier, reported up to ~20× faster.
- **Latency floor tunable but real:** Kafka doesn't fsync on the produce path by default (returns after page cache). A tier-1 bank held **sub-5 ms p99 e2e at 1.6M msg/s** (<5 KB messages) only by tuning serialization, 10GbE, broker/replication. Moving to XFS cut producer-latency outliers >65 ms SLO by **82%** (Allegro).

### Formulas & heuristics

- **Partition sizing (ZK-era):** ≤4,000/broker and ≤200,000/cluster; on KRaft soft, bounded by FDs, fetcher threads, RAM, `vm.max_map_count` (≥1,000,000).
- **Cross-AZ replication:** 3-AZ RF=3 → `cross-AZ bytes ≈ 2 × ingress (replication) + ~2/3 × ingress (producer) + ~2/3 × consumer-read`, before FFF. Networking ≈ 70–90% of self-managed infra at high throughput.
- **Effective cross-AZ stream cost (AWS retail):** ~$0.02/GiB ($0.01 in + $0.01 out); WarpStream cites ~$0.053/GiB streamed end-to-end through classic Kafka in the best case.
- **Diskless latency/cost heuristic:** pure-S3 swaps a low single-digit-ms floor for ~400 ms produce p99 / ~1 s e2e p99 in exchange for ~5–10× lower TCO and zero cross-AZ replication; WAL-assisted (AutoMQ) recovers single-digit-ms.
- **Kafka-vs-Kinesis throughput/$:** one Kinesis shard = 1 MB/s hard cap; a Kafka partition = 10–100+ MB/s. Kafka wins throughput-per-unit; Kinesis wins zero-ops + pay-per-shard for spiky/low-volume.
- **Global ordering cost:** total order forces partition count = 1 — design event keys for partition-local ordering instead.
- **Benchmark validation checklist (Vanlightly):** verify (a) equivalent fsync/flush (not `log.flush.interval.messages=1` on Kafka), (b) Java 17+ (not 11, esp. with TLS), (c) consistent offset-commit cadence, (d) Coordinated Omission corrected, (e) long-running (12–36h).

### Case studies

- **Confluent / Vanlightly (May 2023):** re-ran Redpanda's own OMB on identical 3× i3en.6xlarge. Redpanda collapsed under 50 producers (330 MB/s vs 500 target; 24 s e2e under TLS) and degraded over 12+ hours while Kafka held/improved. Root causes: OMB misconfig (forced fsync, Java 11, inconsistent commits). Redpanda's 10×/3× claims do not generalize.
- **StreamNative (Pulsar vendor, 2022):** reported Pulsar beating Kafka on 3× i3en.6xlarge. Caveat: "no journal" Pulsar weakens durability vs Kafka `acks=all` — not apples-to-apples on safety.
- **Confluent cost model:** self-managed at 100 MBps ≈ $40,988/mo of which $24,192 (87%) networking, $14,515 (52%) storage, $2,281 (8%) compute — networking dominates TCO.
- **WarpStream customers (Grafana Labs, Character.AI, Cursor, Robinhood, Goldsky, ShareChat):** 48–90% TCO reduction vs Kafka/MSK by eliminating local disks and cross-AZ replication — at ~1 s p99 e2e.
- **Allegro (InfoQ):** migrated brokers to XFS + ext4 tuning (writeback journaling, fast-commit), cut producer-latency outliers >65 ms SLO by 82%, p999 → 500–800 ms — much tail latency is filesystem/OS-level, not protocol-level.
- **Tier-1 bank (Confluent):** sub-5 ms p99 e2e at 1.6M msg/s with <5 KB messages for trading — but only via Protobuf serialization, 10GbE, broker tuning, minimized replication delay.

### Cautions

- **Vendor benchmarks are advocacy:** StreamNative, Redpanda, WarpStream, AutoMQ all favor their product. Check durability parity (fsync/journal/acks), JVM version, commit cadence, test duration. Vanlightly's re-run reversed Redpanda's headlines.
- **Redpanda "Kafka doesn't fsync = unsafe" is FALSE** — Kafka relies on replication + log recovery rather than mandatory per-write fsync; a deliberate latency/durability design.
- **200K partitions is a ZooKeeper-era number** — KRaft raises ~100× (lab ~2M), but practical limits (FDs, fetcher threads, RAM, `vm.max_map_count=65,530` default) still apply. Don't quote 200K as current hard limit without the KRaft caveat.
- **Rebalance "stop-the-world" is largely historical** — KIP-429 (2.4, 2019) and KIP-848 (GA 4.0) substantially reduce disruption. Critiques citing full stop-the-world apply to old clients/eager assignors.
- **Diskless Kafka is still early/evolving upstream** — WarpStream/AutoMQ are products; in Apache Kafka itself, diskless is in-flight (KIP-1150 + revisions, KIP-1163/Inkless lineage). Don't present upstream diskless as shipped/GA.
- **Cross-AZ % varies with throughput and optimization** — the 70–90% figure is for high-throughput clusters without FFF; quote the workload assumption.
- **Per-message TTL/priority/routing gaps are by design, not bugs** — Kafka's topic-level retention, strict per-partition order, and key-based routing are intentional log-semantics tradeoffs.
- **Kinesis/Pub-Sub shard economics flip with scale** — cheap for spiky/low-volume but on-demand ~2–3× more expensive at sustained throughput; the 1 MB/s shard cap + 500-shard default + hot-shard skew become real ceilings.

### Sources

- Confluent — *Apache Kafka Supports 200K Partitions Per Cluster* — https://www.confluent.io/blog/apache-kafka-supports-200k-partitions-per-cluster/
- Instaclustr — *KRaft Abandons the ZooKeeper Part 3 — Maximum Partitions* — https://www.instaclustr.com/blog/apache-kafka-kraft-abandons-the-zookeeper-part-3-maximum-partitions-and-conclusions/
- Jack Vanlightly — *Kafka vs Redpanda Performance — Do the claims add up?* — https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up
- Jack Vanlightly — *A Fork in the Road: Deciding Kafka's Diskless Future* — https://jack-vanlightly.com/blog/2025/10/22/a-fork-in-the-road-deciding-kafkas-diskless-future
- Redpanda — *Redpanda vs. Kafka: A performance comparison* — https://www.redpanda.com/blog/redpanda-vs-kafka-performance-benchmark
- StreamNative — *Apache Pulsar vs. Apache Kafka 2022 Benchmark* — https://streamnative.io/blog/apache-pulsar-vs-apache-kafka-2022-benchmark
- Conduktor — *Kafka vs Pulsar: Architecture Compared* — https://www.conduktor.io/glossary/kafka-vs-pulsar
- WarpStream — *Kafka Is Dead, Long Live Kafka* — https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka
- WarpStream — *Architecture (docs)* — https://docs.warpstream.com/warpstream/overview/architecture
- AutoMQ — *Innovation in Shared Storage Makes Kafka Great Again* — https://www.automq.com/blog/innovation-in-shared-storage-makes-kafka-great-again
- Confluent — *Uncovering Kafka's Hidden Infrastructure Costs* — https://www.confluent.io/blog/understanding-and-optimizing-your-kafka-costs-part-1-infrastructure/
- Apache cwiki — *The Path Forward for Saving Cross-AZ Replication Costs KIPs* — https://cwiki.apache.org/confluence/display/KAFKA/The+Path+Forward+for+Saving+Cross-AZ+Replication+Costs+KIPs
- *KIP-392: Allow consumers to fetch from closest replica* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica
- *KIP-848: The Next Generation of the Consumer Rebalance Protocol* — https://cwiki.apache.org/confluence/display/KAFKA/KIP-848:+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
- Confluent — *KIP-848: A New Consumer Rebalance Protocol for Apache Kafka 4.0* — https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/
- Quix — *Apache Kafka vs. RabbitMQ* — https://quix.io/blog/apache-kafka-vs-rabbitmq-comparison
- Index.dev — *Apache Kafka vs Amazon Kinesis vs Google Pub/Sub* — https://www.index.dev/skill-vs-skill/apache-kafka-vs-amazon-kinesis-vs-google-pubsub
- Confluent — *Tail Latency at Scale with Apache Kafka* — https://www.confluent.io/blog/configure-kafka-to-minimize-latency/
- Confluent — *How a Tier-1 Bank Tuned Apache Kafka for p99 Latency for Trading* — https://www.confluent.io/blog/tier-1-bank-ultra-low-latency-trading-design/
- Martin Kleppmann — *Should you put several event types in the same Kafka topic?* — https://martin.kleppmann.com/2018/01/18/event-types-in-kafka-topic.html
- Kreps, Narkhede, Rao (2011) — *Kafka: a Distributed Messaging System for Log Processing* — https://notes.stephenholiday.com/Kafka.pdf

---

# Quick-reference appendices

## Appendix A. Capacity & sizing formulas (quick-reference)

| # | Quantity | Formula | Notes / source |
|---|----------|---------|----------------|
| A1 | **Partition count** | `N ≥ max(t/p, t/c)` | `t` = target throughput; `p`/`c` = single-partition producer/consumer rate. Use the slowest consumer path for `c`. (Jun Rao) |
| A2 | **Per-partition planning rate** | ~**10 MB/s** (conservative; up to "10s of MB/s") | e.g., 100 MB/s topic ÷ 10 ≈ 10 partitions. (Jun Rao) |
| A3 | **Broker count** | `max(peak × RF / per-broker-ceiling, partitions / max_per_broker, 3)` | Floor of 3 for availability. |
| A4 | **Egress** | plan for **≥ 2× ingress** | Replication + consumer fan-out. |
| A5 | **Per-broker partition cap (replication latency)** | `≤ 100 × (#brokers) × (RF)` | Bounds the ~20 ms/1000-partition replication latency. (Jun Rao) |
| A6 | **Storage cost/month** | `(MB ingress/sec) × 86,400 × (retention days) × 0.001 GB/MB × RF × ($/GB-mo)` | EBS ≈ $0.08/GB-mo. (Confluent) |
| A7 | **Cross-AZ throughput (3 AZ)** | `(ingress × 2/3) + (egress × 2/3) + (ingress × [RF-1])` | RF=3 → replication term = `ingress × 2`, dominates. (Confluent) |
| A8 | **Quick cross-AZ $/mo (AWS, RF=3)** | `GB/mo × (2/3 + (RF-1) + fanout × 2/3) × $0.02`, where `GB/mo = ingress_MiB/s × 60 × 60 × 24 × 30 / 1024` | $0.02/GB effective ($0.01 each direction). (AutoMQ) |
| A9 | **Replication amplification** | `(RF-1)` GB cross-AZ per 1 GB written | RF=3 → 2 GB cross-AZ per 1 GB. (AutoMQ) |
| A10 | **Broker file descriptors** | `(#partitions × partition_size) / segment_size + 1 per connection` | Set `ulimit nofile ≥ 100,000`. (Confluent/Cloudera) |
| A11 | **`num.io.threads`** | `≈ 8 × (#data disks)` | Bounded by CPU cores / disk bandwidth. (Strimzi/Red Hat) |
| A12 | **Socket buffer size** | `= bandwidth-delay product = link_bandwidth × round-trip latency` | Default 100 KB throttles high-BDP links; raise toward 1 MB. (Strimzi) |
| A13 | **Consumer fetch memory** | `≈ #brokers × fetch.max.bytes`, and `≈ #partitions × max.partition.fetch.bytes` | Size heap before raising fetch limits. (Strimzi) |
| A14 | **`heartbeat.interval.ms`** | `≈ session.timeout.ms / 3` | (Strimzi/Conduktor) |
| A15 | **`buffer.memory`** | `≥ batch.size` (+ headroom for compression + in-flight) | Else `send()` blocks up to `max.block.ms`. (Strimzi) |
| A16 | **`vm.max_map_count`** | `≥ 262144` (above # of `.index` files); KRaft per-broker partition ceiling ≈ `65,530 / 2 ≈ 32,765` until raised | (Cloudera/Confluent; Instaclustr) |
| A17 | **JVM heap** | ~**6 GB**; rest of RAM → page cache; provision cache for `write_throughput × ~30 s` | 64 GB box / 6 GB heap → ~28–30 GB cache. (Confluent) |
| A18 | **Compacted-topic size** | `≈ (distinct live keys) × (record size)` | Independent of total throughput. |
| A19 | **State reconstruction** | `current_state = fold(event log from offset 0)` | Event sourcing / Kappa. |
| A20 | **Consumer-group parallelism ceiling** | `max consumers = partition count` | Cannot scale consumption beyond partitions. |
| A21 | **Failover unavailability (mental model)** | unclean broker loss `≈ 5 ms × #leader-partitions`; ZK controller loss adds `≈ 2 ms × total partitions` | KRaft removes the second term. (Jun Rao, 2015-era) |

---

## Appendix B. Partition-count heuristics (box)

> **Sizing**
> - `partitions = max(t/p, t/c)`; per-partition planning rate ~10 MB/s.
> - Worked example: 100 MB/s ÷ 10 MB/s ≈ 10 partitions to start.
> - **Throughput peaks around ~100 partitions** (~2M msg/s in Instaclustr's test) and **latency rises sharply past ~1,000**. More partitions ≠ more performance.
>
> **Limits**
> - **ZooKeeper-era:** ≤ ~4,000/broker, ≤ ~200,000/cluster (Kafka 1.1.0+ only — KIP-227). Pre-1.1.0 was far more fragile (50K partitions = 6.5 min controlled shutdown).
> - **KRaft:** targets "millions"; Confluent lab 2M/cluster; Instaclustr ~600k/single broker. But per-broker is still gated by FDs, fetcher threads, RAM, and `vm.max_map_count` (~32,765/broker default on Linux until raised).
>
> **Costs of too many partitions**
> - Longer leader-election / controller-failover windows (`~5 ms`/partition election; `~2 ms`/partition ZK metadata init).
> - More open file descriptors (set `nofile ≥ 100,000`), more producer/consumer buffer memory (tens of KB/partition).
> - More replication latency (`~20 ms` per 1,000 partitions replicated).
> - Direct dollar cost in metered clouds (~$13/partition/year Confluent Cloud — verify).
>
> **Repartitioning keyed topics (the hard problem)**
> - `hash(key) % N` remaps keys when N changes → per-key ordering breaks across the resize boundary; Kafka Streams state stores don't follow.
> - **You cannot decrease partitions.**
> - **Fix:** create a NEW topic at the target count, copy/replay old → new, switch producers/consumers (dual-write + drain). **Over-partition up front** for keyed topics.
> - Adding partitions does **not** fix a hot key (it still hashes to one partition) — redesign or salt the key instead.

---

## Appendix C. Cost levers (box)

> Ordered by effort/impact. Networking ≥ 50% of a multi-AZ cloud Kafka bill (80–90%+ after tiered storage). Compute is usually smallest. **(AWS/GCP-centric — Azure inter-AZ historically free.)**
>
> | # | Lever | Effect | Tradeoff / caveat |
> |---|-------|--------|-------------------|
> | 1 | **Producer compression** (lz4 default, zstd for ratio) | Cuts **storage + replication + fetch** bytes at once; ~10–12× on text/JSON | Near-free; per-batch (needs decent batch depth); little benefit on binary/pre-compressed data |
> | 2 | **Fetch-from-follower** (KIP-392 + rack-awareness KIP-881) | Eliminates ~all **consumer** cross-AZ (Grab → zero); ~50% total cluster cost cut | +up to ~500 ms tail latency; broker load skew; does NOT touch produce/replication |
> | 3 | **Tune RF & retention** | Linear cut to storage AND cross-AZ replication (RF 3→2 on non-critical topics; shorter retention) | Lower RF reduces fault tolerance |
> | 4 | **Tiered storage** (KIP-405, GA 3.6) | Cuts storage **30–90%** (S3 ~$0.02/GiB-mo vs EBS ~$0.08–0.10); decouples storage from compute | **Does NOT reduce cross-AZ networking** — common misconception; networking can become 80–90%+ of TCO |
> | 5 | **Diskless / object-store** (WarpStream, AutoMQ, KIP-1150) | Kills replication + produce cross-AZ (writes straight to S3, leaderless); ~80% TCO cut | +200–400 ms typical (up to ~2.4 s e2e); KIP-1150 accepted ~Mar 2026 but not yet production-ready OSS; vendor % figures are directional |
>
> **Hard floor:** with classical Kafka + fetch-from-follower, produce + replication cross-AZ remains (~$13.8k/mo of AutoMQ's $24k example) — only diskless/object-store designs remove it.

---

## Appendix D. Comparative systems table

| System | Architecture model | Storage | Ordering | Key tradeoff / when it wins |
|--------|-------------------|---------|----------|------------------------------|
| **Apache Kafka** | Brokers co-own compute + storage; partitioned, replicated log; OS page cache + zero-copy | Local disk (EBS/NVMe); tiered storage (KIP-405) offloads cold segments to S3 | Per-partition only (never global) | Highest throughput-per-partition + replay/storage; but cross-AZ replication cost (70–90% of cloud bill), no per-message TTL/priority/routing, partition ceiling, tunable latency floor |
| **Apache Pulsar** | Stateless brokers; compute/storage **separated** | Apache BookKeeper (distributed WAL across "bookies"); own caches, not OS page cache | Per-partition (+ ordering keys) | Independent scaling + seamless storage add without partition reassignment; operational complexity (ZK + BookKeeper + brokers); "no-journal" benchmarks weaken durability |
| **Redpanda** | Single C++ binary, **thread-per-core**, no JVM, Raft, no page-cache reliance | Local disk (tiered to object store available) | Per-partition | Markets 10× lower tail latency / 3× fewer nodes — but Vanlightly showed claims reverse on identical hardware; simpler ops (no JVM/ZK), Kafka-API compatible |
| **Amazon Kinesis** | Fully managed, **metered shard** model | Managed (24h–365d retention) | Per-shard (partition key) | Zero-ops + pay-per-shard for spiky/low-volume; hard **1 MB/s write per shard** cap, 500-shard default quota, hot-shard skew, ~2–3× pricier on-demand at sustained scale |
| **Google Pub/Sub** | Fully managed, global, auto-scaling; no partitions/shards to manage | Managed | **No ordering by default** (opt-in ordering keys per key) | Hands-off elastic scaling + global by default; less throughput-per-unit control, at-least-once with separate ordering semantics |
| **RabbitMQ** | "Smart broker / dumb consumer"; exchange-based routing | In-memory + disk queues; per-message broker state | Per-queue (FIFO); **per-message priority** breaks strict order | Rich routing (direct/topic/fanout/headers), **per-message TTL + priority**, content-based routing; lower throughput, latency degrades >~30 MB/s; per-message state is the cost |
| **WarpStream** | **Diskless / S3-native**; stateless "Agents," no local disk, no inter-broker replication, leaderless | Object storage (S3 / S3 Express One Zone) only | Per-partition (via control plane) | ~5–10× TCO reduction (no local disk, no cross-AZ replication), Kafka-API compatible; **~400 ms produce p99 / ~1 s e2e p99** — not for sub-100 ms workloads |

*All cross-system performance claims are vendor benchmarks unless attributed to an independent source (e.g., Vanlightly) — treat as directional and check durability parity, JVM version, and test duration.*

---

## Appendix E. Common pitfalls / version caveats

**Partition limits & control plane**
- The **~4,000/broker, ~200,000/cluster** numbers are **ZooKeeper-era** and only safe in **Kafka 1.1.0+** (KIP-227 + batched controller writes). Don't present 200K as a universal constant.
- KRaft's "millions of partitions" is a **target + 2M lab result**, not a per-broker production SLA. Real per-broker counts are gated by FDs, RAM, fetcher overhead, rebalance time, and `vm.max_map_count` (~32,765/broker default). "KRaft means partitions are free" is wrong.
- **ZooKeeper mode is deprecated in Kafka 3.5 and removed in 4.0** — ZK-specific metrics/runbook steps apply only to older clusters; KRaft's controller quorum replaces them.
- "Controller failover takes 20+ minutes" is a ZK-at-extreme-scale figure — cite the canonical 1.1.0 numbers (6.5 min→3 s controlled shutdown; 28 s→14 s reload) or the 2M lab (~120 s→~20–30 s).

**Durability & data loss**
- **`unclean.leader.election.enable`** default is version-dependent: **TRUE before 0.11.0, FALSE after** (and KRaft). Any `UncleanLeaderElectionsPerSec > 0` is actual data loss.
- **`min.insync.replicas` alone guarantees nothing if RF is lower** — `effectiveMinIsr` silently caps it (RF=1 + min.insync.replicas=2 still accepts writes). Verify `min.insync.replicas ≤ replication.factor` per topic.
- **`acks=all` does NOT mean data is on disk** — Kafka acks once replicas have it in **page cache**; no per-message fsync by default. Durability claims must specify fsync (`flush.messages`/`flush.ms`).
- **You cannot reduce partition count**, and increasing it on a **keyed** topic silently breaks per-key ordering and co-partitioned Streams state — the most under-appreciated constraint.

**Producer / consumer tuning**
- **`linger.ms` default:** classic Kafka = 0; **newer builds = 5**. State the version.
- **Compression default is `none`/`producer`, NOT zstd.** zstd arrived in 2.1 (KIP-110); fine-grained level tuning is recent (KIP-390/KIP-780, ~3.8+). Compression is per-batch — tiny batches compress poorly.
- **`max.poll.records` does NOT change fetch volume** — it caps cached records per `poll()` (protects against rebalances).
- **`fetch.min.bytes=1` is tuned for latency, NOT cost** — Kafka does not batch reads by default (New Relic's 15% CPU win came from fixing this).
- **`vm.swappiness=1`, NOT 0** (0 forbids swap and removes the OOM safety net).
- **`linger.ms > 0` only helps when produce rate fills the batch** within the window — on low-rate streams it adds pure latency.

**Rebalances & transactions**
- **Stop-the-world rebalances are largely historical** — KIP-429 (2.4) + KIP-848 (GA 4.0) reduce disruption. Critiques apply to old clients/eager assignors.
- **Hanging-transaction tooling (KIP-664) shipped in Kafka 3.0** — older clusters have no clean abort path. KAFKA-18957 reports recovery still hard under EOS in KRaft 3.9.
- **`transaction.timeout.ms` is bounded by broker `transaction.max.timeout.ms` (15 min)** — a misconfigured/abandoned transactional producer can freeze a partition for `read_committed` consumers up to 15 min. Don't assume the 60 s producer default is the worst case.
- **Give each transactional instance a unique `transactional.id`** — shared ids cause "fencing avalanche."

**Monitoring**
- **`RequestHandlerAvgIdlePercent` is commonly mis-reported** (mis-calculated KAFKA-7295; rate-vs-gauge Datadog #516; KRaft combined-mode KIP-1207). Confirm 0–1 gauge before trusting thresholds.
- **`OfflinePartitionsCount` / `ActiveControllerCount` are controller-level** — aggregate with SUM; alert on the cluster aggregate.
- **Produce purgatory non-zero is NORMAL under `acks=all`** — use as a diagnostic, not an alert.
- **Client-side `records-lag-max` only covers running consumers** — use Burrow / `kafka_exporter` to catch dead/stalled groups.
- Confluent gives **no official `RequestHandlerAvgIdlePercent` threshold** — the 0.2/20% figure is Instaclustr/community practice.

**Cost & diskless**
- **Cross-AZ rate:** AWS = $0.01/GB **each direction** → effective ~$0.02/GB. State per-direction vs effective. GCP ~$0.01/GB once; **Azure inter-AZ historically free** (so cross-AZ ROI is AWS/GCP-centric).
- **Tiered storage (KIP-405) does NOT cut cross-AZ networking** — only storage. Fetch-from-follower / diskless are the networking levers.
- **KIP-1150 (Diskless Topics) was accepted ~March 2026 but acceptance ≠ production-ready OSS.** Design fragmented into Rev1/Rev2/Rev3 + competing KIP-1176.
- **Diskless trades latency for cost** (~200–400 ms typical, up to ~2.4 s e2e) — designed to coexist with classic low-latency topics, not replace them.
- Vendor cost % figures (WarpStream 80–85%, Aiven 80%, AutoMQ ~100%) use favorable assumptions — directional, not audited.

**Architecture pattern**
- **Event sourcing ≠ CQRS ≠ Kafka** — distinct concepts; conflating them over-engineers. CQRS adds risk; use narrowly (Fowler).
- **Outbox/CDC gives at-least-once + eventual consistency, NOT exactly-once or cross-system 2PC** — consumers must be idempotent. Kafka EOS is within Kafka read-process-write only.
- **Log compaction does NOT guarantee one record per key at any instant** — multiple values/tombstones can coexist; timing is non-deterministic. Only "at least the last value per key" holds.
- **Kafka is pull/poll-based, not push** — part of why per-message latency exceeds push brokers (RabbitMQ) at low load.
- **"7 trillion msgs/day" is the ~2019 LinkedIn figure**, not from Kreps' 2013 "The Log" (60 billion writes/day). Pin org scale numbers to their year (LinkedIn: 800B→1.1T→7T→32T, 2015→2025).

**Benchmarks (general)**
- **All vendor benchmarks are advocacy.** Verify: equivalent fsync/flush (not `log.flush.interval.messages=1` on Kafka), Java 17+ (not 11, esp. with TLS), consistent offset-commit cadence, Coordinated Omission corrected, long-running (12–36h). Vanlightly's independent re-run reversed Redpanda's headline numbers.
- **"Redpanda: Kafka doesn't fsync so it's unsafe" is FALSE** — Kafka relies on replication + log recovery by design.
- Confluent's "605 MB/s / p99 5 ms" and Kreps' "2M writes/sec" are config-specific (fsync off; three producers + async) — don't quote without qualifiers.
