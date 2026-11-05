# Document 4 — ADR-001: Atomic Valkey Hash Projection with Composite LSN/Timestamp Ordering and Soft Tombstones

- **Status:** Accepted
- **Date:** 2026-11-05
- **Author:** Breno Santos Barroso
- **Context:** NsTech Data Platform — Technical Challenge (`case-cdc-vehicle-cache`)

---

## 1. Context & Problem Statement

We need to build a `.NET 10 Worker` that consumes two independent Debezium PostgreSQL CDC streams from Kafka (`cdc.public.vehicles` and `cdc.public.positions`) and maintains a low-latency read model in **Valkey** answering, in a single read per vehicle:

> *"Where is it right now, and what state is it in?"*

The architecture must satisfy four strict constraints:
1. **CDC-Only Source of Truth:** The worker is prohibited from querying PostgreSQL directly.
2. **Single-Read Low Latency:** A consumer querying Valkey for a vehicle must retrieve both its current lifecycle state and its latest GPS coordinates in one round-trip (`O(1)`).
3. **Resilience to Distributed Systems Anomalies:** The projection must handle out-of-order delivery, duplicate delivery (at-least-once + initial snapshot `op: "r"`), cross-stream races (`positions` arriving before `vehicles`), and deletes/tombstones (`op: "d"` + Kafka `null` payload) without ever resurrecting a deleted vehicle or regressing state/position.
4. **Deterministic Self-Healing Replay:** Wiping Valkey and replaying both Kafka topics from offset `0` must reconstruct the exact same Valkey state regardless of relative consumption speed between the two topics.

---

## 2. Decision

We decided to implement the projection using **three combined mechanisms**:

### 2.1. Unified Per-Vehicle Valkey Hash (`vehicle:{id}`) with Separate Domain Watermarks
Instead of storing `vehicle:state:{id}` and `vehicle:position:{id}` in separate Valkey keys (which would require `MGET` or multiple round-trips and complicate cross-key atomicity), we store a single Valkey Hash per vehicle at `vehicle:{id}` containing two orthogonal watermark pairs plus a tombstone fence:
- **State Watermark:** `(stateUpdatedAt, stateLsn)`
- **Position Watermark:** `(posRecordedAt, posLsn)`
- **Delete Fence (Tombstone Watermark):** `deletedLsn` (with `deleted = "0" | "1"`)

### 2.2. Server-Side Atomic Conditional Writes via Valkey Lua Scripts (`EVALSHA`)
All mutations to `vehicle:{id}` are executed through preloaded server-side **Lua scripts** (`UpsertOrDeleteVehicle.lua` and `UpsertPosition.lua`):
- Valkey executes Lua scripts atomically on its single-threaded command loop.
- Each script reads the current watermarks (`HGET`), compares them against the incoming event's `(domainTimestamp, source.lsn)`, and conditionally applies `HSET` only if the incoming event is strictly newer and not fenced by `deletedLsn`.
- This completely eliminates Read-Modify-Write (RMW) race conditions between concurrent worker tasks consuming `vehicles` and `positions` simultaneously.

### 2.3. Soft-Tombstone Pattern (`deleted = "1"`, `deletedLsn`) Instead of Blind `DEL`
When Debezium emits `op: "d"` for a deleted row in `public.vehicles` with PostgreSQL WAL `source.lsn = L_del`:
- We do **not** execute `DEL vehicle:{id}`.
- Instead, `UpsertOrDeleteVehicle.lua` transitions the Hash to a **soft tombstone**: `deleted = "1"`, `state = "DELETED"`, `deletedLsn = L_del`, and clears active coordinates (`lat = ""`, `lng = ""`).
- Any late-arriving pre-delete `position` or `vehicle` event has `source.lsn < L_del` and is deterministically rejected by the Lua fence (`if incomingLsn <= deletedLsn then return 0`).

---

## 3. Alternatives Considered

### Alternative A — Application-Side Read-Modify-Write (`HGETALL` in .NET + `HSET` / `WATCH-MULTI-EXEC`)
- **How it works:** The C# worker reads the current Hash from Valkey, evaluates ordering rules in C#, and writes back with `HSET` or optimistic locking (`WATCH`).
- **Why rejected:**
  - Requires at least 2 network round-trips per CDC event.
  - Under high-frequency GPS updates or concurrent cross-stream consumption (`vehicles` + `positions` hitting the same `vehicle:{id}`), optimistic locking causes contention retries, while unguarded `HGETALL → HSET` introduces lost-update race conditions.
  - Server-side Lua (`EVALSHA`) performs the comparison and mutation in **1 network round-trip** with **zero lock contention**.

### Alternative B — Hard Key Deletion (`DEL vehicle:{id}`) on `op: "d"`
- **How it works:** When a vehicle is deleted in Postgres, the worker deletes the Valkey key `DEL vehicle:{id}`.
- **Why rejected:**
  - **Fails Hard Case #5 (Zombie Resurrection):** Because `cdc.public.positions` and `cdc.public.vehicles` are consumed from independent Kafka topics (and during an offset-0 replay, the `vehicles` consumer can easily reach the `DELETE` offset before the `positions` consumer finishes draining earlier GPS rows for that vehicle!), a hard `DEL` erases the memory that the vehicle was deleted. The very next pre-delete position event would recreate `vehicle:{id}` as an active entry, breaking both delete correctness and offset-0 replay determinism!
  - Keeping a soft tombstone with `deletedLsn` (optionally paired with a long TTL in production, e.g., 7 days exceeding Kafka topic retention) makes cross-stream replay 100% commutative.

### Alternative C — Kafka Streams / ksqlDB Stream-Table Join Before Valkey
- **How it works:** Join the two topics inside a stateful stream processor (RocksDB state store) and sink the joined record to Valkey.
- **Why rejected:**
  - The challenge specifically evaluates a **.NET 10 Worker** managing the CDC projection logic.
  - Maintaining state in both a local consumer state store and Valkey duplicates operational complexity; pushing the monotonic merge into Valkey Lua keeps the `.NET 10 Worker` completely **stateless** and horizontally scalable.

---

## 4. Trade-Offs & Consequences

### Positive Consequences
1. **Stateless, Horizontally Scalable .NET 10 Worker:** Because state convergence and ordering live atomically in Valkey Lua scripts, we can run 1 or 10 replicas of the `.NET 10 Worker` (up to the partition count of the Kafka topics) without distributed locks in C#.
2. **True Commutativity Across Topics (Order-Independent Replay):** Whether `vehicles` is replayed faster than `positions` or vice-versa during `just e2e`, the final Valkey state is mathematically guaranteed to be identical because every field group (`state*`, `pos*`, `deleted*`) is governed by monotonic watermarks.
3. **O(1) Single-Read Consumer Experience:** Downstream APIs execute a single `HGETALL vehicle:{id}` (or `HMGET`) to obtain both state and position in sub-millisecond latency.

### Negative Consequences & Mitigations
1. **Tombstone Memory Overhead in Valkey:**
   - *Trade-off:* Soft tombstones (`deleted = "1"`) remain in Valkey instead of freeing memory immediately via `DEL`.
   - *Mitigation:* Each tombstone Hash is ~180 bytes. In production, we can attach an `EXPIRE vehicle:{id} <retention_seconds>` (e.g., 7 days, matching Kafka segment retention) on deleted keys so they are garbage-collected once no pre-delete Kafka event can possibly be replayed.
2. **Business Clock Skew on `recorded_at`:**
   - *Trade-off:* Ordering positions primarily by `recorded_at` protects against late ingestion of older GPS packets, but a malfunctioning GPS device sending a future timestamp could pin the position until that timestamp passes.
   - *Mitigation:* At 10x/production scale, the worker validates `recorded_at <= now + max_clock_skew` (e.g., `+5 minutes`) before projecting, routing anomalous future timestamps to a dead-letter/quarantine topic.

---

## 5. What Changes at 10x Load (Production Readiness Notes for Live Review)

If GPS telemetry scales 10x (e.g., from 5,000 to 50,000+ positions/sec across 100,000 vehicles):
1. **Valkey Pipelining / Batch `EVALSHA`:** Instead of awaiting `EVALSHA` one Kafka message at a time, the worker drains a micro-batch from `Confluent.Kafka` (e.g., up to 500 messages or `5ms` window), pre-aggregates the latest state/position per `vehicleId` in memory within the batch, and fires concurrent pipelined `EVALSHA` tasks via `StackExchange.Redis` multiplexer, followed by a single batch offset commit.
2. **Valkey Cluster Hash Tags:** When sharding Valkey across a cluster, keys already use `vehicle:{id}`, which naturally distributes vehicles evenly across hash slots while keeping each vehicle's Lua script single-key (`KEYS[1]`), remaining 100% compatible with Valkey Cluster.
3. **Partitioning by `vehicle_id`:** Ensure Debezium keys both `cdc.public.vehicles` and `cdc.public.positions` by `vehicle_id` across `N` partitions so per-vehicle throughput scales linearly with worker pod replicas.
