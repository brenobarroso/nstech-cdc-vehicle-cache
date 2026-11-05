# Document 2 — Technical Specification & Implementation Plan

> **System:** CDC-Driven Vehicle Read-Model Cache (`.NET 10 Worker`)  
> **Workflow Stage:** RFC → ADR → **Spec & Plan** → Tests → PR

---

## 1. Problem Overview & Invariants

The worker consumes two independent Change Data Capture (CDC) streams produced by **Debezium for PostgreSQL** (`cdc.public.vehicles` and `cdc.public.positions`) and projects a unified, low-latency **Valkey** read model answering:

> *"Where is vehicle `{id}` right now, and what operational state is it in?"*

To remain **correct and self-healing** under real-world distributed systems failure modes, the projection enforces five strict invariants:

| # | Invariant | Mechanism |
|---|---|---|
| **I1** | **Monotonic State Ordering** | A `vehicles` event (`op ∈ {r, c, u}`) updates state fields if and only if its PostgreSQL WAL Log Sequence Number (`source.lsn`) is strictly greater than both the stored `stateLsn` and `deletedLsn`. |
| **I2** | **Monotonic Position Ordering** | A `positions` event (`op ∈ {r, c}`) updates GPS coordinates if and only if `(recordedAt, source.lsn)` is strictly greater than the stored `(posRecordedAt, posLsn)` **and** `source.lsn > deletedLsn`. |
| **I3** | **Cross-Stream Independence** | If a `positions` event for `vehicleId` arrives before any `vehicles` event, the Hash `vehicle:{id}` is initialized immediately with the GPS fields and `state = "UNKNOWN"` (`stateLsn = 0`). When the `vehicles` event subsequently arrives, it populates the state fields without clobbering the newer GPS fields. |
| **I4** | **Tombstone & Anti-Resurrection on Delete** | When a vehicle is deleted in PostgreSQL (`op = "d"` at `deleteLsn`), the Hash `vehicle:{id}` is converted into a **soft tombstone** (`deleted = "1"`, `state = "DELETED"`, `deletedLsn = deleteLsn`, active coordinates cleared). Any late-arriving pre-delete `position` or `vehicle` event with `source.lsn <= deletedLsn` is rejected atomically. |
| **I5** | **Deterministic Replay Parity (Offset 0)** | Because every write decision is a pure monotonic function of `(recordedAt, source.lsn)` executed atomically inside Valkey, flushing Valkey and resetting the Kafka consumer group to offset `0` always converges to the exact same final state. |

---

## 2. Source Database Schema (PostgreSQL 16)

 Configured with `wal_level = logical` and `REPLICA IDENTITY FULL` on `public.vehicles` so `DELETE` events include the full `before` image:

```sql
CREATE TABLE public.vehicles (
    id              UUID PRIMARY KEY,
    plate           VARCHAR(16) NOT NULL,
    state           VARCHAR(32) NOT NULL, -- REGISTERED | ACTIVE | IDLE | MAINTENANCE | DECOMMISSIONED
    updated_at      TIMESTAMPTZ NOT NULL
);

ALTER TABLE public.vehicles REPLICA IDENTITY FULL;

CREATE TABLE public.positions (
    id              BIGSERIAL PRIMARY KEY,
    vehicle_id      UUID NOT NULL,
    lat             DOUBLE PRECISION NOT NULL,
    lng             DOUBLE PRECISION NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_positions_vehicle_recorded ON public.positions (vehicle_id, recorded_at DESC);
```

---

## 3. Debezium CDC Event Shapes

Debezium is configured with the PostgreSQL `pgoutput` plugin (`topic.prefix = "cdc"`, `time.precision.mode = "connect"`, `decimal.handling.mode = "double"`).

### 3.1. `cdc.public.vehicles` Envelope (`op: "r" | "c" | "u" | "d"`)

```json
{
  " payload": {
    "before": {
      "id": "11111111-1111-1111-1111-111111111111",
      "plate": "NST-0001",
      "state": "REGISTERED",
      "updated_at": "2026-11-05T12:00:00.000000Z"
    },
    "after": {
      "id": "11111111-1111-1111-1111-111111111111",
      "plate": "NST-0001",
      "state": "ACTIVE",
      "updated_at": "2026-11-05T12:05:00.000000Z"
    },
    "source": {
      "version": "2.7.3.Final",
      "connector": "postgresql",
      "name": "cdc",
      "ts_ms": 1762344300000,
      "snapshot": "false",
      "db": "logistics",
      "schema": "public",
      "table": "vehicles",
      "txId": 742,
      "lsn": 34892160
    },
    "op": "u",
    "ts_ms": 1762344300123
  }
}
```

- **On `op: "d"` (Delete):** `after` is `null` and `before` contains the deleted row (`id`, `plate`, `state`, `updated_at`), while `source.lsn` carries the exact PostgreSQL WAL LSN of the `DELETE` operation.
- **On Kafka Compaction Tombstone:** Following an `op: "d"` message, Debezium emits a second Kafka record with the same key and `value = null`. The worker explicitly recognizes `Message.Value == null` as a Kafka log-compaction tombstone and logs/acks it as a safe no-op.

### 3.2. `cdc.public.positions` Envelope (`op: "r" | "c"`)

```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 10042,
      "vehicle_id": "11111111-1111-1111-1111-111111111111",
      "lat": -23.55052,
      "lng": -46.633308,
      "recorded_at": "2026-11-05T12:05:10.000000Z"
    },
    "source": {
      "version": "2.7.3.Final",
      "connector": "postgresql",
      "name": "cdc",
      "ts_ms": 1762344310000,
      "snapshot": "false",
      "db": "logistics",
      "schema": "public",
      "table": "positions",
      "txId": 745,
      "lsn": 34895008
    },
    "op": "c",
    "ts_ms": 1762344310045
  }
}
```

> **Note on Envelope Flexibility:** `DebeziumEnvelopeParser` transparently supports both wrapped (`{ "schema": ..., "payload": { ... } }`) and unwrapped (`{ "before": ..., "after": ..., "source": ..., "op": ... }`) JSON formats so the worker operates identically whether `schemas.enable` is `true` or `false` in Kafka Connect.

---

## 4. Valkey Read-Model Schema & Atomic Lua Scripts

### 4.1. Key Naming & Hash Structure

Each vehicle is stored in a single Valkey Hash at key **`vehicle:{vehicleId}`** (e.g., `vehicle:11111111-1111-1111-1111-111111111111`).

| Hash Field | Type | Description |
|---|---|---|
| `vehicleId` | `string (UUID)` | Canonical vehicle identifier |
| `plate` | `string` | Vehicle license plate (empty string if created via position-first race before vehicle row arrives) |
| `state` | `string` | `REGISTERED`, `ACTIVE`, `IDLE`, `MAINTENANCE`, `DECOMMISSIONED`, `UNKNOWN` (position arrived first), or `DELETED` |
| `stateUpdatedAt` | `int64 (epoch ms)` | Business timestamp (`updated_at`) of the currently applied vehicle state |
| `stateLsn` | `int64` | PostgreSQL WAL `source.lsn` of the last applied vehicle state mutation (`0` if not yet received) |
| `lat` | `string (invariant double)` | Latest latitude (or empty string if no position received yet or vehicle is deleted) |
| `lng` | `string (invariant double)` | Latest longitude (or empty string if no position received yet or vehicle is deleted) |
| `posRecordedAt` | `int64 (epoch ms)` | GPS `recorded_at` timestamp in epoch milliseconds (`0` if no position received yet) |
| `posLsn` | `int64` | PostgreSQL WAL `source.lsn` of the last applied position row (`0` if no position received yet) |
| `deleted` | `"0" \| "1"` | `"1"` if the vehicle has been deleted (`op = "d"`), `"0"` otherwise |
| `deletedLsn` | `int64` | PostgreSQL WAL `source.lsn` of the delete event (`0` if never deleted) |

### 4.2. Why Ordering Uses `(updatedAt, stateLsn)` and `(recordedAt, posLsn)`

In PostgreSQL + Debezium:
1. **`source.lsn`** is the globally monotonic byte offset in the PostgreSQL Write-Ahead Log. Every transaction committed to PostgreSQL receives a strictly increasing LSN.
2. However, business applications (and the challenge's Mock Writers) may also emit **logical out-of-order updates** where a later SQL write carries an older domain timestamp (`updated_at` or `recorded_at`), OR network/partitioning delays deliver an older CDC event after a newer one.
3. Therefore, our Lua scripts enforce **composite monotonic ordering**:
   - **Anti-Resurrection Barrier (Delete Fence):** Any incoming event with `incomingLsn <= current.deletedLsn` OR `incomingTimestamp < current.deletedAt` is rejected immediately (`return 0`).
   - **Vehicle State Ordering (`UpsertOrDeleteVehicle.lua`):**
     - For `op ∈ {"r", "c", "u"}`: Accepted if and only if `(incomingUpdatedAt > currentStateUpdatedAt) OR (incomingUpdatedAt == currentStateUpdatedAt AND incomingLsn > currentStateLsn)`.
     - For `op == "d"`: Accepted if and only if `incomingLsn > currentDeletedLsn` and `incomingLsn >= currentStateLsn`. Marks `deleted = "1"`, `state = "DELETED"`, sets `deletedLsn = incomingLsn`, and clears active coordinates so downstream readers immediately see the vehicle is deleted while retaining the tombstone barrier against late pre-delete events.
   - **Position Ordering (`UpsertPosition.lua`):**
     - Accepted if and only if `incomingLsn > currentDeletedLsn` AND `incomingRecordedAt >= currentStateDeletedAt` AND `((incomingRecordedAt > currentPosRecordedAt) OR (incomingRecordedAt == currentPosRecordedAt AND incomingLsn > currentPosLsn))`.
     - If the key `vehicle:{id}` does not yet exist (position arrived before vehicle creation), the script initializes `state = "UNKNOWN"`, `stateLsn = 0`, `deleted = "0"`, `deletedLsn = 0`, and writes the position fields.

### 4.3. Single-Read Query Contract for Downstream Readers

A downstream client queries a vehicle in **one atomic O(1) command**:
```text
HGETALL vehicle:11111111-1111-1111-1111-111111111111
```
- If the returned Hash is empty **or** `deleted == "1"`, the vehicle does not exist (active cache miss / deleted).
- Otherwise, `state`, `lat`, `lng`, `posRecordedAt`, and `stateUpdatedAt` give the complete, consistent point-in-time view of the vehicle.

---

## 5. Worker Component Responsibilities (`.NET 10`)

The solution is structured cleanly into 4 projects inside `VehicleCache.sln`:

1. **`src/VehicleCache.Core` (Domain & Contracts)**
   - `VehicleState` enum and constants (`REGISTERED`, `ACTIVE`, `IDLE`, `MAINTENANCE`, `DECOMMISSIONED`, `UNKNOWN`, `DELETED`).
   - Immutable records: `VehicleCdcEvent`, `PositionCdcEvent`, `VehicleReadModel`, `ProjectionOutcome` (`Applied`, `SkippedStale`, `SkippedDuplicate`, `SkippedTombstoned`, `IgnoredKafkaTombstone`).
   - `DebeziumEnvelopeParser`: zero-allocation-friendly `System.Text.Json` parser handling Debezium `r/c/u/d` payloads and Kafka null tombstones.
2. **`src/VehicleCache.Infrastructure` (Valkey Lua Projector & Kafka Consumer Wiring)**
   - `ValkeyLuaProjector`: manages `IConnectionMultiplexer` (`StackExchange.Redis`), embeds and executes the two atomic Lua scripts (`UpsertOrDeleteVehicle.lua` and `UpsertPosition.lua`), and provides `GetVehicleAsync` and `SnapshotAllVehiclesAsync` for verification.
   - `KafkaCdcConsumer`: wraps `Confluent.Kafka` `IConsumer<string, string>` with `EnableAutoCommit = false` and `EnableAutoOffsetStore = false`, committing offsets only after Valkey projection succeeds.
3. **`src/VehicleCache.Worker` (.NET 10 Worker Host)**
   - `CdcProjectionWorker` (`BackgroundService`): runs concurrent consumption loops for `cdc.public.vehicles` and `cdc.public.positions` (allowing cross-stream parallelism while Lua guarantees atomic per-vehicle serialization in Valkey) plus health check and structured logging.
4. **`src/VehicleCache.MockWriters` (Deterministic & Chaos Writers to PostgreSQL)**
   - `PostgresVehicleWriter` & `PostgresPositionWriter`: write strictly to PostgreSQL (`public.vehicles` and `public.positions`), supporting:
     - Deterministic scenario playback (fixed seed / scripted timeline)
     - Injectable out-of-order `recorded_at` / `updated_at` writes
     - Duplicate event replay simulation
     - Vehicle deletion (`DELETE FROM public.vehicles`) followed by late pre-delete events
5. **`tests/VehicleCache.Tests` (Automated Verification Suite)**
   - Unit & component tests for `DebeziumEnvelopeParser` and Lua script ordering semantics.
   - End-to-End integration & replay verification suite (`E2eReplayTests`) executed by `just e2e`.

---

## 6. Sequenced Implementation Plan

We broke the work into 5 sequential, verifiable milestones following `RFC → ADR → Spec → Tests → PR`:

| Step | Milestone | Deliverables | Verification Gate |
|---|---|---|---|
| **Step 1** | **Architecture & Spec (Docs 1–4)** | `01-c4-model.md`, `02-spec-and-implementation-plan.md`, `03-test-behaviors.md`, `04-adr-001-atomic-lsn-projection.md` | All 8 test behaviors and 5 hard cases explicitly traced to C4 components and Lua invariants. |
| **Step 2** | **Core Domain, Parser & Valkey Lua Engine** | `VehicleCache.Core`, `VehicleCache.Infrastructure`, Lua scripts (`UpsertOrDeleteVehicle.lua`, `UpsertPosition.lua`) | Unit tests validate envelope parsing (`op: r/c/u/d` + null tombstones) and Lua script edge cases. |
| **Step 3** | **.NET 10 Worker & Mock Writers** | `VehicleCache.Worker` (`BackgroundService`), `VehicleCache.MockWriters` (PostgreSQL writers) | `dotnet build` and `dotnet test` pass with zero warnings. |
| **Step 4** | **Reproducible Local K8s Environment** | `devenv.nix`, `devspace.yaml`, `justfile`, K8s manifests (`postgres`, `kafka`, `debezium`, `valkey`, `worker`) | `just up` boots the full stack from a clean checkout with zero manual intervention. |
| **Step 5** | **Automated E2E & Offset-0 Replay Suite** | `just e2e` runner executing all 8 behaviors + full Valkey flush + Kafka offset reset to `0` + snapshot equality check | `just e2e` exits `0` with deterministic pass report on every run. |
