# Document 3 — Test-Behaviors Specification (`Given / When / Then`)

> **System:** CDC-Driven Vehicle Read-Model Cache (`.NET 10 Worker`)  
> **Workflow Stage:** RFC → ADR → Spec & Plan → **Test-Behaviors** → PR  
> **Coverage:** All 8 mandatory domain, concurrency, failure-mode, and replay-rebuild scenarios required by the challenge specification.

---

## 1. Summary Matrix of Verified Behaviors

| ID | Behavior / Scenario | Stream(s) Exercised | Expected Projection Outcome | Automated Test Reference |
|---|---|---|---|---|
| **B1** | Happy-path vehicle lifecycle (`REGISTERED → ACTIVE → IDLE → MAINTENANCE → DECOMMISSIONED`) | `cdc.public.vehicles` (`op: c, u`) | State & `stateUpdatedAt` advance monotonically through all 5 states; `stateLsn` reflects latest WAL LSN. | `VehicleProjectionTests.B1_HappyPath_VehicleLifecycle_TransitionsMonotonically` + `E2eReplayTests` |
| **B2** | Happy-path high-frequency position updates | `cdc.public.positions` (`op: c`) | `lat`, `lng`, `posRecordedAt`, and `posLsn` advance to the newest GPS reading while preserving `state`. | `VehicleProjectionTests.B2_HappyPath_PositionUpdates_AdvanceCoordinates` + `E2eReplayTests` |
| **B3** | Out-of-order position event (stale `recorded_at`) | `cdc.public.positions` (`op: c`) | **Rejected (`SkippedStale`)**: newer coordinates in Valkey are never overwritten by an older `recorded_at`. | `VehicleProjectionTests.B3_OutOfOrder_StalePosition_IsRejected` + `E2eReplayTests` |
| **B4** | Out-of-order vehicle state transition (stale `updated_at` / lower `lsn`) | `cdc.public.vehicles` (`op: u`) | **Rejected (`SkippedStale`)**: newer vehicle state in Valkey is never overwritten by an older state transition. | `VehicleProjectionTests.B4_OutOfOrder_StaleStateTransition_IsRejected` + `E2eReplayTests` |
| **B5** | Duplicate CDC event delivery (at-least-once / redelivery) | Both topics (`op: r, c, u`) | **Idempotent No-Op (`SkippedDuplicate`)**: re-applying identical `(timestamp, lsn)` leaves the Valkey Hash unchanged. | `VehicleProjectionTests.B5_DuplicateDelivery_IsIdempotentNoOp` + `E2eReplayTests` |
| **B6** | Cross-stream race: `position` arrives before `vehicle` row exists | `positions` first, then `vehicles` | Position creates `vehicle:{id}` with `state = "UNKNOWN"` and valid GPS; subsequent `vehicle` event populates `state` and `plate` without losing GPS coordinates. | `VehicleProjectionTests.B6_CrossStreamRace_PositionBeforeVehicle_MergesCleanly` + `E2eReplayTests` |
| **B7** | Deleted vehicle (`op: "d"` + Kafka null tombstone) & anti-resurrection against late pre-delete events | Both topics (`op: d` + late `u`/`c`) | Vehicle Hash transitions to soft-tombstone (`deleted = "1"`, `state = "DELETED"`, `deletedLsn = LSN_del`, coordinates cleared); late pre-delete events (`lsn < LSN_del`) are **rejected (`SkippedTombstoned`)**. | `VehicleProjectionTests.B7_DeletedVehicle_SoftTombstones_And_RejectsLatePreDeleteEvents` + `E2eReplayTests` |
| **B8** | Full replay from Kafka offset `0` after Valkey cache flush | Both topics (`offset 0 → EOF`) | Rebuilt Valkey snapshot (`Snapshot B`) is **100% byte-for-byte identical** to pre-flush snapshot (`Snapshot A`) across all vehicles and tombstones. | `VehicleProjectionTests.B8_FullReplayFromOffsetZero_RebuildsIdenticalValkeyState` + `E2eReplayTests` |

---

## 2. Detailed `Given / When / Then` Specifications

### Behavior B1 — Happy-Path Vehicle Lifecycle
- **Given** a clean Valkey cache and a newly registered vehicle `V1` (`plate: "NST-1001"`),
- **When** PostgreSQL commits the sequential lifecycle transitions for `V1`:
  1. `INSERT` with `state = "REGISTERED"` at `t = 10:00:00Z` (`lsn = 100`)
  2. `UPDATE` to `state = "ACTIVE"` at `t = 10:05:00Z` (`lsn = 110`)
  3. `UPDATE` to `state = "IDLE"` at `t = 10:15:00Z` (`lsn = 120`)
  4. `UPDATE` to `state = "MAINTENANCE"` at `t = 10:30:00Z` (`lsn = 130`)
  5. `UPDATE` to `state = "DECOMMISSIONED"` at `t = 11:00:00Z` (`lsn = 140`)
- **Then** after each CDC event is projected, `HGETALL vehicle:{V1}` reflects the latest state, `stateUpdatedAt = 11:00:00Z`, `stateLsn = 140`, and `deleted = "0"`.

---

### Behavior B2 — Happy-Path Position Stream Updates
- **Given** vehicle `V1` exists in Valkey with `state = "ACTIVE"`,
- **When** PostgreSQL inserts sequential GPS telemetry rows into `public.positions` for `V1`:
  1. `(lat = -23.5505, lng = -46.6333, recorded_at = 10:06:00Z, lsn = 200)`
  2. `(lat = -23.5610, lng = -46.6400, recorded_at = 10:07:00Z, lsn = 210)`
  3. `(lat = -23.5725, lng = -46.6512, recorded_at = 10:08:00Z, lsn = 220)`
- **Then** `HGETALL vehicle:{V1}` returns both `state = "ACTIVE"` and the latest coordinates `(lat = -23.5725, lng = -46.6512, posRecordedAt = 10:08:00Z, posLsn = 220)` in a single Valkey read.

---

### Behavior B3 — Out-of-Order Position Rejected
- **Given** vehicle `V1` currently holds a position in Valkey recorded at `10:08:00Z` (`lat = -23.5725, lng = -46.6512, posLsn = 220`),
- **When** a delayed or out-of-order position CDC event arrives for `V1` with `recorded_at = 10:06:30Z` (`lat = -23.0000, lng = -46.0000`, even if its WAL `lsn = 230` is higher due to late ingestion),
- **Then** the atomic Lua script `UpsertPosition.lua` rejects the write (`ProjectionOutcome.SkippedStale`), and `HGETALL vehicle:{V1}` retains `(lat = -23.5725, lng = -46.6512, posRecordedAt = 10:08:00Z, posLsn = 220)`.

---

### Behavior B4 — Out-of-Order Vehicle State Transition Rejected
- **Given** vehicle `V1` is in `state = "MAINTENANCE"` in Valkey with `stateUpdatedAt = 10:30:00Z` and `stateLsn = 130`,
- **When** an out-of-order vehicle CDC event arrives with `state = "ACTIVE"`, `updated_at = 10:05:00Z` (or lower `lsn = 110`),
- **Then** the atomic Lua script `UpsertOrDeleteVehicle.lua` rejects the transition (`ProjectionOutcome.SkippedStale`), and `HGETALL vehicle:{V1}` remains `state = "MAINTENANCE"` (`stateUpdatedAt = 10:30:00Z, stateLsn = 130`).

---

### Behavior B5 — Duplicate CDC Event Delivery (At-Least-Once Idempotency)
- **Given** vehicle `V1` has already projected state event `(state = "ACTIVE", updated_at = 10:05:00Z, lsn = 110)` and position event `(lat = -23.5725, lng = -46.6512, recorded_at = 10:08:00Z, lsn = 220)`,
- **When** Kafka/Debezium redelivers either or both CDC events with the exact same `(timestamp, lsn)` (or emits an initial snapshot `op = "r"` followed by streaming events covering the same LSN),
- **Then** the Lua scripts return `0` (`ProjectionOutcome.SkippedDuplicate`), no fields in `vehicle:{V1}` are mutated, and consumer offsets advance cleanly without error.

---

### Behavior B6 — Cross-Stream Race: Position Arrives Before Vehicle Row
- **Given** no key `vehicle:{V2}` exists in Valkey yet,
- **When** a `positions` CDC event for `V2` (`lat = -25.4284, lng = -49.2733, recorded_at = 12:00:10Z, lsn = 310`) is consumed **before** the `vehicles` `INSERT` event for `V2` (`plate = "NST-2002", state = "REGISTERED", updated_at = 12:00:00Z, lsn = 300`),
- **Then**:
  1. Immediately after the position event, `HGETALL vehicle:{V2}` exists with `state = "UNKNOWN"`, `stateLsn = 0`, and `lat = -25.4284, lng = -49.2733, posRecordedAt = 12:00:10Z, posLsn = 310`.
  2. Immediately after the delayed `vehicles` event (`lsn = 300`) arrives, `HGETALL vehicle:{V2}` merges the vehicle metadata so that `state = "REGISTERED"`, `plate = "NST-2002"`, `stateLsn = 300`, while **preserving** `lat = -25.4284, lng = -49.2733, posRecordedAt = 12:00:10Z, posLsn = 310`.

---

### Behavior B7 — Deleted Vehicle (`op: "d"` + Kafka Null Tombstone) & Anti-Resurrection
- **Given** vehicle `V3` exists in Valkey with `state = "IDLE"`, `stateLsn = 400`, and `posLsn = 410`,
- **When**:
  1. PostgreSQL deletes `V3` (`DELETE FROM public.vehicles WHERE id = V3`), producing a Debezium event with `op = "d"`, `before.id = V3`, `source.lsn = 450`, followed by a Kafka compaction tombstone (`payload = null`), and
  2. Subsequently, a delayed pre-delete position event (`lsn = 420, recorded_at = 12:01:00Z`) and a delayed pre-delete vehicle update (`lsn = 430, state = "ACTIVE"`) arrive out of order,
- **Then**:
  1. Upon processing `op = "d"` (`lsn = 450`), `vehicle:{V3}` is atomically marked with `deleted = "1"`, `state = "DELETED"`, `deletedLsn = 450`, and its active coordinates (`lat`, `lng`) are cleared.
  2. The Kafka `null` payload tombstone is acknowledged without error (`IgnoredKafkaTombstone`).
  3. Both late pre-delete events (`lsn = 420` and `lsn = 430 < deletedLsn = 450`) are **rejected (`SkippedTombstoned`)** — the deleted vehicle is **never resurrected**.

---

### Behavior B8 — Full Replay from Offset 0 Produces Identical Valkey State
- **Given** all scenarios **B1 through B7** have been executed through PostgreSQL → Debezium → Kafka → `.NET 10 Worker` → Valkey, and we capture the complete dictionary of all `vehicle:*` Hashes in Valkey as **`Snapshot A`**,
- **When** the E2E harness:
  1. Stops/pauses the `.NET 10 Worker` consumer group,
  2. Executes `FLUSHALL` on Valkey (wiping the entire cache to zero keys),
  3. Resets the Kafka consumer group offsets for both `cdc.public.vehicles` and `cdc.public.positions` back to **offset `0`** (`TopicPartitionOffset(tp, Offset.Beginning)`), and
  4. Resumes the `.NET 10 Worker` until it reaches the high-watermark offset on all partitions, capturing **`Snapshot B`**,
- **Then** **`Snapshot B` is 100% identical to `Snapshot A`** (every key and every Hash field `vehicleId`, `plate`, `state`, `stateUpdatedAt`, `stateLsn`, `lat`, `lng`, `posRecordedAt`, `posLsn`, `deleted`, `deletedLsn` matches exactly).
