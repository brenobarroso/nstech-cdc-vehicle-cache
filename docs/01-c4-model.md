# Document 1 — C4 Architecture Model (`case-cdc-vehicle-cache`)

> **System:** CDC-Driven Vehicle Read-Model Cache (`.NET 10 Worker`)  
> **Team:** NsTech Data Platform — Technical Challenge  
> **Scope:** Context (Level 1), Container (Level 2), and Component (Level 3) views matching the exact topology deployed by `just up` and verified by `just e2e`.

---

## 1. System Context View (C4 — Level 1)

At the highest level, upstream operational producers (`Mock Vehicle Writer` and `Mock Position Writer`) mutate the operational **PostgreSQL** database. Without ever querying PostgreSQL directly, the **CDC Vehicle Cache System** captures row-level Write-Ahead Log (WAL) changes via **Debezium + Kafka**, projects them through a **.NET 10 Worker**, and maintains a low-latency, self-healing **Valkey** read model that answers in a single atomic read (`HGETALL vehicle:{id}`):

> *"Where is vehicle `{id}` right now, and what operational state is it in?"*

```mermaid
flowchart TB
    subgraph Producers["Operational Upstream Producers"]
        VW["Vehicle Writer\n(Mock Service / E2E Driver)\nInserts, updates & deletes vehicles"]
        PW["Position Writer\n(Mock Service / E2E Driver)\nHigh-frequency GPS stream"]
    end

    subgraph SystemBoundary["CDC Vehicle Cache System (Kubernetes Cluster via DevSpace)"]
        PG[("PostgreSQL 16\n(Source Operational DB)\nwal_level = logical")]
        CDC["CDC Vehicle Cache Pipeline\n(Debezium + Kafka + .NET 10 Worker + Valkey)"]
    end

    subgraph Consumers["Downstream Consumers & Verification"]
        API["Logistics Control Tower / API Consumers\nLow-latency O(1) read per vehicle"]
        E2E["E2E & Replay Verifier (`just e2e`)\nAsserts 8 behaviors & offset-0 rebuild parity"]
    end

    VW -->|"SQL INSERT / UPDATE / DELETE\n(public.vehicles)"| PG
    PW -->|"SQL INSERT\n(public.positions)"| PG
    PG -->|"WAL Logical Replication Stream\n(pgoutput)"| CDC
    CDC -->|"HGETALL vehicle:{id}\nSingle low-latency read"| API
    E2E -->|"Drives deterministic scenarios\n& resets consumer offsets"| CDC
    E2E -->|"Drives deterministic writes"| VW
    E2E -->|"Drives deterministic writes"| PW
```

---

## 2. Container View (C4 — Level 2)

The Container diagram reflects the exact runtime containers orchestrated inside the local Kubernetes cluster by `devspace` (`just up`) and exercised by `just e2e`:

| Container | Technology | Responsibility |
|---|---|---|
| `postgres` | PostgreSQL 16 (`wal_level=logical`) | Operational source of truth hosting `public.vehicles` (mutable lifecycle + deletes) and `public.positions` (insert-only GPS telemetry). Never queried by the Worker. |
| `kafka` | Apache Kafka (KRaft mode) | Durable, partitioned event log storing Debezium CDC topics (`cdc.public.vehicles` and `cdc.public.positions`) from offset `0`. |
| `debezium-connect` | Debezium 2.7 / Kafka Connect | Tails PostgreSQL WAL via `pgoutput` replication slot and emits structured change envelopes (`op: r, c, u, d` + `source.lsn` + `source.ts_ms`). |
| `vehicle-cache-worker` | .NET 10 `BackgroundService` (`C# 13`) | Consumes both CDC topics with manual offset control and executes atomic Lua projection scripts against Valkey. |
| `valkey` | Valkey 8.0 (OSS Redis-compatible) | Low-latency in-memory store holding one Hash per vehicle (`vehicle:{id}`) with monotonic LSN/timestamp guards and soft-tombstone protection. |
| `mock-writers` / `e2e-runner` | .NET 10 CLI / xUnit Test Harness | Deterministically seeds PostgreSQL (normal lifecycle, out-of-order events, duplicates, cross-stream races, deletes) and verifies full replay parity. |

```mermaid
flowchart TB
    subgraph K8s["Local Kubernetes Cluster (managed by DevSpace & Just)"]
        subgraph Writers["Upstream Simulation & E2E"]
            MW["Mock Writers & E2E Verifier\n(.NET 10 / Npgsql / Confluent.Kafka / StackExchange.Redis)"]
        end

        subgraph StorageSource["Source Database Tier"]
            PG[("PostgreSQL 16\nTables: public.vehicles, public.positions\nReplication Slot: debezium_slot")]
        end

        subgraph StreamingTier["CDC & Event Streaming Tier"]
            DBZ["Debezium Kafka Connect\n(PostgresConnector / pgoutput)"]
            KFK[["Apache Kafka (KRaft)\nTopics:\n- cdc.public.vehicles\n- cdc.public.positions"]]
        end

        subgraph ProjectionTier["Projection & Cache Tier"]
            WRK["Vehicle Cache Worker\n(.NET 10 BackgroundService)\nConfluent.Kafka + StackExchange.Redis"]
            VLK[("Valkey 8.0\nRead Model Hash: vehicle:{id}\nAtomic Lua Evaluators")]
        end
    end

    MW -->|"1. Writes deterministic & edge-case rows\n(Npgsql TCP :5432)"| PG
    PG -->|"2. Streams WAL changes\n(Logical Replication)"| DBZ
    DBZ -->|"3. Publishes CDC JSON envelopes\n(op: r/c/u/d + source.lsn)"| KFK
    KFK -->|"4. Consumes batches (at-least-once)\nConsumer Group: vehicle-cache-worker"| WRK
    WRK -->|"5. Atomic conditional projection\n(EVALSHA Lua Scripts :6379)"| VLK
    MW -.->|"6. Reads Snapshot A, flushes Valkey,\nresets Kafka offsets to 0, asserts Snapshot B == A"| VLK
    MW -.->|"Resets Consumer Group Offsets"| KFK
```

---

## 3. Component View — `.NET 10 Worker` (C4 — Level 3)

Inside the `vehicle-cache-worker` container, responsibilities are cleanly separated following **Single Responsibility** and **Dependency Inversion** so that CDC envelope parsing, domain ordering rules, and Valkey Lua execution can be unit-tested independently and verified end-to-end:

```mermaid
flowchart TB
    subgraph KafkaBroker["Apache Kafka"]
        TV[["Topic: cdc.public.vehicles"]]
        TP[["Topic: cdc.public.positions"]]
    end

    subgraph WorkerProcess[".NET 10 Worker (VehicleCache.Worker)"]
        HOST["CdcConsumerHostedService\n(BackgroundService)\nManages consumer loops, graceful shutdown & manual offset commit"]
        DES["DebeziumEnvelopeParser\n(System.Text.Json)\nExtracts op (r/c/u/d), before/after payloads, source.lsn, source.ts_ms,\nand handles Kafka null tombstones"]
        DISP["CdcEventDispatcher\nRoutes parsed VehicleChangeEvent & PositionRecordedEvent\nto the projection engine with telemetry & retry policy"]
        PROJ["ValkeyLuaProjector\n(IVehicleReadModelProjector)\nPreloads SHA1 Lua scripts and executes atomic conditional updates"]
        LUA_V["Lua Script: UpsertOrDeleteVehicle.lua\nEnforces: incoming.stateLsn > max(current.stateLsn, current.deletedLsn)\nApplies soft-tombstone on op == 'd'"]
        LUA_P["Lua Script: UpsertPosition.lua\nEnforces: incoming.posLsn > current.deletedLsn AND\n(recordedAt, posLsn) > (current.recordedAt, current.posLsn)"]
        OBS["WorkerMetrics & StructuredLogger\n(OpenTelemetry / ILogger)\nTracks applied vs. rejected stale/duplicate/tombstoned events"]
    end

    subgraph ValkeyStore["Valkey 8.0"]
        HASH[("Hash: vehicle:{id}\nFields:\n- vehicleId, plate, model\n- state, stateUpdatedAt, stateLsn\n- lat, lng, posRecordedAt, posLsn\n- deleted, deletedLsn")]
    end

    TV -->|"ConsumeResult<string, string>"| HOST
    TP -->|"ConsumeResult<string, string>"| HOST
    HOST -->|"Raw JSON / Tombstone"| DES
    DES -->|"Strongly-typed CDC Domain Event"| DISP
    DISP -->|"ProjectVehicleAsync / ProjectPositionAsync"| PROJ
    DISP -->|"Logs & Counters"| OBS
    PROJ -->|"EVALSHA (Vehicle)"| LUA_V
    PROJ -->|"EVALSHA (Position)"| LUA_P
    LUA_V -->|"Atomic HGET/HSET"| HASH
    LUA_P -->|"Atomic HGET/HSET"| HASH
```

### Key Architectural Guarantees Shown in the Component View
1. **Strict Zero-Postgres Dependency in Worker:** `VehicleCache.Worker` has zero reference to `Npgsql` or PostgreSQL connection strings. Its sole inputs are the Kafka CDC topics.
2. **Manual Offset Commit After Projection:** Offsets are committed to Kafka (`StoreOffset` / `Commit`) **only after** `ValkeyLuaProjector` confirms the atomic Lua script executed in Valkey, guaranteeing at-least-once delivery without silent data loss.
3. **Single-Key Read Model (`vehicle:{id}`):** Both `UpsertOrDeleteVehicle.lua` and `UpsertPosition.lua` project into the same Valkey Hash key `vehicle:{id}`, allowing downstream consumers to fetch the complete vehicle state + latest GPS coordinates in one `HGETALL vehicle:{id}` network round-trip.
