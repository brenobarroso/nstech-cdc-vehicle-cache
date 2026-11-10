# CDC-Driven Vehicle Cache (`.NET 10 Worker`)

> **NsTech Data Platform — Technical Challenge (`case-cdc-vehicle-cache`)**  
> **Workflow:** `RFC → ADR → Spec → Tests → PR`

A self-healing, low-latency **Valkey** read-model cache built **100% from Debezium PostgreSQL CDC streams** (`cdc.public.vehicles` and `cdc.public.positions`) over **Apache Kafka**, projected by a stateless **.NET 10 `BackgroundService` Worker** using atomic server-side **Lua scripts (`EVALSHA`)**.

Answers in a single `O(1)` Valkey read (`HGETALL vehicle:{id}`):
> *"Where is vehicle `{id}` right now, and what operational state is it in?"*

---

## Architecture & Design Documents (`docs/`)

Following the Data Platform team's document-driven engineering workflow, start here:

| # | Document | Summary |
|---|---|---|
| **1** | [**C4 Architecture Model (`docs/01-c4-model.md`)**](docs/01-c4-model.md) | Mermaid **Context**, **Container**, and **Component** diagrams matching the exact topology run by `just up` and `just e2e`. |
| **2** | [**Technical Spec & Implementation Plan (`docs/02-spec-and-implementation-plan.md`)**](docs/02-spec-and-implementation-plan.md) | Invariants (`I1–I5`), PostgreSQL WAL schema (`REPLICA IDENTITY FULL`), Debezium CDC event shapes, Valkey Hash schema (`vehicle:{id}`), Lua watermarks, and work breakdown. |
| **3** | [**Test-Behaviors Specification (`docs/03-test-behaviors.md`)**](docs/03-test-behaviors.md) | `Given / When / Then` behavioral specifications for all **8 mandatory scenarios (`B1–B8`)**, mapped 1:1 to automated tests. |
| **4** | [**ADR-001: Atomic LSN/Timestamp Projection & Soft Tombstones (`docs/04-adr-001-atomic-lsn-projection.md`)**](docs/04-adr-001-atomic-lsn-projection.md) | Architectural decision record covering server-side Lua conditional writes, soft-tombstone anti-resurrection fencing, rejected alternatives, trade-offs, and **10x scale** evolution. |

---

## Quick Start (Zero-to-Green from a Clean Checkout)

All tools (`dotnet 10`, `kubectl`, `helm`, `devspace`, `just`, `kind`) are pinned in [`devenv.nix`](devenv.nix).

```bash
# 1. Enter reproducible shell (if using devenv)
devenv shell

# 2. Boot PostgreSQL (logical replication), Valkey, Kafka, Debezium Connect & .NET 10 Worker in local K8s
just up

# 3. Run unit tests + E2E scenarios (B1..B7) + Full Offset-0 Replay Rebuild verification (B8)
just e2e
```

Additional recipes available in [`justfile`](justfile):
- `just test` — Run fast deterministic xUnit suite (`B1–B8` in-memory + parser verification) in `< 2s`.
- `just down` — Tear down local Kubernetes cluster and port-forwards.

---

## How the 5 Hard Cases Are Solved

| Hard Case | Solution in `VehicleCache` |
|---|---|
| **1. Out-of-order delivery** | `UpsertOrDeleteVehicle.lua` and `UpsertPosition.lua` compare `(updated_at, source.lsn)` and `(recorded_at, source.lsn)` atomically inside Valkey; stale events return `SkippedStale` (`-1`) without mutating state. |
| **2. At-least-once + Replay** | Duplicate `(timestamp, lsn)` pairs across initial snapshots (`op: "r"`) or redeliveries return `SkippedDuplicate` (`0`) as an idempotent no-op. |
| **3. Cross-stream race (`position` before `vehicle`)** | If a GPS event arrives before its `vehicles` row, `UpsertPosition.lua` initializes `vehicle:{id}` with `state = "UNKNOWN"`, `stateLsn = 0`, and valid coordinates; when the `vehicles` event arrives later, it populates `state` and `plate` without overwriting GPS fields. |
| **4. Snapshot vs. Streaming** | `DebeziumEnvelopeParser` normalizes both snapshot (`op: "r"`) and streaming (`op: "c" \| "u" \| "d"`) envelopes into unified `VehicleCdcEvent` / `PositionCdcEvent` records governed by the same monotonic watermarks. |
| **5. Deletes / Tombstones (Anti-Resurrection)** | On `op: "d"`, `UpsertOrDeleteVehicle.lua` writes a canonical **soft tombstone** (`deleted = "1"`, `state = "DELETED"`, `deletedLsn = source.lsn`, clearing coordinates). Any late pre-delete position or state update (`lsn <= deletedLsn`) is rejected (`SkippedTombstoned`), and Kafka `null` compaction tombstones are safely acknowledged (`IgnoredKafkaTombstone`). |

---

## Repository Structure

```text
├── docs/
│   ├── 01-c4-model.md                         # C4 Context, Container & Component views (Mermaid)
│   ├── 02-spec-and-implementation-plan.md     # Read-model Spec, CDC envelopes, Lua schema & Plan
│   ├── 03-test-behaviors.md                   # Given/When/Then matrix for Behaviors B1..B8
│   └── 04-adr-001-atomic-lsn-projection.md    # Architecture Decision Record & 10x scale analysis
├── src/
│   ├── VehicleCache.Core/                     # Domain models, abstractions & DebeziumEnvelopeParser
│   ├── VehicleCache.Infrastructure/           # ValkeyLuaProjector, Lua scripts & CdcEventDispatcher
│   ├── VehicleCache.Worker/                   # .NET 10 BackgroundService Kafka CDC consumer
│   └── VehicleCache.MockWriters/              # Deterministic & edge-case PostgreSQL writers + E2E runner
├── tests/
│   └── VehicleCache.Tests/                    # xUnit test suite covering B1..B8
├── deploy/k8s/infrastructure.yaml             # Kubernetes manifests deployed by DevSpace
├── devenv.nix                                 # Pinned developer & reviewer toolchain
├── devspace.yaml                              # Local Kubernetes image build, deploy & port-forwarding
├── justfile                                   # Automation entrypoints (just up, just e2e, just test)
└── Dockerfile                                 # Multi-stage .NET 10 container build for the Worker
```
