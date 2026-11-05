# Technical Challenge: CDC-Driven Vehicle Cache (.NET 10 Worker)

This challenge is for a position on the **Data Platform team at NsTech**. Thanks for taking the time to work through it. It is a take-home problem designed to look like real work, not a puzzle. We care far more about **how you think** than about a polished happy path — read the evaluation section before you start.

**How we work.** We run a **document-driven workflow**: features move through a disciplined chain — **RFC → ADR → Spec → tests → PR** — where decisions are captured as durable artifacts, humans author the intent and LLMs refine it, and **every change lands as a reviewed PR, never an auto-merge**. The deliverables below (C4, Spec, test-behaviors, ADR) deliberately mirror that chain, and you will submit your solution the same way we ship: as a reviewed PR (see §5).

> **Using AI is expected, not just allowed.** Use Claude, Cursor, Copilot, whatever you normally reach for. We are not testing whether you can produce code an agent can produce — we are testing **your judgment**: how you decompose an ambiguous problem, which trade-offs you surface, what you choose to verify, and whether you can defend every decision in the final interview. The phases AI handles well are the ones we weight least.

## 1. The Challenge

A fleet system stores two things in an operational database:

- **Vehicles** — a row per vehicle with a mutable **state** (e.g. `REGISTERED → ACTIVE → IDLE → MAINTENANCE → DECOMMISSIONED`). Vehicles are **inserted and updated** over time, and can be **deleted**.
- **Positions** — a high-frequency stream of `(vehicleId, lat, lng, recordedAt)` rows. Positions are **inserted**, never updated.

A hot read path — tracking UIs, alerting, risk rules — needs to answer one question with very low latency, for any vehicle: **"Where is it right now, and what state is it in?"** Querying the operational database directly does not scale: it adds read load to the write path and couples every reader to the database.

The architecture you will build decouples them with **Change Data Capture**. Both tables are captured by **Debezium** and streamed as CDC events into **Kafka**. A **.NET 10 worker** consumes those CDC streams and maintains a **Valkey** cache that always reflects, per vehicle, its **latest position** and **current state**. Readers hit Valkey, never the database.

**The hard constraint:** the cache is built **only by reading CDC**. The worker never queries the source database. Everything it knows — every vehicle, every state transition, every position — arrives as a CDC event off Kafka. The cache must be **correct and self-healing**: replay the CDC stream from the beginning and you must rebuild the identical cache.

**What this problem is really about.** This is a **read-model / materialized-view** problem over **two independently-partitioned CDC streams** that must be **joined per vehicle**. The interesting work is not wiring a Kafka consumer to a cache client — it is getting **correctness** right under the realities of CDC:

- **Out-of-order delivery** — a stale position or an old state must never overwrite a newer one.
- **At-least-once + replay** — Debezium re-emits on restart; cache writes must be **idempotent**.
- **Cross-stream races** — a position can arrive *before* the vehicle row's CDC event exists.
- **Snapshot vs streaming** — Debezium's initial snapshot vs ongoing change events.
- **Deletes / tombstones** — a decommissioned or deleted vehicle.

A naive "consume event, `SET` the key" worker passes a demo and is silently wrong in production. Showing us how you avoid that — in your design and in the final interview — is the point of the exercise.

## 2. What You'll Build

### 2.1 Mock writers (drive the source DB only)

You build the things that write to the database, so the system has data to capture:

- **Position writer** — inserts `positions` rows for vehicles at a configurable rate. Must support **deterministic replay** of a fixed sequence (for tests) and the ability to inject **out-of-order** `recordedAt` values and **duplicate** inserts.
- **Vehicle writer** — **inserts** vehicles and **updates** them to drive **state transitions** over time; must also be able to **delete** a vehicle. Must support deterministic replay and injectable **out-of-order updates**.

These write **only** to PostgreSQL. They do not touch Kafka or Valkey — CDC does that.

### 2.2 The .NET 10 worker (the main deliverable)

A .NET 10 worker (e.g. Worker Service / `BackgroundService`) that consumes the two CDC topics and projects them into the Valkey vehicle view. Kafka and Valkey client libraries are your choice — justify them. The worker:

- Derives the entire view **from CDC only** — no direct DB access.
- Applies **event-time / log-position ordering** so older events never clobber newer state.
- Is **idempotent** — re-consuming the same offsets yields the same cache.
- Handles **deletes/tombstones**, the **snapshot→stream** transition, and the **position-before-vehicle** race.

### 2.3 Suggested Valkey view (illustrative — your design wins if defended)

A per-vehicle entry answering both questions in one read, e.g. a hash per `vehicle:{id}` carrying `state`, `stateUpdatedAt`, `lat`, `lng`, `recordedAt`. How you key it, how you make writes conditional on event time, and how a deleted vehicle is represented are decisions for your ADR.

## 3. Deliverables

Your solution is submitted as a **pull request** in your own solution repository (see §5). It must contain the code **and** four short documents. The documents matter as much as the code — they are the artifact chain we actually review.

1. **C4 model** (Mermaid, in-repo): **Context** (mock writers, PostgreSQL, Debezium/Kafka, the worker, Valkey, a cache reader) and **Container** (the .NET worker, Postgres, Kafka + Connect/Debezium, Valkey, the mock-writer apps) levels at minimum; a **Component** view of the worker (per-topic consumers, the per-vehicle projector/merger, ordering + idempotency, cache writer) is a plus. Diagrams must match what `just e2e` actually runs.
2. **Spec + implementation plan**: the read-model design, the CDC event shapes you rely on, the Valkey key/value schema, the worker's component responsibilities, and how you sequenced the work.
3. **Test-behaviors document** (Markdown, Given/When/Then) — must explicitly cover: vehicle state update reflected in cache; new position reflected; **out-of-order** position/state ignored when stale; **duplicate** CDC event is a no-op; **delete/tombstone** removes/flags the vehicle; **position arrives before vehicle exists**; **worker restart** mid-stream loses nothing; **full replay from offset 0 rebuilds the identical cache**. Automate the critical ones.
4. **Reproducible environment** that runs **end-to-end with a single `just` command** from a clean checkout (`just e2e`): env up → mocks write to Postgres → Debezium → Kafka → worker → Valkey → assertion that the view is correct, plus the replay-rebuild check. The environment uses `devenv` + `devspace` + `just` on a local Kubernetes cluster.
5. **ADR** (metadata + Context + Decision + reasoning) recording your **final decisions** — ordering key & conditional-write strategy, idempotency mechanism, Valkey schema, delete/snapshot/replay semantics, cross-stream race handling, and your PostgreSQL/Debezium/Kafka/Valkey/.NET-library choices — with alternatives considered and trade-offs accepted.

## 4. How We Evaluate

Weighted toward judgment, because that is what the role needs.

| Dimension                    | Weight  | What we look for                                                                                                                                                                                           |
| ---------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Design (C4 + Spec + ADR)** | **40%** | Correctness is reasoned through, not stumbled into: ordering, idempotency, the cross-stream race, deletes, snapshot→stream, and replay are all addressed. C4 matches reality.                              |
| **Final interview**          | **25%** | You can explain *why*, identify where your design breaks (esp. out-of-order cross-stream merges and replay), and reason under "what if" pressure — including where an AI suggestion would have been wrong. |
| **Reproducibility**          | **20%** | `just e2e` works from a clean checkout with zero manual steps. If it needs a README of manual fixes, it failed.                                                                                            |
| **Build**                    | **15%** | The worker is correct under duplicates, disorder, and restart; the replay-rebuild check passes. Clean, typed, tested .NET 10.                                                                              |

**Minimum bar to advance:** (a) the env comes up with a single command; (b) `just e2e` proves the Valkey view is correct after a run with duplicates and out-of-order events, **and** that a full replay rebuilds the identical cache; (c) you can defend your design in the interview.

> **If you run short on time, optimize for the design artifacts and a defensible partial build — not a polished happy path.** A sharp C4 + ADR with a worker that handles the hard cases beats a feature-complete worker that silently corrupts the cache on a late event. Our scoring is built to reward that choice; make it deliberately and tell us what you cut and why.

## 5. Time, Process & Submission

- **Time.** Work at your own pace within **one week** of receiving this. We estimate the core is reachable in roughly **6–8 focused hours** — please **do not gold-plate**. If something is taking far longer than that, stop and write down what you would do with more time; that note is itself signal.
- **Optional design review (encouraged).** Before you build, you may book a **~30-minute call** to walk us through your intended design (the §1 hard cases, your Valkey view, your ordering/idempotency strategy). Sharing a design before committing code is exactly how we work — using this is a positive signal, not a crutch.
- **Final interview (~30 min).** After you submit, we run a live session where you present your C4 and defend your decisions, and we probe "what breaks if…". Be ready to screen-share and reason out loud; this round carries real weight.
- **How to submit — a PR, then an email.** Create a Git repository for your solution, do the work on a branch, and open a **pull request** into its main branch (leave it open — do not merge). The PR must contain: the .NET 10 worker, the mock writers, the `devspace`/`devenv` config and Kubernetes manifests, the `just` recipes, and the four documents. Include a top-level **README** with prerequisites and the two commands that matter: `just up` and `just e2e` — assume we clone your branch fresh and run exactly those. Then **send a notification email** to the address your recruiter provided, with the PR link, repo access, and anything you want us to read first. Submitting via an open PR is part of the exercise: it is how the team ships, and the diff (plus your PR description) is what we review.

## 6. Technology Stack

- **Repo philosophy:** `devenv` + `devspace` + `just`, single-command reproducible.
- **Worker:** **.NET 10** (Worker Service / `BackgroundService`). Kafka and Valkey client libraries are your choice, justified in the ADR.
- **Source DB:** **PostgreSQL** (logical replication enabled for CDC).
- **CDC:** **Debezium** on **Kafka Connect**, capturing the `vehicles` and `positions` tables.
- **Streaming:** **Kafka**.
- **Cache:** **Valkey** (Redis-compatible, OSS).
- **Diagrams:** C4 model rendered in **Mermaid** (renders on GitHub, stays maintained).

If you have never used `devenv`/`devspace`, that is fine — they are general-purpose OSS tools, and getting a single-command environment working is part of what we are assessing.

## 7. Hints for Success

- **"Only from CDC" is the whole point.** If the worker ever reads the source database to fix up the cache, you have missed the challenge. The cache must be a pure projection of the CDC stream.
- **Replay is the correctness oracle.** A cache you can rebuild from offset 0 to an identical state is the proof your projection is sound. Design for it from the start.
- **Order by the log, write conditionally.** Decide what "newer" means (Debezium LSN/`ts_ms` vs business timestamps) and make every cache write conditional on it, so a late or replayed event never wins.
- **Mind the cross-stream race.** Positions and vehicle-state are different topics with independent partitions and lag. A position for a vehicle you haven't "seen" yet is normal — decide what the cache shows then, and defend it.
- **Cache: prefer conditional/delete over blind set.** A stale `SET` propagates wrong data to every reader; reason about it explicitly given out-of-order events and deletes.
- **Reproducibility is graded, not a nicety.** Test `just up` and `just e2e` on a clean checkout before you submit.

## 8. Reference Reading (public)

- **Change Data Capture & Debezium** — log-based CDC, the snapshot→streaming model, source metadata (LSN, `ts_ms`), tombstones. ([Debezium documentation](https://debezium.io/documentation/))
- **The dual-write problem & CDC as the fix** — why a cache fed by CDC off the DB beats writing DB-and-cache directly. ([Confluent — Dual-Write Problem](https://www.confluent.io/blog/dual-write-problem/))
- **Idempotent consumers & at-least-once** — the correct default posture; conditional/last-write-wins for cache updates under out-of-order delivery. ([abstractalgorithms — Dual Write Problem & Solutions](https://www.abstractalgorithms.dev/dual-write-problem-and-solutions))
- **.NET Worker Services** — `BackgroundService` / `IHostedService` for long-running consumers. ([.NET Worker Services](https://learn.microsoft.com/dotnet/core/extensions/workers))
- **C4 + Mermaid** — Context 5–10 elements, Container 10–15, one level per diagram. ([Mermaid C4](https://mermaid.js.org/syntax/c4.html))

— Good luck. We are looking forward to seeing how you reason about this. —
