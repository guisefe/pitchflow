# pitchflow — Project Specification

> **Real-time football match-event streaming analytics.**
> Replays real StatsBomb match data as a live Kafka stream, processes it with
> Spark Structured Streaming into a Delta Lake medallion, and serves live
> analytics (xG race, momentum, win probability, shot map) on a Streamlit
> dashboard. 100% open-source, runs locally, zero cloud cost.

---

## 1. What it is (in one paragraph)

pitchflow is a streaming data platform that turns a football match into a live
analytics feed. A real match (from StatsBomb's free open data) is replayed
event-by-event in match-clock order onto a Kafka log. A Spark Structured
Streaming job consumes that log, lands the raw events (Bronze), cleans and types
them (Silver), and computes live football metrics (Gold) into Delta Lake. A
Streamlit dashboard reads the Gold tables and updates in real time as the match
unfolds. It is a **streaming medallion** — the same Bronze→Silver→Gold pattern
used in batch lakehouses, but driven by a continuous event stream.

**Elevator pitch:** *"A real-time football analytics pipeline: Kafka ingests live
match events, Spark Structured Streaming computes live xG and momentum into a
Delta lakehouse, and a Streamlit dashboard shows the match come alive — all
open-source and reproducible."*

---

## 2. Goals & non-goals

### Goals
- Demonstrate a **production-shaped streaming pipeline** end to end.
- Showcase the senior patterns: event log, stateful stream processing,
  watermarking, idempotent sink, streaming medallion, live serving.
- Be **fully reproducible and free** — one `docker compose up`, no cloud, no paid data.
- Be **demoable live** in an interview (dashboard updating in real time).

### Non-goals (explicitly out of scope)
- Not a live betting / production-grade win-probability model (the win-prob is a
  documented heuristic, not a trained ML model — unless taken as a stretch goal).
- Not multi-match concurrent ingestion at scale (single match at a time; the
  design allows scaling, but we won't build a cluster).
- Not a cloud deployment (free + local is a deliberate constraint). A future
  Terraform/cloud variant is noted as a separate possible project.
- Not real-time data from a paid live API — replay is the deliberate substitute.

---

## 3. Functional requirements

| ID | Requirement |
| -- | ----------- |
| FR-1 | Download and cache any StatsBomb open-data match by `match_id`. |
| FR-2 | Replay the match's events to Kafka in true chronological order. |
| FR-3 | Replay speed is configurable (e.g. 60× = 90-min match in ~90s). |
| FR-4 | Consume the Kafka stream and persist raw events to a Bronze Delta table. |
| FR-5 | Transform Bronze into a typed, cleaned Silver event table. |
| FR-6 | Compute Gold metrics as streaming aggregates: cumulative xG per team, momentum, live match state (score + win probability), and a shot table. |
| FR-7 | Handle late/out-of-order events correctly (event-time + watermark). |
| FR-8 | Writes are idempotent — replaying the same match never double-counts. |
| FR-9 | A Streamlit dashboard reads Gold and refreshes live during a replay. |
| FR-10 | The dashboard shows: xG race chart, momentum swing, live scoreline + win prob, and a shot map. |

---

## 4. Non-functional requirements

| ID | Requirement |
| -- | ----------- |
| NFR-1 | **Reproducible** — `docker compose up` + documented commands; no manual steps. |
| NFR-2 | **Free** — only open-source tools and free open data; no cloud bill. |
| NFR-3 | **Testable** — core logic unit-tested without a running broker; CI green on every push. |
| NFR-4 | **Runs on a laptop** — fits in ~8 GB RAM (single-node Spark, capped memory). |
| NFR-5 | **Observable** — Redpanda console to inspect the stream; Spark UI for the job. |
| NFR-6 | **Documented** — README quickstart, architecture diagram, and this spec. |
| NFR-7 | **Correct attribution** — StatsBomb credited per their data user agreement. |

---

## 5. Architecture

```
StatsBomb Open Data (real events, free)
        │  download + cache (producer/download.py)
        ▼
┌──────────────────┐   match-clock replay    ┌──────────────┐
│  Replay Producer │ ──────────────────────▶ │   Redpanda   │  topic: match.events
│ (producer/)      │                         │   (Kafka)    │
└──────────────────┘                         └──────┬───────┘
                                                    │
                                    ┌───────────────▼─────────────────┐
                                    │   Spark Structured Streaming     │
                                    │   (streaming/)                   │
                                    │                                  │
                                    │  Bronze  raw events (append)     │
                                    │     │                            │
                                    │  Silver  typed + cleaned events  │
                                    │     │                            │
                                    │  Gold    live aggregates         │
                                    │   • team_xg_timeline             │
                                    │   • team_momentum                │
                                    │   • match_state (score + win%)   │
                                    │   • shots (for shot map)         │
                                    └───────────────┬─────────────────┘
                                                    │  Delta Lake (ACID, idempotent)
                                    ┌───────────────▼─────────────────┐
                                    │   Streamlit live dashboard       │
                                    │   (dashboard/)                   │
                                    └──────────────────────────────────┘
```

**Why a streaming medallion:** it maps the batch lakehouse pattern the author
already works with (Databricks) onto a real-time stream, so the project both
reuses proven structure and proves the streaming skill that batch portfolios lack.

---

## 6. Technology stack

| Layer | Tool | Version | Why this choice |
| ----- | ---- | ------- | --------------- |
| Event log | **Redpanda** (Kafka API) | 24.1.x | Single binary, no ZooKeeper, low RAM — Kafka semantics without the operational weight. |
| Kafka client | **confluent-kafka** | 2.4.0 | Production-standard Python client; works against Redpanda. |
| Stream processing | **Spark Structured Streaming** | 3.5.1 | Industry standard, mirrors the author's Databricks day job; native watermarking + stateful aggregation. |
| Storage / table format | **Delta Lake** (`delta-spark`) | 3.2.0 | ACID, idempotent streaming sink, time travel — the lakehouse standard, Spark 3.5-compatible. |
| Ad-hoc query | **DuckDB** + `deltalake` | 1.0 / 0.18 | Fast local reads of Delta tables for the dashboard without a Spark session. |
| Dashboard | **Streamlit** + **Plotly** | 1.35 / 5.22 | Fast to build, live auto-refresh, interview-demoable. |
| Orchestration (run) | **Docker Compose** | — | One-command, reproducible local cluster. |
| Testing | **pytest** + **pytest-cov** | 8.2 / 5.0 | Unit tests for pure logic; coverage in CI. |
| CI | **GitHub Actions** | — | Lint + tests on every push; green badge. |
| Data source | **StatsBomb Open Data** | — | Free, real, professional event data with xG and coordinates. |

> **RAM note:** Spark + Delta + Kafka in Docker target ~8 GB. If the machine is
> tighter, the stream processor can be swapped to **Bytewax** (pure-Python,
> lightweight) without changing the architecture or the Delta/dashboard layers.

---

## 7. Data

**Source:** StatsBomb Open Data — `github.com/statsbomb/open-data`. Free for
research and public use; this project credits StatsBomb per their user agreement.

**Default match:** 2022 World Cup Final, Argentina 3–3 France (`match_id 3869685`)
— 4,407 events, goes to extra time. Any open-data match works via `MATCH_ID`.

**Event fields used (per event):** `index` (order), `period`, `minute`, `second`,
`type.name`, `team.name`, `possession_team.name`, `player.name`, `location [x,y]`,
and for shots `shot.statsbomb_xg`, `shot.outcome.name`.

---

## 8. Gold metrics — defined explicitly

| Metric | Definition | How it's computed |
| ------ | ---------- | ----------------- |
| **Cumulative xG** | Running total of expected goals per team. | Stateful running sum of `shot.statsbomb_xg` grouped by team, ordered by event time. |
| **Momentum** | Which team is on top *right now*. | Sliding window (last N match-minutes) of weighted attacking actions per team: `Σ(w_shot·shots + w_xg·xg + w_keypass·key_passes + w_boxentry·entries)`. Output swings between the two teams. |
| **Live match state** | Current score + live win probability. | Score = count of `Goal` outcomes per team. Win prob = heuristic logistic function of `(goal_difference, xg_difference, minutes_remaining)`. Documented as heuristic, not ML. |
| **Shot table** | Every shot for the shot map. | Silver shots projected to `(team, player, minute, x, y, xg, outcome)`. |
| **Pass completion** *(stretch)* | Rolling pass accuracy per team. | Windowed ratio of completed passes to attempted passes. |

---

## 9. Repository structure (target)

```
pitchflow/
├── README.md                  # overview, quickstart, architecture
├── docs/
│   └── PROJECT.md             # this spec
├── docker-compose.yml         # Redpanda + console + (Phase 2/3) spark + dashboard
├── requirements.txt
├── Makefile
├── .env.example
├── producer/                  # Phase 1 — replay producer  ✅
│   ├── config.py
│   ├── download.py
│   ├── replay.py
│   └── tests/
├── streaming/                 # Phase 2 — Spark Structured Streaming
│   ├── bronze.py              # Kafka -> Bronze Delta
│   ├── silver.py              # Bronze -> typed Silver
│   ├── gold.py                # Silver -> live aggregates
│   ├── metrics.py             # pure metric functions (unit-tested)
│   └── tests/
├── dashboard/                 # Phase 3 — Streamlit
│   └── app.py
└── .github/workflows/ci.yml
```

---

## 10. Delivery plan — what we will do

| Phase | What we build | Definition of done |
| ----- | ------------- | ------------------ |
| **1. Producer** ✅ | StatsBomb download + replay to Kafka | Events stream to `match.events` in order; tests green. |
| **2. Streaming core** | Spark job: Kafka → Bronze → Silver → Gold (Delta) | Gold tables update live during a replay; watermark + idempotent sink; metric functions unit-tested. |
| **3. Dashboard** | Streamlit live view (xG race, momentum, score+win%, shot map) | Dashboard refreshes live through a full replay. |
| **4. Polish** | Full README, architecture diagram, CI for streaming, observability notes | CI green; repo reads as a finished product. |
| **⭐ Stretch** | AI tactical-commentary layer | On key events, an LLM turns live aggregates into a one-line insight (free Groq tier or local Ollama). |

---

## 11. Testing & CI strategy

- **Pure logic is unit-tested** with no infrastructure: replay timing (done),
  and the Gold metric functions (`metrics.py`) — xG accumulation, momentum
  window, win-prob heuristic — tested on fixed inputs.
- **Streaming I/O is isolated** behind thin wrappers so tests never need a live
  broker or Spark cluster.
- **CI (GitHub Actions)** installs deps and runs `pytest` with coverage on every
  push and PR; a failing test blocks merge.

---

## 12. Risks & mitigations

| Risk | Mitigation |
| ---- | ---------- |
| Spark + Kafka RAM on a laptop | Single-node Spark, capped memory; Bytewax fallback documented. |
| "Live data is paid" | Event replay of free StatsBomb data — deterministic and reproducible. |
| Out-of-order / late events | Event-time processing with watermarks in Spark. |
| Double-counting on re-runs | Idempotent Delta writes + dedup on event `id`. |
| Win-probability over-claiming | Explicitly framed as a heuristic, not a trained model. |
| Data licensing | StatsBomb credited per their user agreement; non-commercial, research use. |

---

## 13. What "senior" looks like in this repo

- A real **event log** (Kafka), not a script writing CSVs.
- **Stateful** stream processing with **watermarks**, not a batch loop.
- An **idempotent** lakehouse sink, not append-and-pray.
- **Pure, tested** metric logic separated from I/O.
- Explicit **scope, requirements, and risks** (this document).
- Honest framing of what is and isn't a model.

---

*Data provided by StatsBomb (github.com/statsbomb/open-data). Not affiliated with
StatsBomb.*
