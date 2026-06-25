# pitchflow

> **Real-time football match-event streaming into a Delta lakehouse.**
> Replays real StatsBomb match data as a live Kafka stream, processes it with
> Spark Structured Streaming into a Delta Lake medallion, and serves live
> analytics — xG race, momentum, win probability — on a Streamlit dashboard.
> 100% open-source, runs in the cloud or on a laptop, zero data cost.

![CI](https://github.com/guisefe/pitchflow/actions/workflows/ci.yml/badge.svg)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/guisefe/pitchflow)

---

## Why this project (the detailed why)

**Why the name.** *pitch* (the football field) + *flow* (a continuous data
stream). The name encodes exactly what the project is: football data, flowing in
real time.

**Why streaming.** Almost every data-engineering portfolio is batch — scheduled
jobs over finished tables. Real-time stream processing is the skill that
separates a mid-level engineer from a senior one, and it's in high demand
precisely because it's harder: the data never stops arriving, so you have to
reason about *when* "now" is, what to do with events that show up late, and how
to keep running totals without re-reading everything. pitchflow exists to prove
that skill end to end.

**Why football, and why StatsBomb.** A portfolio piece lands harder when the
domain is real and the author clearly cares about it. StatsBomb publishes
[free, professional-grade event data](https://github.com/statsbomb/open-data) —
every pass, carry and shot, each shot tagged with an expected-goals (xG) value
and pitch coordinates. It's the dataset the serious football-analytics community
actually uses, so building on it signals genuine domain fluency, not a toy CSV.

**Why event replay.** Live football event feeds are paywalled. Instead of paying,
pitchflow *replays* a real, finished match in match-clock order at configurable
speed — a 90-minute game streams in ~90 seconds. This isn't a workaround; replay
is a real production pattern (it's how you backfill and reprocess historical
data through a streaming system), and it makes the whole pipeline **deterministic
and reproducible** — anyone can run the exact same match and get the exact same
result.

**Why a lakehouse / medallion.** The Bronze → Silver → Gold medallion is the
proven lakehouse pattern used in production on platforms like Databricks.
pitchflow applies that *same* structure to a continuous stream instead of a batch
table — so it reuses an architecture that scales while demonstrating it in its
harder, streaming form.

**Why these tools.** Each choice maps to a real-world, in-demand skill: a Kafka
log (Redpanda), stateful stream processing with watermarks (Spark Structured
Streaming), an ACID, idempotent sink (Delta Lake), and a live serving layer
(Streamlit). The full justification for every tool and version is in
[`docs/PROJECT.md`](docs/PROJECT.md).

## Architecture

```
StatsBomb Open Data (real events, free)
        │  download + cache
        ▼
┌──────────────────┐   match-clock replay   ┌──────────────┐
│  Replay Producer │ ─────────────────────▶ │   Redpanda   │   topic: match.events
└──────────────────┘                        │   (Kafka)    │
                                            └──────┬───────┘
                                                   │
                                   ┌───────────────▼────────────────┐
                                   │  Spark Structured Streaming     │   (Phase 2)
                                   │  windowed + stateful aggregates │
                                   └───────────────┬────────────────┘
                                                   │ exactly-once
                                   ┌───────────────▼────────────────┐
                                   │  Delta Lake — Bronze→Silver→Gold│
                                   └───────────────┬────────────────┘
                                                   │
                                   ┌───────────────▼────────────────┐
                                   │  Streamlit live dashboard       │   (Phase 3)
                                   │  xG race · momentum · shot map  │
                                   └─────────────────────────────────┘
```

## Roadmap

| Phase | Deliverable | Status |
| ----- | ----------- | ------ |
| 1 | Replay producer → Redpanda (tested) | ✅ done |
| 2 | Spark Structured Streaming → Delta medallion | 🔜 next |
| 3 | Streamlit live dashboard | ⬜ |
| 4 | Tests, CI, observability, docs polish | ⬜ |
| ⭐ | AI tactical-commentary layer (LLM on live aggregates) | ⬜ stretch |

## Quickstart (Phase 1)

```bash
git clone https://github.com/guisefe/pitchflow.git
cd pitchflow

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # defaults to the 2022 World Cup Final

make up                       # start Redpanda + console (http://localhost:8080)
make download                 # cache the match from StatsBomb open-data
make replay                   # stream it into Kafka in match-clock order
```

Open the console at [localhost:8080](http://localhost:8080), select the
`match.events` topic, and watch the final replay live.

## Develop in GitHub Codespaces

The repo is Codespaces-native via `.devcontainer/` — click **Open in Codespaces**
and the environment builds itself (Python, Docker, deps, forwarded ports).

```bash
make up        # Redpanda + console (port 8080 auto-forwards)
make download  # cache the match
make replay    # stream it
```

**Cost-aware workflow (free tier = 120 core-hours/month):**

- The streaming stack wants a **4-core/16 GB** machine — that burns ~4 core-hours
  per real hour (~30 free hours/month).
- For light work (producer, metrics, tests) pick a **2-core** machine to conserve hours.
- **Stop *and delete*** the codespace when done — a stopped codespace still bills
  storage against the 15 GB free quota.

## Configuration

All via environment variables (`.env`):

| Variable | Default | Meaning |
| -------- | ------- | ------- |
| `MATCH_ID` | `3869685` | StatsBomb match to replay (2022 WC Final) |
| `REPLAY_SPEED` | `60` | Speed multiplier (60 ⇒ 90-min match in ~90s) |
| `MAX_SLEEP_SECONDS` | `3` | Caps real-time gaps (e.g. half-time) |
| `KAFKA_BROKER` | `localhost:19092` | Redpanda external listener |
| `MATCH_TOPIC` | `match.events` | Destination topic |

## Tests

```bash
make test
```

The replay timing logic (event ordering, match-clock conversion, delay scaling)
is pure and unit-tested; the Kafka emit is tested with a fake producer, so no
broker is required in CI.

## Data attribution

Football data is provided by **StatsBomb** via their
[open-data](https://github.com/statsbomb/open-data) release, free for research
and public use. This project is not affiliated with StatsBomb. Per their user
agreement, any analysis derived from this data credits StatsBomb as the source.

## Author

**Guilherme Senis** — Data & AI Engineer
[GitHub](https://github.com/guisefe) · [Email](mailto:gui.senis635@gmail.com)
