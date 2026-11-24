# Karachi Surge

**A real-time streaming engine for Karachi traffic and ride prices.** It ingests live traffic and weather
signals into Kafka (Redpanda), lands them in TimescaleDB, and is being built out to compute surge pricing
per H3 zone and forecast corridor congestion 30 minutes ahead.

There is no public Karachi ride or traffic time series, so the pipeline is developed on **real NYC and
Boston trip data replayed as live streams** (Track A) while **live Karachi data collects in the
background** (Track B). Full design: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

## Architecture

```text
 TomTom traffic API ──► traffic_poller ──┐
 Open-Meteo weather ──► weather_poller ──┼──► Redpanda (Kafka API) ──► raw_sink ──► TimescaleDB hypertables
 NYC TLC trips ───────► replay (H3) ─────┘     traffic.flow                │          (idempotent inserts,
                                               weather.current             │           offsets committed
                                               rides.requests              └──► dlq.raw  after DB commit)

 Planned: stream processing (windows, joins, watermarks) ─► surge calculator ─► online ML (River)
          ─► FastAPI (REST + WebSocket) ─► live H3 map dashboard · Grafana
```

## Status

| Stage | State |
|---|---|
| Docker Compose stack: Redpanda, Console, TimescaleDB, Redis | Done |
| Live Karachi pollers (TomTom traffic, Open-Meteo weather) with retries | Done |
| NYC TLC replay producer with zone-to-H3 mapping and speed control | Done |
| Karachi weather backfill since 2022 | Done |
| Raw sink to TimescaleDB with validation and dead-letter topic | Done |
| Stream processing, surge pricing, congestion forecast, dashboard | Planned |

## Tech stack

Python · Redpanda (Kafka) · TimescaleDB (PostgreSQL) · Redis · H3 · Docker Compose · uv · pytest

## Engineering highlights

- **Exactly-once-style landing:** inserts use `ON CONFLICT DO NOTHING` and Kafka offsets are committed only
  after the database commit, so a crash never loses or duplicates an event.
- **Bad messages never block the stream:** anything that fails schema validation goes to `dlq.raw`.
- **City abstraction:** corridors and bounds live in `config/cities/karachi.yaml`; replayed NYC zones are
  mapped to H3 cells so downstream code is the same for every city.
- **Replay with real event time:** `--speed 60` streams one hour of trips per minute over a chosen date range.

## Setup

Requires Docker, `uv` and `make`.

```bash
cp .env.example .env          # add TOMTOM_API_KEY (free at developer.tomtom.com)
make up                       # Redpanda, Console, TimescaleDB, Redis, pollers, raw sink
make test
```

- Redpanda Console: http://localhost:8080
- TimescaleDB: `make psql` (or `localhost:5433`, user/pass/db `surge`)
- Kafka from the host: `localhost:19092`

Without a TomTom key the traffic poller exits with an error and restarts; everything else runs.

## Data

```bash
make prepare                  # download NYC TLC FHVHV (NYC_MONTH=2026-09) + taxi zones, map zones to H3
make replay ARGS="--speed 60 --start-date 2026-09-01 --end-date 2026-09-02"
make backfill                 # Karachi hourly weather since 2022 from the Open-Meteo Archive API
```

Boston Uber/Lyft data comes from Kaggle and is downloaded manually into `data/raw/boston/`:

- `rideshare_kaggle.csv` from "Uber and Lyft Dataset Boston, MA"
- `cab_rides.csv` and `weather.csv` from "Uber & Lyft Cab prices"

## Topics

| Topic | Producer |
|---|---|
| `traffic.flow` | `producers/traffic_poller.py` (TomTom, Karachi corridors in `config/cities/karachi.yaml`) |
| `weather.current` | `producers/weather_poller.py` (Open-Meteo) |
| `rides.requests` | `producers/replay/replay.py` (NYC TLC replay) |
| `dlq.raw` | `processing/sinks/raw_sink.py` (messages that fail validation) |

`processing/sinks/raw_sink.py` writes all three topics into TimescaleDB hypertables. The inserts are idempotent (`ON CONFLICT DO NOTHING`), and offsets are committed only after the DB commit.
