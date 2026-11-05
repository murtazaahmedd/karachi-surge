# Karachi Surge

Real-time traffic and ride-price engine for Karachi. See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

The pipeline is built on replayed NYC/Boston data (Track A) while live Karachi data collects in the background (Track B).

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
