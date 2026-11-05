# Karachi Surge — Real-Time Traffic & Ride-Price Engine

**Implementation Plan**

A real-time streaming system that ingests Karachi traffic, weather, fuel, prayer-time and event signals, then computes live congestion and surge-pricing estimates per zone. It also predicts congestion 30 minutes ahead.

---

## 1. Goals

| Goal | What it proves |
|---|---|
| Ingest 5+ live data sources into a stream | Data engineering, API integration |
| Process the streams with windows, joins and watermarks | Streaming systems, event-time semantics |
| Compute live surge pricing per H3 zone | Real-time business logic |
| Predict corridor congestion for the next 30 minutes | Real-time / online ML |
| Show a live map dashboard plus pipeline monitoring | Full-stack delivery, observability |
| Run everything locally with Docker Compose | Reproducible, production-style setup |

**Out of scope:** scraping Careem, Uber, inDrive or Yango. They have no public APIs and scraping breaks their terms. Ride demand and prices are **simulated** from real signals.

---

## 2. Data Sources

| Signal | Source | Auth | Poll rate | Topic |
|---|---|---|---|---|
| Traffic speed / flow | TomTom Traffic Flow API (alternative: HERE Traffic) | Free dev key | 2–5 min | `traffic.flow` |
| Route travel time | Google Routes API (free credit) / OSRM for free-flow baseline | Optional key | 10 min | `traffic.routes` |
| Weather | Open-Meteo | None | 15 min | `weather.current` |
| Fuel prices | OGRA / PSO notifications (light scrape) | None | Daily | `ref.fuel` |
| Prayer times | Aladhan API (city=Karachi) | None | Daily | `ref.prayer` |
| Public holidays | Nager.Date / manual `holidays_pk.csv` | None | Yearly | `ref.holidays` |
| Events | Manual `events.csv` (PSL, concerts, rallies) + news RSS keyword match ("road closed", "rally", "protest") | None | Hourly | `events.city` |
| Zones / map | OpenStreetMap + Uber H3 (resolution 7–8) | None | Once | static |
| Ride requests | Custom simulator | — | Continuous | `rides.requests` |
| Driver supply | Custom simulator | — | Continuous | `rides.drivers` |

> Check each provider's current free-tier limits and terms before you start.

### 2.1 Historical datasets (bootstrap and replay)

There is no public Karachi traffic time series or ride-price dataset. Careem and inDrive data is private. The project therefore **develops on real datasets from other cities, replayed as live streams**, while Karachi data collects in the background.

| Dataset | What it gives | Use |
|---|---|---|
| **NYC TLC Trip Records** (taxi + Uber/Lyft "FHV" trips, Parquet) | Billions of real trips with pickup/dropoff zones, timestamps and fares | Main **replay stream** for `rides.requests`; demand forecasting; load testing |
| **Uber & Lyft Boston** (Kaggle, `rideshare_kaggle.csv`, ~693k rows, 57 cols incl. weather) | Real prices + `surge_multiplier` + hourly weather | Train/validate the **surge model**. Use the Lyft rows, because Uber's surge is always 1. |
| **Uber & Lyft Cab Prices** (Kaggle, `cab_rides.csv` + `weather.csv`) | Prices every 5 min + hourly weather | Second surge validation set |
| **Chicago Taxi Trips** (city open data / BigQuery public dataset) | Real trips with timestamps and fares | Optional second replay city |
| **Open-Meteo Historical API** (Karachi) | Years of real hourly Karachi weather, no key | Karachi weather features from day 1 |
| **Pakistan Traffic Accidents** (Kaggle) | Yearly accident stats | Context only, for the README |

**Replay producer** (`producers/replay/replay.py`):
- Reads Parquet/CSV sorted by event time
- Publishes to the same topic schemas as the live producers
- `--speed 60` means 1 hour of data streams in 1 minute
- `--start-date` / `--end-date` select a slice; chaos flags (`--late-pct`, `--dup-pct`) still apply
- Maps source zones (NYC taxi zones) to H3 cells, so downstream code stays the same for every city

**City abstraction:** `config/cities/{nyc,boston,karachi}.yaml` holds the zones, the corridors and which producers run. Switching cities is a config change, not a code change.

### Monitored corridors (~15)
Shahrah-e-Faisal · MA Jinnah Road · Lyari Expressway · University Road · II Chundrigar Road · Clifton / Do Talwar · Saddar · Tariq Road · Gulshan-e-Iqbal (Rashid Minhas Rd) · Airport (Star Gate) · Korangi Road · DHA Phase 6 (Khayaban-e-Ittehad) · Nazimabad · North Karachi · Karsaz

Store them in `config/corridors.yaml` with start/end coordinates and a TomTom sample point.

---

## 3. Architecture

```
┌───────────────── PRODUCERS (Python) ─────────────────┐
│ traffic_poller   weather_poller   ref_pollers        │
│ events_poller    ride_simulator   driver_simulator   │
└───────────────────────────┬──────────────────────────┘
                            ▼
              ┌──────── Redpanda (Kafka API) ────────┐
              │ topics + Schema Registry + Console   │
              └───────────────────┬──────────────────┘
                                  ▼
      ┌──────────── STREAM PROCESSING (Bytewax / PyFlink) ────────────┐
      │ 1. clean + dedupe + event-time watermarks                     │
      │ 2. window aggs per zone (5-min tumbling, 15-min sliding)      │
      │ 3. join rides ⨝ drivers ⨝ traffic ⨝ weather ⨝ events          │
      │ 4. surge calculator                                           │
      │ 5. online ML scoring (River) → congestion forecast            │
      └───────┬────────────────────┬───────────────────┬──────────────┘
              ▼                    ▼                   ▼
        TimescaleDB              Redis           dead-letter topic
        (history)            (latest/zone)
              │                    │
              └──────┬─────────────┘
                     ▼
          FastAPI (REST + WebSocket)
                     ▼
     Web map (deck.gl / H3 heatmap)  ·  Grafana  ·  Prometheus
```

---

## 4. Repository Structure

```
karachi-surge/
├── docker-compose.yml
├── .env.example
├── Makefile
├── config/
│   ├── corridors.yaml
│   ├── zones.yaml
│   ├── events.csv
│   └── holidays_pk.csv
├── schemas/                 # Avro / JSON schemas per topic
├── producers/
│   ├── common/              # kafka client, retry, logging
│   ├── traffic_poller.py
│   ├── weather_poller.py
│   ├── ref_pollers.py       # fuel, prayer, holidays
│   ├── events_poller.py
│   └── simulator/
│       ├── demand.py
│       └── drivers.py
├── processing/
│   ├── flows/               # bytewax dataflows
│   │   ├── zone_aggregates.py
│   │   ├── surge.py
│   │   └── forecast.py
│   └── features/
├── ml/
│   ├── notebooks/
│   ├── train_offline.py
│   ├── online_model.py
│   └── evaluate.py
├── api/
│   └── main.py              # FastAPI + WebSocket
├── web/                     # map dashboard
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/dashboards/
├── tests/
└── docs/
    ├── architecture.png
    └── product_brief.md
```

---

## 5. Event Schemas

**`traffic.flow`**
```json
{
  "event_id": "uuid",
  "corridor_id": "shahrah_e_faisal",
  "h3_cell": "872f5a...",
  "current_speed_kmh": 18.4,
  "free_flow_speed_kmh": 55.0,
  "congestion_ratio": 0.33,
  "confidence": 0.92,
  "event_time": "2026-11-05T17:40:00+05:00",
  "ingest_time": "2026-11-05T17:40:03+05:00",
  "source": "tomtom"
}
```

**`weather.current`**: `temp_c`, `precip_mm`, `humidity`, `wind_kmh`, `weather_code`, `event_time`

**`rides.requests`**: `request_id`, `pickup_h3`, `dropoff_h3`, `est_distance_km`, `event_time`

**`rides.drivers`**: `driver_id`, `h3_cell`, `status` (idle/busy), `event_time`

**`zone.surge`** (output): `h3_cell`, `window_start`, `window_end`, `demand`, `supply`, `ratio`, `congestion`, `rain_flag`, `surge_multiplier`, `est_fare_pkr`

---

## 6. Ride Simulator Design

Demand per zone per minute follows a Poisson rate λ:

```
λ = base_zone_rate
  × hour_of_day_curve[h]          # morning/evening peaks
  × weekday_factor[d]             # Friday Jummah dip then spike, Sunday low
  × (1 + 0.6 · rain_flag)         # rain pushes demand way up in Karachi
  × (1 + event_boost)             # PSL match near National Stadium, etc.
  × (1 + 0.3 · (1 − congestion))  # inverse effect: bad traffic → fewer completions
  × fuel_factor                   # fuel price hike → slight demand shift
```

Driver supply: a pool of N drivers moving between neighbouring H3 cells. Supply drops in rain and when congestion is heavy.

Chaos options for learning:
- `--late-pct 5`: send 5% of events 1–10 minutes late
- `--dup-pct 1`: send duplicate events
- `--rate-multiplier 100`: load testing

---

## 7. Surge Formula

```
ratio      = demand / max(supply, 1)
surge_raw  = 1 + α·max(ratio − 1, 0) + β·(1 − congestion_ratio) + γ·rain_flag
surge      = clip(EMA(surge_raw), 1.0, 3.0)    # smoothing stops flicker
fare_pkr   = (base_fare + per_km·distance + per_min·duration) × surge
fuel adj   : per_km scales with the current petrol price vs a reference price
```

Start with α=0.5, β=0.4, γ=0.3 and tune them in a config file.

---

## 8. ML Plan: 30-Minute Congestion Forecast

**Target:** `congestion_ratio` per corridor at t + 30 min

**Features**
- Lags: t-5, t-15, t-30, t-60 min, plus the same slot last week
- Rolling mean/std over 15 and 60 min
- Hour, weekday, holiday flag, minutes to/from Jummah and Maghrib
- Rain now and rain in the last hour, temperature
- Event within 3 km flag
- Congestion on neighbouring corridors (spatial lag)

**Model ladder**
1. Baseline: same slot last week
2. Offline LightGBM, trained on collected history
3. Online model: River (`HoeffdingTreeRegressor` / `SNARIMAX`) that learns from each new event
4. Compare MAE/MAPE on a rolling basis on a Grafana panel

**Monitoring:** feature drift (PSI), prediction error over time, model version tag on every prediction

---

## 9. Week-by-Week Plan

> **Two tracks run in parallel:** Track A builds and tests the pipeline on **replayed NYC/Boston data**. Track B collects **live Karachi data** in the background from Week 1. They merge in Week 6.

### Week 1: Foundations + data in hand
- [ ] `docker-compose.yml`: Redpanda, Redpanda Console, TimescaleDB, Redis
- [ ] Download NYC TLC (1–3 months of Parquet) + Boston Uber/Lyft from Kaggle into `data/raw/` (git-ignored)
- [ ] `replay.py` publishing NYC trips to `rides.requests` at configurable speed
- [ ] **Track B:** get a free TomTom/HERE key, write `config/cities/karachi.yaml`, start `traffic_poller.py` + `weather_poller.py`
- [ ] Backfill Karachi weather history from the Open-Meteo Historical API
- [ ] Raw sink to TimescaleDB
- [ ] **Leave the Karachi pollers running 24/7** (old laptop, or a free-tier VM)

**Done when:** NYC replay events and live Karachi events both show in the Console and land in TimescaleDB.

### Week 2: Reference data + supply simulator + city abstraction
- [ ] City config system (`nyc`, `boston`, `karachi`), with NYC taxi zones mapped to H3
- [ ] Historical NYC weather (Open-Meteo) replayed in sync with trips
- [ ] Driver-supply simulator. Replayed trips give the demand; supply is simulated, since no dataset has it.
- [ ] Karachi reference producers: fuel, prayer, holidays, events
- [ ] Karachi demand simulator (Section 6). Calibrate its hourly/rain curves from patterns found in the NYC data.
- [ ] Schema Registry plus schema validation
- [ ] Late/duplicate chaos flags in replay and the simulators

**Done when:** a full NYC day replays in minutes with weather and supply, and the Karachi topics keep flowing.

### Week 3: Stream processing
- [ ] Bytewax dataflow: dedupe → watermark → 5-min tumbling window per zone
- [ ] 15-min sliding window for smoothing
- [ ] Stream joins (rides + drivers + traffic + weather + events)
- [ ] Surge calculator → `zone.surge` topic
- [ ] Late-data handling and a dead-letter topic
- [ ] Unit tests for the windowing logic

**Done when:** `zone.surge` emits correct multipliers and late events are handled correctly.

### Week 4: Serving + dashboard
- [ ] Sinks: TimescaleDB (history), Redis (latest per zone)
- [ ] FastAPI endpoints: `/zones`, `/zones/{h3}/surge`, `/corridors/{id}/history`, WebSocket `/live`
- [ ] Web map: H3 hex heatmap of surge with corridor congestion lines (deck.gl)
- [ ] Grafana: throughput, consumer lag, per-zone surge

**Done when:** the map updates live without a refresh.

### Week 5: Real-time ML
- [ ] EDA on NYC + Boston (demand peaks, rain effect, real surge drivers)
- [ ] **Surge model** trained on the Boston Lyft data (real `surge_multiplier`); compare it with the hand-tuned formula in Section 7
- [ ] **Demand forecast** (next 30 min per zone) trained on NYC history
- [ ] **Congestion forecast** on Karachi traffic history collected so far (4+ weeks by now)
- [ ] Baseline + LightGBM offline training and evaluation
- [ ] River online model inside the stream
- [ ] Forecast topic + API endpoint + dashboard layer
- [ ] Drift and error monitoring panels

**Done when:** the dashboard shows the 30-minute forecast and MAE beats the baseline.

### Week 6: Switch to Karachi + production polish
- [ ] Run the pipeline with `CITY=karachi`: live traffic + weather + simulated demand
- [ ] Dashboard city toggle (NYC replay vs Karachi live)
- [ ] Document the "built on NYC, deployed on Karachi" story in the README
- [ ] Prometheus metrics in all services, alert rules (lag, poller failures)
- [ ] Retries with backoff, idempotent producers, replay from offsets
- [ ] Load test: simulator at 10k events/sec, record throughput and latency
- [ ] CI (GitHub Actions): lint, tests, docker build
- [ ] README: architecture diagram, demo GIF, setup in 3 commands
- [ ] `docs/product_brief.md`: target user, problem, key metrics, tradeoffs, what you'd do next

**Done when:** a stranger can run `make up` and see the live map in under 5 minutes.

---

## 10. Tech Stack

| Layer | Choice | Alternative |
|---|---|---|
| Language | Python 3.11+ | — |
| Broker | Redpanda | Apache Kafka |
| Stream processing | Bytewax | PyFlink / Apache Flink |
| Time-series DB | TimescaleDB | ClickHouse |
| Cache | Redis | — |
| Geo indexing | H3 | S2 |
| ML | LightGBM, River | Prophet, statsforecast |
| API | FastAPI | — |
| Frontend | deck.gl + Vite | Kepler.gl, Streamlit (fastest) |
| Monitoring | Prometheus + Grafana | — |
| Infra | Docker Compose | k3d later |

---

## 11. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Free-tier API rate limits | Poll fewer corridors, cache responses, rotate between TomTom and HERE |
| Laptop off → gaps in data | Run pollers on a small free-tier cloud VM; mark gaps explicitly |
| No Karachi ride data | Develop on real NYC/Boston data; calibrate the Karachi simulator from it; be explicit about this in the README |
| Large NYC files | Use 1–3 months of Parquet; DuckDB for preprocessing |
| Simulated rides look fake | Document assumptions; calibrate curves from public reports; be transparent in the README |
| Flink learning curve | Start with Bytewax, port one flow to Flink later as a stretch goal |
| Scope creep | Each week's "Done when" is the gate before moving on |

---

## 12. Stretch Goals
- Port the surge flow to Apache Flink and compare
- Deploy to GCP (Pub/Sub + Dataflow) or k3d with Helm
- Telegram/WhatsApp bot: "What's surge in Clifton right now?"
- Add a local LLM (Ollama) later for natural-language questions over the data
- Public read-only demo dashboard

---

## 13. Learning Checklist
- [ ] Kafka concepts: topics, partitions, consumer groups, offsets
- [ ] Event time vs processing time, watermarks, late data
- [ ] Windowing: tumbling, sliding, session
- [ ] Stream-stream and stream-table joins
- [ ] Exactly-once vs at-least-once delivery, idempotency
- [ ] Geospatial indexing with H3
- [ ] Online vs batch ML, concept drift
- [ ] Observability for data pipelines
