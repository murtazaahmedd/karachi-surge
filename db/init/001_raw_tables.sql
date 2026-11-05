CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS raw_traffic_flow (
    event_id            uuid        NOT NULL,
    city                text        NOT NULL,
    corridor_id         text        NOT NULL,
    h3_cell             text        NOT NULL,
    current_speed_kmh   real        NOT NULL,
    free_flow_speed_kmh real        NOT NULL,
    congestion_ratio    real        NOT NULL,
    confidence          real        NOT NULL,
    event_time          timestamptz NOT NULL,
    ingest_time         timestamptz NOT NULL,
    source              text        NOT NULL,
    PRIMARY KEY (event_id, event_time)
);
SELECT create_hypertable('raw_traffic_flow', 'event_time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS raw_traffic_flow_corridor_time ON raw_traffic_flow (corridor_id, event_time DESC);

CREATE TABLE IF NOT EXISTS raw_weather (
    event_id     uuid        NOT NULL,
    city         text        NOT NULL,
    temp_c       real        NOT NULL,
    precip_mm    real        NOT NULL,
    humidity     real        NOT NULL,
    wind_kmh     real        NOT NULL,
    weather_code integer     NOT NULL,
    event_time   timestamptz NOT NULL,
    ingest_time  timestamptz NOT NULL,
    source       text        NOT NULL,
    PRIMARY KEY (event_id, event_time)
);
SELECT create_hypertable('raw_weather', 'event_time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS raw_weather_city_time ON raw_weather (city, event_time DESC);

CREATE TABLE IF NOT EXISTS raw_ride_requests (
    request_id      uuid        NOT NULL,
    city            text        NOT NULL,
    pickup_h3       text        NOT NULL,
    dropoff_h3      text        NOT NULL,
    est_distance_km real        NOT NULL,
    event_time      timestamptz NOT NULL,
    ingest_time     timestamptz NOT NULL,
    source          text        NOT NULL,
    PRIMARY KEY (request_id, event_time)
);
SELECT create_hypertable('raw_ride_requests', 'event_time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS raw_ride_requests_city_pickup_time ON raw_ride_requests (city, pickup_h3, event_time DESC);
