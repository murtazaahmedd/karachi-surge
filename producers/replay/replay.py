"""Replay a prepared trip Parquet into rides.requests, paced by original event time."""
import argparse
import time
import uuid
from datetime import date, datetime, time as dtime
from zoneinfo import ZoneInfo

import duckdb

from producers.common.kafka import make_producer, send
from producers.common.log import get_logger
from producers.common.models import RideRequestEvent

log = get_logger("replay")

TOPIC = "rides.requests"
CITY = "nyc"
TZ = ZoneInfo("America/New_York")
BATCH = 10_000


def to_event(trip_key: str, event_ms: int, pickup_h3: str, dropoff_h3: str, km: float) -> RideRequestEvent:
    return RideRequestEvent(
        # Deterministic id: replaying the same file twice yields the same ids, so sinks dedupe.
        request_id=uuid.uuid5(uuid.NAMESPACE_URL, f"nyc-tlc:{trip_key}"),
        city=CITY,
        pickup_h3=pickup_h3,
        dropoff_h3=dropoff_h3,
        est_distance_km=km,
        event_time=datetime.fromtimestamp(event_ms / 1000, TZ),
        ingest_time=datetime.now(TZ),
        source="nyc-tlc-fhvhv",
    )


def local_ms(d: date) -> int:
    return int(datetime.combine(d, dtime(), TZ).timestamp() * 1000)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/processed/nyc_fhvhv_2026-09.parquet")
    ap.add_argument("--speed", type=float, default=60.0, help="60 = 1 hour of data per minute")
    ap.add_argument("--start-date", type=date.fromisoformat, default=date(1970, 1, 1))
    ap.add_argument("--end-date", type=date.fromisoformat, default=date(9999, 1, 1), help="exclusive")
    args = ap.parse_args()

    cur = duckdb.connect().execute(
        "SELECT trip_key, epoch_ms(event_time), pickup_h3, dropoff_h3, est_distance_km "
        "FROM read_parquet(?) WHERE epoch_ms(event_time) >= ? AND epoch_ms(event_time) < ? "
        "ORDER BY event_time",
        [args.file, local_ms(args.start_date), local_ms(args.end_date)],
    )
    producer = make_producer()
    first_ms, wall0, sent = None, time.monotonic(), 0
    log.info("replaying %s speed=%sx into %s", args.file, args.speed, TOPIC)
    while rows := cur.fetchmany(BATCH):
        for row in rows:
            if first_ms is None:
                first_ms = row[1]
            ahead = (row[1] - first_ms) / 1000 / args.speed - (time.monotonic() - wall0)
            if ahead > 0:
                time.sleep(ahead)
            event = to_event(*row)
            send(producer, TOPIC, event.pickup_h3, event)
        sent += len(rows)
        elapsed = time.monotonic() - wall0
        log.info("sent=%d sim_time=%s rate=%.0f/s", sent, event.event_time, sent / max(elapsed, 1e-9))
    producer.flush(30)
    log.info("replay done, sent=%d", sent)


if __name__ == "__main__":
    main()
