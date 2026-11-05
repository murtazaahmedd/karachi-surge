import os
import sys
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import h3
import httpx

from producers.common.city import load_city
from producers.common.kafka import make_producer, send
from producers.common.log import get_logger
from producers.common.models import City, Corridor, TrafficFlowEvent
from producers.common.retry import get_json

log = get_logger("traffic_poller")

TOPIC = "traffic.flow"
TOMTOM_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
H3_RES = 8


def parse_tomtom(city: City, corridor: Corridor, payload: dict, now: datetime) -> TrafficFlowEvent:
    seg = payload["flowSegmentData"]
    current, free_flow = float(seg["currentSpeed"]), float(seg["freeFlowSpeed"])
    return TrafficFlowEvent(
        event_id=uuid.uuid4(),
        city=city.city,
        corridor_id=corridor.id,
        h3_cell=h3.latlng_to_cell(corridor.sample_point.lat, corridor.sample_point.lon, H3_RES),
        current_speed_kmh=current,
        free_flow_speed_kmh=free_flow,
        # Speeds above free flow happen; cap so 1 - ratio stays a valid congestion score.
        congestion_ratio=round(min(current / free_flow, 1.0), 4),
        confidence=float(seg["confidence"]),
        event_time=now,
        ingest_time=datetime.now(now.tzinfo),
        source="tomtom",
    )


def poll_once(client: httpx.Client, producer, key: str, city: City) -> int:
    now = datetime.now(ZoneInfo(city.timezone)).replace(microsecond=0)
    sent = 0
    for c in city.corridors:
        params = {"point": f"{c.sample_point.lat},{c.sample_point.lon}", "unit": "KMPH", "key": key}
        try:
            event = parse_tomtom(city, c, get_json(client, TOMTOM_URL, params), now)
        except Exception as e:
            log.error("corridor=%s poll failed: %s", c.id, e)
            continue
        send(producer, TOPIC, c.id, event)
        sent += 1
    producer.flush(10)
    return sent


def main() -> None:
    key = os.environ.get("TOMTOM_API_KEY")
    if not key:
        log.error("TOMTOM_API_KEY missing: get a free key at developer.tomtom.com and set it in .env")
        sys.exit(1)
    interval = int(os.environ.get("TRAFFIC_POLL_SECONDS", "600"))
    city = load_city(os.environ.get("CITY", "karachi"))
    producer = make_producer()
    log.info("city=%s polling %d corridors every %ds", city.city, len(city.corridors), interval)
    with httpx.Client(timeout=15) as client:
        while True:
            started = time.monotonic()
            sent = poll_once(client, producer, key, city)
            log.info("sent %d/%d traffic events", sent, len(city.corridors))
            time.sleep(max(0.0, interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
