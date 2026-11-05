"""Backfill hourly city weather history from the Open-Meteo Archive API straight into raw_weather."""
import argparse
import os
import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import psycopg

from producers.common.city import load_city
from producers.common.log import get_logger
from producers.common.models import City, WeatherEvent
from producers.common.retry import get_json
from producers.weather_poller import VARIABLES
from processing.sinks.raw_sink import insert_sql

log = get_logger("backfill_weather")

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def parse_archive(city: City, payload: dict) -> list[WeatherEvent]:
    hourly, tz, now = payload["hourly"], ZoneInfo(city.timezone), datetime.now(ZoneInfo(city.timezone))
    events = []
    for i, t in enumerate(hourly["time"]):
        # The archive lags a few days; the most recent hours come back as nulls.
        if hourly["temperature_2m"][i] is None:
            continue
        events.append(WeatherEvent(
            event_id=uuid.uuid5(uuid.NAMESPACE_URL, f"open-meteo-archive:{city.city}:{t}"),
            city=city.city,
            temp_c=hourly["temperature_2m"][i],
            precip_mm=hourly["precipitation"][i],
            humidity=hourly["relative_humidity_2m"][i],
            wind_kmh=hourly["wind_speed_10m"][i],
            weather_code=hourly["weather_code"][i],
            event_time=datetime.fromisoformat(t).replace(tzinfo=tz),
            ingest_time=now,
            source="open-meteo-archive",
        ))
    return events


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="karachi")
    ap.add_argument("--start", type=date.fromisoformat, default=date(2022, 1, 1))
    ap.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    args = ap.parse_args()
    city = load_city(args.city)
    params = {
        "latitude": city.weather_point.lat,
        "longitude": city.weather_point.lon,
        "start_date": args.start.isoformat(),
        "end_date": args.end.isoformat(),
        "hourly": VARIABLES,
        "timezone": city.timezone,
    }
    with httpx.Client(timeout=120) as client:
        events = parse_archive(city, get_json(client, ARCHIVE_URL, params))
    with psycopg.connect(os.environ.get("PG_DSN", "postgresql://surge:surge@localhost:5433/surge")) as conn:
        conn.cursor().executemany(insert_sql("raw_weather", WeatherEvent), [e.model_dump() for e in events])
    log.info("city=%s backfilled %d hourly rows %s .. %s", city.city, len(events), args.start, args.end)


if __name__ == "__main__":
    main()
