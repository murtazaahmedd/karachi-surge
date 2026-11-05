import os
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from producers.common.city import load_city
from producers.common.kafka import make_producer, send
from producers.common.log import get_logger
from producers.common.models import City, WeatherEvent
from producers.common.retry import get_json

log = get_logger("weather_poller")

TOPIC = "weather.current"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
VARIABLES = "temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m,weather_code"


def parse_open_meteo(city: City, payload: dict) -> WeatherEvent:
    cur = payload["current"]
    tz = ZoneInfo(city.timezone)
    return WeatherEvent(
        # Open-Meteo updates every 15 min; same observation time -> same id, so repeats dedupe downstream.
        event_id=uuid.uuid5(uuid.NAMESPACE_URL, f"open-meteo:{city.city}:{cur['time']}"),
        city=city.city,
        temp_c=cur["temperature_2m"],
        precip_mm=cur["precipitation"],
        humidity=cur["relative_humidity_2m"],
        wind_kmh=cur["wind_speed_10m"],
        weather_code=cur["weather_code"],
        event_time=datetime.fromisoformat(cur["time"]).replace(tzinfo=tz),
        ingest_time=datetime.now(tz),
        source="open-meteo",
    )


def main() -> None:
    interval = int(os.environ.get("WEATHER_POLL_SECONDS", "900"))
    city = load_city(os.environ.get("CITY", "karachi"))
    params = {
        "latitude": city.weather_point.lat,
        "longitude": city.weather_point.lon,
        "current": VARIABLES,
        "timezone": city.timezone,
    }
    producer = make_producer()
    log.info("city=%s polling weather every %ds", city.city, interval)
    with httpx.Client(timeout=15) as client:
        while True:
            started = time.monotonic()
            try:
                event = parse_open_meteo(city, get_json(client, OPEN_METEO_URL, params))
                send(producer, TOPIC, city.city, event)
                producer.flush(10)
                log.info("sent weather event_time=%s temp=%.1fC precip=%.1fmm", event.event_time, event.temp_c, event.precip_mm)
            except Exception as e:
                log.error("weather poll failed: %s", e)
            time.sleep(max(0.0, interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
