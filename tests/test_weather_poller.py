import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from producers.backfill_weather import parse_archive
from producers.common.city import load_city
from producers.weather_poller import parse_open_meteo

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "open_meteo_current.json").read_text())
KARACHI = load_city("karachi")
PKT = ZoneInfo("Asia/Karachi")


def test_parse_open_meteo():
    e = parse_open_meteo(KARACHI, FIXTURE)
    assert e.city == "karachi"
    assert e.event_time == datetime(2026, 11, 5, 17, 45, tzinfo=PKT)
    assert e.temp_c == 27.3
    assert e.precip_mm == 0.4
    assert e.humidity == 61
    assert e.wind_kmh == 12.6
    assert e.weather_code == 61


def test_same_observation_same_event_id():
    assert parse_open_meteo(KARACHI, FIXTURE).event_id == parse_open_meteo(KARACHI, FIXTURE).event_id


def test_parse_archive_skips_null_hours():
    payload = {"hourly": {
        "time": ["2026-11-01T00:00", "2026-11-01T01:00"],
        "temperature_2m": [24.1, None],
        "precipitation": [0.0, None],
        "relative_humidity_2m": [70, None],
        "wind_speed_10m": [8.2, None],
        "weather_code": [0, None],
    }}
    events = parse_archive(KARACHI, payload)
    assert len(events) == 1
    assert events[0].event_time == datetime(2026, 11, 1, 0, 0, tzinfo=PKT)
    assert events[0].source == "open-meteo-archive"
