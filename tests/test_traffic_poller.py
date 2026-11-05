import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import h3
import pytest

from producers.common.city import load_city
from producers.traffic_poller import parse_tomtom

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "tomtom_flow.json").read_text())
NOW = datetime(2026, 11, 5, 17, 40, tzinfo=ZoneInfo("Asia/Karachi"))
KARACHI = load_city("karachi")


def test_karachi_city_config():
    assert KARACHI.timezone == "Asia/Karachi"
    assert len(KARACHI.corridors) == 15
    # Every corridor point should sit within ~0.3 deg of the city centre (catches lat/lon typos).
    for c in KARACHI.corridors:
        for p in (c.start, c.end, c.sample_point):
            assert abs(p.lat - KARACHI.weather_point.lat) < 0.3, c.id
            assert abs(p.lon - KARACHI.weather_point.lon) < 0.3, c.id


def test_parse_tomtom():
    corridor = KARACHI.corridors[0]
    e = parse_tomtom(KARACHI, corridor, FIXTURE, NOW)
    assert e.city == "karachi"
    assert e.corridor_id == corridor.id
    assert e.current_speed_kmh == 18.0
    assert e.free_flow_speed_kmh == 55.0
    assert e.congestion_ratio == pytest.approx(18 / 55, abs=1e-4)
    assert e.confidence == 0.92
    assert e.event_time == NOW
    assert e.source == "tomtom"
    assert h3.get_resolution(e.h3_cell) == 8


def test_parse_tomtom_caps_ratio_above_free_flow():
    payload = {"flowSegmentData": {**FIXTURE["flowSegmentData"], "currentSpeed": 70}}
    assert parse_tomtom(KARACHI, KARACHI.corridors[0], payload, NOW).congestion_ratio == 1.0
