from datetime import datetime
from zoneinfo import ZoneInfo

from producers.replay.replay import to_event

# 2026-09-01 00:00 America/New_York == 04:00 UTC
MS = 1788235200000


def test_to_event_keeps_nyc_event_time():
    e = to_event("fhvhv_2026-09:1", MS, "882a100ab5fffff", "882a1000b5fffff", 19.5)
    assert e.city == "nyc"
    assert e.event_time == datetime(2026, 9, 1, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    assert e.pickup_h3 == "882a100ab5fffff"
    assert e.est_distance_km == 19.5


def test_to_event_id_is_deterministic():
    a = to_event("fhvhv_2026-09:1", MS, "a", "b", 1.0)
    b = to_event("fhvhv_2026-09:1", MS, "a", "b", 1.0)
    c = to_event("fhvhv_2026-09:2", MS, "a", "b", 1.0)
    assert a.request_id == b.request_id != c.request_id
