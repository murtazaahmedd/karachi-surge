from unittest.mock import MagicMock

from processing.sinks.raw_sink import insert_sql, route
from producers.common.models import WeatherEvent


def _msg(topic: str, value: bytes) -> MagicMock:
    m = MagicMock()
    m.topic.return_value = topic
    m.value.return_value = value
    return m


def test_insert_sql_is_idempotent():
    sql = insert_sql("raw_weather", WeatherEvent)
    assert sql.startswith("INSERT INTO raw_weather (event_id, city, temp_c")
    assert sql.endswith("ON CONFLICT DO NOTHING")


def test_route_sends_bad_messages_to_dead_list():
    good = (
        b'{"event_id":"6f1c2b9e-1d2a-4c8e-9f3a-2b7d6e5a4c31","city":"karachi","temp_c":27.3,"precip_mm":0,'
        b'"humidity":61,"wind_kmh":12.6,"weather_code":1,"event_time":"2026-11-05T17:45:00+05:00",'
        b'"ingest_time":"2026-11-05T17:45:03+05:00","source":"open-meteo"}'
    )
    rows, dead = route([_msg("weather.current", good), _msg("weather.current", b"not json")])
    assert len(rows["raw_weather"]) == 1
    assert rows["raw_traffic_flow"] == []
    assert rows["raw_ride_requests"] == []
    assert len(dead) == 1
