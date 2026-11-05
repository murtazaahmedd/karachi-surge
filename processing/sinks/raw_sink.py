import os

import psycopg
from confluent_kafka import Consumer, Message
from pydantic import BaseModel, ValidationError

from producers.common.kafka import make_producer
from producers.common.log import get_logger
from producers.common.models import RideRequestEvent, TrafficFlowEvent, WeatherEvent

log = get_logger("raw_sink")

DLQ_TOPIC = "dlq.raw"
ROUTES: dict[str, tuple[type[BaseModel], str]] = {
    "traffic.flow": (TrafficFlowEvent, "raw_traffic_flow"),
    "weather.current": (WeatherEvent, "raw_weather"),
    "rides.requests": (RideRequestEvent, "raw_ride_requests"),
}


def insert_sql(table: str, model: type[BaseModel]) -> str:
    cols = list(model.model_fields)
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) "
        f"VALUES ({', '.join(f'%({c})s' for c in cols)}) ON CONFLICT DO NOTHING"
    )


def route(messages: list[Message]) -> tuple[dict[str, list[dict]], list[tuple[Message, str]]]:
    rows: dict[str, list[dict]] = {table: [] for _, table in ROUTES.values()}
    dead: list[tuple[Message, str]] = []
    for msg in messages:
        model, table = ROUTES[msg.topic()]
        try:
            rows[table].append(model.model_validate_json(msg.value()).model_dump())
        except ValidationError as e:
            dead.append((msg, str(e)[:500]))
    return rows, dead


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        "group.id": "raw-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(ROUTES))
    dlq = make_producer()
    sql = {table: insert_sql(table, model) for model, table in ROUTES.values()}
    with psycopg.connect(os.environ["PG_DSN"]) as conn:
        log.info("consuming %s", list(ROUTES))
        while True:
            batch = consumer.consume(num_messages=500, timeout=1.0)
            for m in batch:
                if m.error():
                    log.error("consumer error: %s", m.error())
            batch = [m for m in batch if not m.error()]
            if not batch:
                continue
            rows, dead = route(batch)
            with conn.cursor() as cur:
                for table, table_rows in rows.items():
                    if table_rows:
                        cur.executemany(sql[table], table_rows)
            conn.commit()
            for msg, err in dead:
                log.warning("dlq topic=%s offset=%d: %s", msg.topic(), msg.offset(), err)
                dlq.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(),
                            headers={"source_topic": msg.topic(), "error": err})
            dlq.flush(10)
            # Commit offsets only after rows are durable; replays are safe due to ON CONFLICT.
            consumer.commit(asynchronous=False)
            log.info("wrote %s, dlq=%d", {t: len(r) for t, r in rows.items() if r}, len(dead))


if __name__ == "__main__":
    main()
