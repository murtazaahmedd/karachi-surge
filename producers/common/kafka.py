import os

from confluent_kafka import Message, Producer
from pydantic import BaseModel

from producers.common.log import get_logger

log = get_logger(__name__)


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        "enable.idempotence": True,
        "linger.ms": 50,
    })


def _on_delivery(err, msg: Message) -> None:
    if err is not None:
        log.error("delivery failed topic=%s key=%s: %s", msg.topic(), msg.key(), err)


def send(producer: Producer, topic: str, key: str, event: BaseModel) -> None:
    producer.produce(topic, key=key, value=event.model_dump_json(), on_delivery=_on_delivery)
    producer.poll(0)
