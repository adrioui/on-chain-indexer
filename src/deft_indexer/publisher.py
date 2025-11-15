from contextlib import contextmanager
from inspect import signature

from kafka import KafkaProducer

from .config import get_settings

settings = get_settings()


@contextmanager
def kafka_producer() -> KafkaProducer:
    kwargs = dict(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: v,
        key_serializer=lambda k: k,
        acks="all",
        linger_ms=20,
    )

    try:
        if "enable_idempotence" in signature(KafkaProducer).parameters:
            kwargs["enable_idempotence"] = True
    except (ValueError, TypeError):
        pass

    producer = KafkaProducer(**kwargs)
    try:
        yield producer
    finally:
        producer.flush()
        producer.close()
