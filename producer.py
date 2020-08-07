from datetime import datetime
from typing import Tuple, Optional
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
from dataclasses import dataclass, asdict
import datetime as dt
import random
import json


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVER = os.environ.get("BOOTSTRAP_SERVER", "localhost:9092")
TOPIC = os.environ.get("TOPIC_MESSAGE", "prices")

random.seed(10)

N_PRICES = 100
MIN_ID, MAX_ID = 100, 200
PRICES = {
    match_id: {"mu": 2.0 + 0.2 * random.random(), "sigma": 0.3 + 0.05 * random.random(), "score": [random.randint(0, 4), random.randint(0, 3)]}
    for match_id in range(MIN_ID, MAX_ID)
}

    


def convert_datetime(o):
    if isinstance(o, dt.datetime):
        return o.__str__()


@dataclass
class Price:
    match_id: int
    price: int
    ts: dt.datetime = dt.datetime.utcnow()
    score: Optional[Tuple[int, int]] = None

    def serialize(self) -> str:
        return json.dumps(asdict(self), default=convert_datetime)

    @classmethod
    def get_random_price(cls):
        match_id = random.randint(MIN_ID, MAX_ID-1)
        price_dict = PRICES[match_id]
        price = random.gauss(mu=price_dict["mu"], sigma=price_dict["sigma"]) + dt.datetime.now().minute
        score = price_dict["score"]
        return cls(match_id=match_id, price=price, score=score, ts=dt.datetime.utcnow())



def callback(err, msg):
    if err is not None:
        log.error(f"Failed to deliver message: {msg.value()}: {err.str()}")
    else:
        log.info(f"Message produced: {msg.key()}:{msg.value()} to partition {msg.partition()}")


def create_topic(conf, topic_name):
    admin = AdminClient(conf)

    if not admin.list_topics().topics:
        log.info(f"Creating new topic {topic_name}...")
        topic = NewTopic(topic=topic_name, num_partitions=2, replication_factor=1)
        res = admin.create_topics([topic], operation_timeout=2)[topic_name].result()


if __name__ == "__main__":
    conf = {"bootstrap.servers": BOOTSTRAP_SERVER}

    create_topic(conf, TOPIC)
    producer = Producer(conf)

    try:
        while True:
            price = Price.get_random_price()
            producer.produce(
                TOPIC,
                value=price.serialize(),
                key=str(price.match_id).encode("utf-8"),
                callback=callback,
            )
            producer.poll(0.001)
            # log.info(f"Messages produced {price}")
    except KeyboardInterrupt:
        log.info(f"Stopping kafka producer")
    finally:
        log.info(f"Waiting for flushing the last messages")
        producer.flush()
