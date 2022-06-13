import datetime
import time

from random import randrange

from common_kafka.kafka_python.producer import KafkaProducerCustom
from common_kafka.kafka_python.consumer import KafkaConsumerCustom


def producer_example():
    producer = KafkaProducerCustom()
    for _ in range(1000):
        time.sleep(1)
        payload = {
            "time": datetime.datetime.now().isoformat(),
            "value": randrange(0, 10000)
        }
        producer.publish_message(topic='test', value=payload)
        print(payload)


def consumer_example():
    def handler(message: dict):
        print(f"{message.topic}:{message.partition}:{message.offset}: key={message.key} value={message.value}")

    consumer = KafkaConsumerCustom('test')
    for message in consumer.get_consumer():
        handler(message)