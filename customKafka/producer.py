from typing import List

import boto3
import json

from kafka import KafkaProducer


class KafkaProducerCustom:
    def __init__(self, cluster_arn: str = "", bootstrap_servers: List[str] = ["localhost:9092"], simulate: bool = True):
        self.__simulate = simulate
        self.__bootstrap_brokers = bootstrap_servers
        if not self.__simulate:
            self.__kafka_client = boto3.client("kafka")
            self.__bootstrap_brokers = self.__kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)[
                'BootstrapBrokerString']
        self.__producer = KafkaProducer(bootstrap_servers=self.__bootstrap_brokers,
                                        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                                        security_protocol="PLAINTEXT")

    def close_producer(self):
        self.__producer.close()

    def status(self):
        return self.__producer.bootstrap_connected()

    def publish_message(self, topic: str, value: dict):
        try:
            self.__producer.send(topic=topic, value=value)
            return True
        except Exception as e:
            print(e)
            return False


