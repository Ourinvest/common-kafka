from typing import List

import boto3
import json
from kafka import KafkaConsumer


class KafkaConsumerCustom:
    def __init__(self, topic: str, group_id: str = None, cluster_arn: str = "",
                 bootstrap_servers: List[str] = ["localhost:9092"],
                 simulate: bool = True):
        self.__simulate = simulate
        self.__bootstrap_brokers = bootstrap_servers
        if not self.__simulate:
            self.__kafka_client = boto3.client("kafka")
            self.__bootstrap_brokers = self.__kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)[
                'BootstrapBrokerString']
        self.__consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.__bootstrap_brokers,
            group_id=group_id,
            security_protocol="PLAINTEXT",
            value_deserializer=lambda x: json.loads(x),
        )

    def get_consumer(self) -> KafkaConsumer:
        return self.__consumer

    def close(self) -> None:
        self.__consumer.close()

