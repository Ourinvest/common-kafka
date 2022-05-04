from aiokafka import AIOKafkaConsumer
from aiokafka.errors import (
    KafkaError,
    KafkaTimeoutError,
    OffsetOutOfRangeError,
    RecordTooLargeError,
)
import asyncio
from random import randint
import json
from typing import Any


class AIOConsumer:
    def __init__(self, topic, servers, group_prefix, loop=None):
        group_id = f"{group_prefix}-{randint(0, 10000)}"
        self._loop = loop or asyncio.get_event_loop()
        try:
            self._consumer = AIOKafkaConsumer(
                topic, bootstrap_servers=servers, group_id=group_id
            )
        except OffsetOutOfRangeError:
            print("Cosumption from not valid offset")

        self.topic = topic
        self.group_id = group_id
        self.servers = servers

    async def initialize(self):

        print(
            f"Initializing KafkaConsumer for topic={self.topic}, group_id={self.group_id}"
            f" and using bootstrap servers={self.servers}"
        )

        print(f"Initializing KafkaConsumer...")
        try:
            await self._consumer.start()
            partitions = self._consumer.assignment()
            nr_partitions = len(partitions)

            if nr_partitions != 1:
                print(
                    f"Found {nr_partitions} partitions for topic {self.topic}. Expecting "
                    f"only one, remaining partitions will be ignored!"
                )

                print("Found partitions for the topic!")
            for tp in partitions:

                # get the log_end_offset
                end_offset_dict = await self._consumer.end_offsets([tp])
                end_offset = end_offset_dict[tp]

                if end_offset == 0:
                    print(
                        f"Topic ({self.topic}) has no messages (log_end_offset: "
                        f"{end_offset}), skipping initialization .."
                    )

                    print(
                        f"The messages in the topic is done!"
                        f"Skipping initialization..."
                    )
                    return

                print(
                    f"Found log_end_offset: {end_offset} seeking to {end_offset-1}")
                self._consumer.seek(tp, end_offset - 1)
                msg = await self._consumer.getone()
                print(f"Initializing API with data from msg: {msg}")

                # update the API state
                self._update_state(msg)
                return
        except KafkaError as err:
            print("some kafka error: {}".format(err))

    def _update_state(self, message: Any) -> None:
        value = json.loads(message.value)
        global _state
        _state = value["state"]

    async def consume(self):
        global consumer_task
        try:
            consumer_task = asyncio.create_task(self.send_consumer_message())

        except OffsetOutOfRangeError:
            print("Cosumption from not valid offset")
        except RecordTooLargeError:
            print("Broker has a MessageSet larger than max_partition_fetch_bytes")
        except KafkaTimeoutError:
            print("Cosumption timeout error")

    async def send_consumer_message(self):
        try:
            # consume messages
            async for msg in self._consumer:
                # x = json.loads(msg.value)
                message = str(msg.value, encoding="utf-8")
                print(f"Consumed msg value: {message}")

                # update the API state
                self._update_state(msg)
        except KafkaError as err:
            print("some kafka error: {}".format(err))

    async def stop_app(self):
        try:
            consumer_task.cancel()
            await self._consumer.stop()
        except KafkaError as err:
            print("some kafka error: {}".format(err))
