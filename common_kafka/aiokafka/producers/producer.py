from aiokafka import AIOKafkaProducer
import asyncio
import json


class AIOProducer:
    def __init__(self, server, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = AIOKafkaProducer(bootstrap_servers=server)
    
    async def close(self):
        await self._producer.stop()

    async def produce(self, topic, value):
        await self._producer.start()
        result = self._loop.create_future()
        value_json = json.dumps(value).encode('utf-8')
        await self._producer.send_and_wait(topic, value_json)
        await self.close()
        return result
