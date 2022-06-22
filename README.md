# common-kafka
Library for Apache Kafka common use between services. Basically Setups a custom producer
and a custom consumer

## Installation
### SSH
The following command will install from the main branch. **Be careful**.

`pip install git+ssh://git@github.com/Ourinvest/common-kafka.git`

You can choose manually the branch too, using:

`pip install git+ssh://git@github.com/Ourinvest/common-kafka.git@development`

As this is private repository, correct authentication will be required (a.k.a Token). 

### HTTPS

Create a token 
[here](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token), 
or get one from AWS Secret Manager

```bash
pip install git+https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/Ourinvest/common-kafka.git
```

Or, for specfic branch:

```bash
pip install git+https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/Ourinvest/common-kafka.git@${BRANCH_NAME}
```

## Basic usage

### Producer
This script sends 1000 messages with the actual timestamp and a random integer 0-10000
to a topic named "test", one each second. **Rembember that the script must be running until
all messages are sent, else it will just finish and stop sending**.

```python
import datetime
import time

from random import randrange

from customKafka.producer import KafkaProducerCustom


producer = KafkaProducerCustom()
for _ in range(1000):
    time.sleep(1)
    payload = {
        "time": datetime.datetime.now().isoformat(),
        "value": randrange(0, 10000)
    }
    producer.publish_message(topic='test', value=payload)
    print(payload)
```

### Consumer
Connects to a topic and open a connection which receives all messages, then sends it
to a handler function. 

```python
from customKafka.consumer import KafkaConsumerCustom


def handler(message: dict):
    print(f"{message.topic}:{message.partition}:{message.offset}: key={message.key} value={message.value}")

consumer = KafkaConsumerCustom('test')
for message in consumer.get_consumer():
    handler(message)
```

