# kafka-retry-lib

Implement retries using backoff strategy. Built using the awesome [`kafka-message-scheduler`](https://github.com/etf1/kafka-message-scheduler) service.

## Documents

- <https://www.uber.com/en-IT/blog/reliable-reprocessing/>
- <https://medium.com/naukri-engineering/retry-mechanism-and-delay-queues-in-apache-kafka-528a6524f722>

## Basic Usage

```python
import random

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka_retry_lib.retry import retry


@retry(
    bootstrap_servers="127.0.0.1:29092",
    retriable_exceptions=[ValueError],
    max_attempts=10,
)
async def handle_message(msg: ConsumerRecord, consumer: AIOKafkaConsumer) -> None:
    if random.random() > 0.8:
        raise ValueError("ValueError aaaaaaah panic")
    if random.random() > 0.8:
        raise TypeError("TypeError aaaaaaah panic")
    tp = TopicPartition(msg.topic, msg.partition)
    await consumer.commit({tp: msg.offset + 1})


async def main():
    consumer = AIOKafkaConsumer(
        "topic",
        bootstrap_servers="127.0.0.1:29092",
        enable_auto_commit=False,
        group_id="test-group",
        auto_offset_reset="earliest",
    )
    async with consumer as consumer:
        async for msg in consumer:
            await handle_message(msg, consumer)


if __name__ == "__main__":
    import anyio

    anyio.run(main)
```

All happens in the `retry` decorator.

If the handler fails, the `retry` decorator will catch the exception and sends a new message to the Kafka scheduler topic, requesting a re-send after a duration defined by the `BackoffStrategy`.

Only accepted exceptions (`retriable_exceptions`) will end up retried, all others will be sent to a Dead Letter Topic (`dlt`) for human intervention.

After the defined `max_attempts`, retried messages will end up in the `dlt`.
