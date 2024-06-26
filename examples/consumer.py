import logging
import random

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka_retry_lib.retry import retry

logging.basicConfig(level=logging.INFO)


@retry(
    bootstrap_servers="127.0.0.1:29092",
    retriable_exceptions=[ValueError],
    max_attempts=10,
)
async def handle_message(msg: ConsumerRecord, consumer: AIOKafkaConsumer) -> None:
    # raise ValueError("unable to handle this")
    if random.random() > 0.8:
        raise ValueError("ValueError aaaaaaah panic")
    if random.random() > 0.8:
        raise TypeError("TypeError aaaaaaah panic")
    print("ah no error, nice", msg.value)
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

    anyio.run(main)  # type: ignore
