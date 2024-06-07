from uuid import uuid4

import anyio
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka_retry_lib.retry import retry


@pytest.mark.anyio
async def test_retry_on_retryable_exception():
    topic = str(uuid4())

    @retry(
        bootstrap_servers="127.0.0.1:29092",
        retriable_exceptions=[ValueError],
        max_attempts=10,
    )
    async def bad_handler(msg: ConsumerRecord, consumer: AIOKafkaConsumer) -> None:
        raise ValueError("err")

    @retry(
        bootstrap_servers="127.0.0.1:29092",
        retriable_exceptions=[ValueError],
        max_attempts=10,
    )
    async def good_handler(
        msg: ConsumerRecord,
        consumer: AIOKafkaConsumer,
    ) -> None:
        tp = TopicPartition(msg.topic, msg.partition)
        await consumer.commit({tp: msg.offset + 1})

    async def main():
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers="127.0.0.1:29092",
            enable_auto_commit=False,
            group_id="test-group",
            auto_offset_reset="earliest",
        )
        async with consumer:
            msg_1 = await anext(consumer)
            await bad_handler(msg_1, consumer)
            # check that message has been committed when retried
            another_consumer = AIOKafkaConsumer(
                bootstrap_servers="127.0.0.1:29092",
            )
            async with another_consumer as another_consumer:
                tp = TopicPartition(topic, msg_1.partition)
                another_consumer.assign([tp])
                await another_consumer.seek_to_end()
                committed_offset = await consumer.position(tp)

            assert msg_1.offset + 1 == committed_offset
            assert msg_1.value == b"ciao"
            assert msg_1.headers == (("ciao", b"ciao"),)
            msg_2 = await anext(consumer)
            assert msg_2.value == b"ciao"
            headers = dict(msg_2.headers)
            assert "scheduler-epoch" in headers.keys()
            assert "scheduler-timestamp" in headers.keys()
            assert "scheduler-key" in headers.keys()
            assert headers.get("scheduler-target-topic") == topic.encode()
            assert headers.get("scheduler-target-key") == b"retry"
            assert headers.get("retry-attempt") == b"1"
            assert headers.get("scheduler-topic") == b"schedules"
            assert headers.get("ciao") == b"ciao"
            await bad_handler(msg_2, consumer)
            msg_3 = await anext(consumer)
            assert msg_3.value == b"ciao"
            headers = dict(msg_3.headers)
            assert "scheduler-epoch" in headers.keys()
            assert "scheduler-timestamp" in headers.keys()
            assert "scheduler-key" in headers.keys()
            assert headers.get("scheduler-target-topic") == topic.encode()
            assert headers.get("scheduler-target-key") == b"retry"
            assert headers.get("retry-attempt") == b"2"
            assert headers.get("scheduler-topic") == b"schedules"
            assert headers.get("ciao") == b"ciao"
            await good_handler(msg_3, consumer)
            # there are no more messages
            with pytest.raises(TimeoutError):
                with anyio.fail_after(5):
                    _ = await anext(consumer)

    async with AIOKafkaProducer(bootstrap_servers="localhost:29092") as producer:
        await producer.send(topic, b"ciao", headers=[("ciao", b"ciao")])
    await main()
