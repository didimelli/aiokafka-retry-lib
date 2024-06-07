import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka_retry_lib.retry import retry


@pytest.mark.anyio
async def test_retry_on_retryable_exception():
    @retry(
        bootstrap_servers="127.0.0.1:29092",
        retriable_exceptions=[ValueError],
        max_attempts=10,
    )
    async def handler(msg: ConsumerRecord, consumer: AIOKafkaConsumer, idx: int) -> int:
        if idx == 1:
            raise ValueError("err")
        tp = TopicPartition(msg.topic, msg.partition)
        await consumer.commit({tp: msg.offset + 1})
        return msg.offset

    async def main():
        consumer = AIOKafkaConsumer(
            "topic",
            bootstrap_servers="127.0.0.1:29092",
            enable_auto_commit=False,
            group_id="test-group",
            auto_offset_reset="earliest",
        )
        async with consumer:
            msg_1 = await anext(consumer)
            await handler(msg_1, consumer, 1)
            # check that message has been committed when retried
            another_consumer = AIOKafkaConsumer(
                bootstrap_servers="127.0.0.1:29092",
            )
            async with another_consumer as another_consumer:
                tp = TopicPartition("topic", msg_1.partition)
                another_consumer.assign([tp])
                await another_consumer.seek_to_end()
                committed_offset = await consumer.position(tp)

            assert msg_1.offset + 1 == committed_offset
            assert msg_1.value == b"ciao"
            assert msg_1.headers == (("ciao", b"ciao"),)
            msg_2 = await anext(consumer)
            print(msg_1, msg_2)
            assert msg_2.value == b"ciao"
            assert "scheduler-epoch" in msg_2.headers[0]
            assert msg_2.headers[1] == ("scheduler-target-topic", b"topic")
            assert msg_2.headers[2] == ("scheduler-target-key", b"retry")
            assert msg_2.headers[3] == ("retry-attempt", b"1")
            assert msg_2.headers[4] == ("ciao", b"ciao")
            assert "scheduler-timestamp" in msg_2.headers[5]
            assert "scheduler-key" in msg_2.headers[6]
            assert msg_2.headers[7] == ("scheduler-topic", b"schedules")
            second_offset = await handler(msg_2, consumer, 2)
            assert msg_1 != second_offset

    async with AIOKafkaProducer(bootstrap_servers="localhost:29092") as producer:
        await producer.send("topic", b"ciao", headers=[("ciao", b"ciao")])
    await main()
