from aiokafka import AIOKafkaConsumer, ConsumerRecord


async def handle_message(msg: ConsumerRecord, consumer: AIOKafkaConsumer) -> None:
    print(msg)
    await consumer.commit()


async def main():
    consumer = AIOKafkaConsumer(
        "topic",
        bootstrap_servers="localhost:9092",
        enable_auto_commit=False,
        group_id="test",
        auto_offset_reset="earliest",
    )
    async with consumer as consumer:
        async for msg in consumer:
            await handle_message(msg, consumer)


a = handle_message

if __name__ == "__main__":
    import anyio

    anyio.run(main)  # type: ignore
