from aiokafka import AIOKafkaProducer


async def main():
    async with AIOKafkaProducer(bootstrap_servers="localhost:29092") as producer:
        await producer.send("topic", b"ciao", headers=[("ciao", b"ciao")])


if __name__ == "__main__":
    import anyio

    anyio.run(main)
