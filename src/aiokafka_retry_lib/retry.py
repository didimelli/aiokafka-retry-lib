from typing import Any, Callable, Coroutine, List, Optional, Type

from aiokafka import AIOKafkaConsumer

from aiokafka_retry_lib.errors import RetriableError
from aiokafka_retry_lib.topics import Topics


class RetriableConsumer:
    def __init__(
        self,
        topic: str,
        retry_suffix: str,
        dlt_suffix: str,
        retriable_errors: Optional[List[Type[BaseException]]] = None,
        **aio_kafka_consumer_init_kwargs,
    ) -> None:
        self._topics = Topics(topic, retry_suffix, dlt_suffix)
        self._consumer = AIOKafkaConsumer(
            self._topics.all(),
            **aio_kafka_consumer_init_kwargs,
        )
        self._retriable_errors = (
            retriable_errors
            if retriable_errors
            else [
                RetriableError,
            ]
        )

    async def handle(self, handler: Callable[..., Coroutine[Any, Any, None]]) -> None:
        async with self._consumer as consumer:
            async for msg in consumer:
                try:
                    await handler(msg)
                except Exception as e:
                    if e in self._retriable_errors:
                        # send to next RETRY topic
                        _next_topic = self._topics.next_topic(msg.topic)
                        pass
                    else:
                        # send to DLT topic
                        _dlt_topic = self._topics.dlt_topic()
                        pass
