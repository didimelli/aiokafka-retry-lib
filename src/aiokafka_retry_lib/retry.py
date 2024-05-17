import datetime
import functools
from dataclasses import asdict, dataclass
from typing import Any, Callable, Coroutine, List, Optional, Sequence, Tuple, Type
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord

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


@dataclass(kw_only=True)
class RetryHeadersOut:
    scheduler_epoch: int
    scheduler_target_topic: str
    scheduler_target_key: bytes
    retry_attempt: int

    def dump(self) -> Sequence[Tuple[str, bytes]]:
        headers = []
        for kw, val in asdict(self).items():
            headers.append(
                (
                    kw.replace("_", "-"),
                    str(val).encode() if not isinstance(val, bytes) else val,
                )
            )
        return headers


@dataclass(kw_only=True)
class RetryHeadersIn:
    # Original message timestamp
    scheduler_timestamp: int
    scheduler_key: Optional[bytes]
    scheduler_topic: str
    retry_attempt: int

    @classmethod
    def parse_incoming_headers(
        cls, headers: Sequence[Tuple[str, bytes]]
    ) -> Optional["RetryHeadersIn"]:
        as_dict = dict(headers)
        scheduler_timestamp = as_dict.get("scheduler-timestamp")
        scheduler_key = as_dict.get("scheduler-key")
        scheduler_topic = as_dict.get("scheduler-topic")
        retry_attempt = as_dict.get("retry-attempt")
        if (
            scheduler_timestamp is not None
            and scheduler_key is not None
            and scheduler_topic is not None
            and retry_attempt is not None
        ):
            return cls(
                scheduler_timestamp=int(scheduler_timestamp),
                scheduler_key=scheduler_key,
                scheduler_topic=str(scheduler_topic),
                retry_attempt=int(retry_attempt),
            )
        else:
            return None


def retry(
    arg: str,
    # kafka bootstrap_servers,
    # retriable exceptions
    # max retries
    # dlt topic
    # scheduler topic
):
    def __wrapper(handler):
        @functools.wraps(handler)
        async def __wrapped(msg: ConsumerRecord, consumer: AIOKafkaConsumer, *args):
            # parse headers
            headers_in = RetryHeadersIn.parse_incoming_headers(msg.headers)
            # here basically use only retry_attempt to compute delay
            # if headers_in is None -> first message, not retried
            print(headers_in)
            try:
                return await handler(msg, consumer, *args)
            except Exception as e:
                print("got error", e)
                headers_out = RetryHeadersOut(
                    scheduler_epoch=int(datetime.datetime.now(datetime.UTC).timestamp())
                    + 10,
                    scheduler_target_topic=msg.topic,
                    scheduler_target_key=msg.key or b"retry",
                    retry_attempt=1,
                )
                # merge custom headers with retries ones
                async with AIOKafkaProducer(
                    bootstrap_servers="localhost:29092"
                ) as producer:
                    await producer.send(
                        "schedules",
                        msg.value,
                        headers=headers_out.dump(),
                        key=str(uuid4()).encode(),
                    )

        return __wrapped

    return __wrapper
