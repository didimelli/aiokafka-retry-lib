import datetime
import functools
import math
import random
from dataclasses import asdict, dataclass
from logging import getLogger
from typing import Dict, List, Optional, Sequence, Tuple, Type, Union
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition

logger = getLogger("aiokafka_retry_lib")


@dataclass(kw_only=True)
class RetryHeadersOut:
    scheduler_epoch: int
    scheduler_target_topic: str
    scheduler_target_key: bytes
    retry_attempt: int


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
    ) -> Tuple[Optional["RetryHeadersIn"], Dict[str, bytes]]:
        as_dict = dict(headers)
        try:
            scheduler_timestamp = as_dict.pop("scheduler-timestamp")
            scheduler_key = as_dict.pop("scheduler-key")
            scheduler_topic = as_dict.pop("scheduler-topic")
            retry_attempt = as_dict.pop("retry-attempt")
            return cls(
                scheduler_timestamp=int(scheduler_timestamp),
                scheduler_key=scheduler_key,
                scheduler_topic=str(scheduler_topic),
                retry_attempt=int(retry_attempt),
            ), as_dict
        except KeyError:
            return None, as_dict


def dump_headers(
    headers: Union[RetryHeadersIn, RetryHeadersOut],
) -> List[Tuple[str, bytes]]:
    dump = []
    for kw, val in asdict(headers).items():
        dump.append(
            (
                kw.replace("_", "-"),
                str(val).encode() if not isinstance(val, bytes) else val,
            )
        )
    return dump


DEFAULT_MINIMUM_DURATION = datetime.timedelta(milliseconds=100)
DEFAULT_MAXIMUM_DURATION = datetime.timedelta(seconds=10)
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_BACKOFF_JITTER = True


class BackoffStrategy:
    def __init__(
        self,
        jitter: bool = DEFAULT_BACKOFF_JITTER,
        factor: float = DEFAULT_BACKOFF_FACTOR,
        minimum: datetime.timedelta = DEFAULT_MINIMUM_DURATION,
        maximum: datetime.timedelta = DEFAULT_MAXIMUM_DURATION,
    ) -> None:
        if factor <= 0:
            self._factor = DEFAULT_BACKOFF_FACTOR
        else:
            self._factor = factor
        self._jitter = jitter
        if minimum < datetime.timedelta():
            self._min = DEFAULT_MINIMUM_DURATION
        else:
            self._min = minimum
        if maximum < datetime.timedelta():
            self._max = DEFAULT_MAXIMUM_DURATION
        else:
            self._max = maximum

    def duration_for_attempt(self, attempt: int) -> datetime.timedelta:
        if self._min >= self._max:
            return self._max
        duration = self._min * math.pow(self._factor, attempt)
        if self._jitter:
            duration = random.random() * (duration - self._min) + self._min
        # clamp value between min and max
        return max(self._min, min(duration, self._max))


def retry(
    *,
    bootstrap_servers: Union[str, List[str]],
    retriable_exceptions: List[Type[BaseException]],
    max_attempts: int,
    strategy: BackoffStrategy = BackoffStrategy(),
    dlt_topic_suffix: str = "-dlt",
    scheduler_topic: str = "schedules",
):
    def __wrapper(handler):
        @functools.wraps(handler)
        async def __wrapped(msg: ConsumerRecord, consumer: AIOKafkaConsumer, *args):
            try:
                return await handler(msg, consumer, *args)
            # Catch all exception to then act on them
            except Exception as e:  # noqa: W0718
                # parse headers
                retry_headers, custom_headers = RetryHeadersIn.parse_incoming_headers(
                    msg.headers
                )
                if retry_headers is not None:
                    current_attempt = retry_headers.retry_attempt
                else:
                    current_attempt = 0
                producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
                if type(e) in retriable_exceptions and current_attempt < max_attempts:
                    logger.info(
                        "Retrying because of %s. Attempt number %s", e, current_attempt
                    )
                    headers_out = RetryHeadersOut(
                        scheduler_epoch=int(
                            datetime.datetime.now(datetime.UTC).timestamp()
                            + strategy.duration_for_attempt(
                                current_attempt
                            ).total_seconds()
                        ),
                        scheduler_target_topic=msg.topic,
                        scheduler_target_key=msg.key or b"retry",
                        retry_attempt=current_attempt + 1,
                    )
                    async with producer:
                        await producer.send(
                            scheduler_topic,
                            msg.value,
                            # merge custom headers with retries ones
                            headers=dump_headers(headers_out)
                            + list(custom_headers.items()),
                            key=str(uuid4()).encode(),
                        )
                else:
                    logger.info("Sending to dlt. Attempt number %s", current_attempt)
                    async with producer:
                        await producer.send(
                            msg.topic + dlt_topic_suffix,
                            msg.value,
                        )
                tp = TopicPartition(msg.topic, msg.partition)
                await consumer.commit({tp: msg.offset + 1})

        return __wrapped

    return __wrapper
