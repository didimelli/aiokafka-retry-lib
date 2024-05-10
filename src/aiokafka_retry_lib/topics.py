from typing import List


class Topics:
    def __init__(
        self,
        topic: str,
        retry_suffix: str,
        dlt_suffix: str,
        max_retries: int = 10,
    ) -> None:
        self._topic = topic
        self._retry_suffix = retry_suffix
        self._dlt_suffix = dlt_suffix
        self._max_retries = max_retries

    def all(self) -> List[str]:
        # return list of all strings
        return [self._retry_topic_base() + f"-{i}" for i in range(self._max_retries)]

    def next_topic(self, topic: str) -> str:
        topic_idx = topic.lstrip(self._topic).lstrip(self._retry_suffix)
        if topic_idx == "":
            # totally consumed, means it was first topic
            return self.first_retry_topic()
        try:
            next_idx = int(topic_idx) + 1
            if next_idx <= self._max_retries:
                return self._topic + self._retry_suffix + f"-{next_idx}"
            else:
                # that was the last topic, send to DLT
                return self.dlt_topic()
        except ValueError:
            # idx was not a number, send to first retry
            # TODO: Check if it is sensible default
            return self.first_retry_topic()

    def _retry_topic_base(self) -> str:
        return self._topic + self._retry_suffix

    def first_retry_topic(self) -> str:
        return self._retry_topic_base() + "-0"

    def dlt_topic(self) -> str:
        return self._topic + self._dlt_suffix
