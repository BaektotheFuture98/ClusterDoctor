"""프로세스 시작/종료 시 정리해야 하는 자원.

composition 자체(``dependency/wiring.py``)와 나누는 이유는 "무엇을 조립하는가"와
"언제 놓아주는가"가 다른 관심사이기 때문이다.
"""

from contextlib import ExitStack
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

from clickhouse_connect.driver.client import Client
from elasticsearch import Elasticsearch


@dataclass
class RuntimeResources:
    """Clients shared by one execution; close them after analysis has stopped."""

    clickhouse_client: Client
    es_client: Elasticsearch
    _exit_stack: ExitStack = field(repr=False)

    def close(self) -> None:
        self._exit_stack.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        return self._exit_stack.__exit__(exc_type, exc_value, traceback)
