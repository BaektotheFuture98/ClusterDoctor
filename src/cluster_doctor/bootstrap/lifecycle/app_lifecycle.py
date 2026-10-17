"""프로세스 시작/종료 시 정리해야 하는 자원.

``main.py``는 실행 객체를 조립하고 공유 클라이언트와 report/SFTP 구현을 만든다.
이 모듈은 공유 클라이언트를 언제 놓아주는지 맡는다. 정상 종료에서는
processor의 분석 정리가 끝난 뒤 닫고, 수동 실행도 같은 자원 수명을 사용한다.
"""

from contextlib import ExitStack
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

from clickhouse_connect.driver.client import Client
from elasticsearch import Elasticsearch


@dataclass
class RuntimeResources:
    """한 실행이 공유하는 클라이언트. 분석이 멈춘 뒤에 닫는다."""

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
