from abc import ABC, abstractmethod

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange

class LlmApiError(RuntimeError):
    """LLM provider가 비-2xx 응답을 반환했다.

    메시지에는 상태 코드만 담는다. 요청 URL·응답 본문·API 키는 담지 않는다 —
    URL에는 provider에 따라 키가 실릴 수 있고, 본문에는 내부 정보가 실린다.
    """


class LlmResponseError(RuntimeError):
    """LLM이 2xx를 반환했지만 응답에 사용할 텍스트가 없다.

    토큰 한도 도달(finish_reason="length")이나 정책 필터
    (finish_reason="content_filter")가 대표적이다. 둘 다 예외가 아닌
    정상 200으로 오므로 어댑터가 직접 판별해야 한다.
    """


class LlmAnalyzer(ABC):
    @abstractmethod
    def analyze(self, time_range: TimeRange, logs: list[LogEntry], model: str | None) -> str:
        ...
