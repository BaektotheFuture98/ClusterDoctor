from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


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


@dataclass(frozen=True)
class DiagnosisResult:
    """진단 한 건의 결과.

    예전에는 ``analyze``가 리포트 문자열만 돌려주고, 실패는 예외로 알렸다.
    그래서 "리포트를 전달할지"와 "실패한 실행을 재트리거할지"가 한 갈래로
    묶여 있었다 — 예외가 나면 ``notify``가 호출되지 않으므로 리포트가
    통째로 사라졌다.

    그 묶임이 실제로 손해를 냈다. 보조 조사(노드 로그 SSH 수집)가 실패하면
    4단계 분석이 온전히 끝난 리포트까지 버려졌다. 문자열 하나로는 "아무것도
    만들지 못했다"와 "만들었지만 일부가 빠졌다"를 구별할 수 없었기 때문이다.

    이제 셋을 따로 싣는다.

      report          운영자에게 전달할 본문. **항상 전달한다.**
      analysis_failed 분석 자체가 실패했는가. 재트리거를 막는 유일한 조건이다.
      gaps            수집하지 못한 보조 근거. 리포트는 유효하지만 일부가 빠졌다.
                      재트리거를 막지 않는다 — 분석은 성공했으므로.
    """

    report: str
    analysis_failed: bool = False
    gaps: tuple[str, ...] = field(default_factory=tuple)


class LlmAnalyzer(ABC):
    @abstractmethod
    def analyze(
        self, log_time: datetime, kafka_receive_time: datetime
    ) -> DiagnosisResult:
        """slowlog 자체 timestamp와 Kafka 수신 시각을 받아 클러스터를 진단한다.

        전달할 것이 아예 없을 때만 예외를 올린다(빈 응답). 분석이 실패했더라도
        agent가 쓴 본문이 있으면 ``analysis_failed=True``로 실어 보낸다 —
        운영자가 로그를 뒤지지 않고 실패를 알 수 있어야 한다.
        """
        ...
