"""application 계층이 말하는 실패.

인프라 예외(clickhouse_connect, paramiko, litellm)를 그대로 올리면 상위가
그 라이브러리를 알아야 하고, 메시지에 접속 문자열·키가 실릴 수 있다. 어댑터가
여기 있는 타입으로 번역한다.
"""

from __future__ import annotations


class ClusterDoctorError(RuntimeError):
    """이 애플리케이션이 스스로 올리는 실패의 뿌리."""


class KafkaUnavailableError(ClusterDoctorError):
    """설정된 제한 시간 안에 Kafka 연결이 복구되지 않았다."""


class LlmApiError(ClusterDoctorError):
    """LLM provider가 비-2xx 응답을 반환했다.

    메시지에는 상태 코드만 담는다. 요청 URL·응답 본문·API 키는 담지 않는다 —
    URL에는 provider에 따라 키가 실릴 수 있고, 본문에는 내부 정보가 실린다.
    """


class LlmResponseError(ClusterDoctorError):
    """LLM이 2xx를 반환했지만 응답에 사용할 텍스트가 없다.

    토큰 한도 도달(finish_reason="length")이나 정책 필터
    (finish_reason="content_filter")가 대표적이다. 둘 다 예외가 아닌
    정상 200으로 오므로 어댑터가 직접 판별해야 한다.
    """


class GuardrailViolation(ClusterDoctorError):
    """런타임 상한이 행동을 거절했다.

    Main Agent의 도구·미들웨어가 거절 메시지로 변환한다.
    거절된 행동을 중단하며, 거절 자체를 Incident 실패로 처리하지 않는다.
    """
