"""application 계층이 말하는 실패.

인프라 예외(clickhouse_connect, paramiko, litellm)를 그대로 올리면 상위가
그 라이브러리를 알아야 하고, 메시지에 접속 문자열·키가 실릴 수 있다. 어댑터가
여기 있는 타입으로 번역한다.
"""

from __future__ import annotations


class ClusterDoctorError(RuntimeError):
    """이 애플리케이션이 스스로 올리는 실패의 뿌리."""


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


class IncidentNotFoundError(ClusterDoctorError):
    """저장소에 없는 Incident를 꺼내려 했다."""


class GuardrailViolation(ClusterDoctorError):
    """런타임 상한이 행동을 거절했다.

    예외로 두는 것은 **호출자가 우회할 수 없게** 하기 위해서다. 판정 결과를
    bool로 돌려주면 확인을 빠뜨린 경로가 조용히 상한을 넘는다. 다만 Supervisor
    사이클은 이것을 잡아 다음 행동으로 넘어간다 — 거절은 Incident의 실패가
    아니다.
    """
