"""deepagents 기반 클러스터 진단 어댑터.

DiagnosisAnalyzer 포트 구현.
- trigger_time만 받아 agent가 직접 분석 구간과 순서를 결정한다.
- fetch_logs / drain_pending은 생성자 주입 → tool 클로저로 전달.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

_KST = timezone(timedelta(hours=9))

_logger = logging.getLogger(__name__)

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.diagnosis_analyzer import (
    DiagnosisResult,
    DiagnosisAnalyzer,
    LlmResponseError,
)
from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.application.port.outbound.node_log_fetcher import NodeLogFetcher
from cluster_doctor.infrastructure.outbound.agent.common.litellm_client import (
    require_supported_provider,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.agent import (
    build_supervisor,
)


def _text_of(message) -> str:
    """메시지에서 텍스트를 꺼낸다. 멀티모달 블록도 평탄화한다."""
    content = getattr(message, "content", "")
    if isinstance(content, list):
        return "\n".join(
            block["text"]
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
    return str(content or "").strip()


# 모델이 쓴 것이 아닌 메시지. langchain 메시지의 ``type``은 공개 속성이고
# 값이 고정돼 있다("ai" / "tool" / "human" / "system").
_NOT_MODEL_TEXT = frozenset({"tool", "human", "system"})


def _last_model_text(messages: list) -> str:
    """모델이 마지막으로 쓴 평문을 찾는다.

    ``messages[-1]``을 보면 안 된다. ``ToolStrategy``가 구조화 출력을 tool로
    바인딩하므로, 모델이 그 tool을 부르면 마지막 메시지는
    ``ToolMessage("Returning structured response: …")``가 된다(실측). 그 문자열을
    리포트로 쓰면 안내문이 리포트가 된다.

    **"AIMessage인 것"이 아니라 "모델이 쓴 것이 아닌 것"을 배제한다.** 타입 이름이나
    클래스로 좁히면 메시지 구현이 바뀔 때 조용히 아무것도 못 찾고, 그때 증상은
    "리포트가 비었다"로만 나타나 원인을 짚기 어렵다. 배제 목록은 그 반대로 —
    모르는 종류가 생기면 포함되는 쪽으로 틀린다.

    **모델이 마지막으로 쓴 것 하나만 본다.** "본문이 있는 마지막 것"을 찾아
    거슬러 올라가면 안 된다 — gemini 계열은 tool_call과 안내 문장을 한
    AIMessage에 함께 싣는 일이 흔해서, 마지막 턴이 조용히 끝났을 때
    "먼저 03:00~03:10 구간을 보겠습니다" 같은 **진행 안내문을 집어 리포트로
    내보내게 된다.** recursion_limit 도달이나 중간 중단도 같은 모양이다.

    tool_call을 달고 있는 메시지는 최종 답이 아니다. 모델이 아직 일하는
    중이었다는 뜻이므로 빈 문자열을 돌려주고 폴백 3단으로 보낸다 — 코드가
    모은 관측값만으로 리포트를 만드는 쪽이, 진행 안내문 한 줄을 진단이라고
    내보내는 것보다 정직하다.
    """
    for message in reversed(messages or []):
        if str(getattr(message, "type", "")) in _NOT_MODEL_TEXT:
            continue
        if getattr(message, "tool_calls", None):
            return ""
        return _text_of(message)
    return ""


class DeepAgentAnalyzer(DiagnosisAnalyzer):
    """deepagents 오케스트레이터 + ES tool + LangGraph 분석을 결합한 어댑터.

    SlowlogTriggerService가 asyncio.to_thread()로 감싸 호출하므로
    analyze()는 blocking sync로 구현한다.
    """

    def __init__(
        self,
        api_key: str,
        default_model: str,
        cluster: ClusterRepository,
        fetch_logs: Callable[[TimeRange], list[LogEntry]],
        drain_pending: Callable[[], list[LogEntry]],
        node_log_fetcher: NodeLogFetcher,
        fetch_node_logs: Callable[..., list],
        provider: str = "gemini",
    ) -> None:
        # 생성자에서 검증한다. 잘못된 provider를 첫 호출까지 끌고 가면
        # 진단 요청 한 건을 통째로 날린 뒤에야 오타를 알게 된다.
        self._provider = require_supported_provider(provider)
        self._api_key = api_key
        self._default_model = default_model
        self._cluster = cluster
        self._fetch_logs = fetch_logs
        self._drain_pending = drain_pending
        self._node_log_fetcher = node_log_fetcher
        self._fetch_node_logs = fetch_node_logs

    def analyze(
        self, log_time: datetime, kafka_receive_time: datetime
    ) -> DiagnosisResult:
        agent, state = build_supervisor(
            provider=self._provider,
            model=self._default_model,
            api_key=self._api_key,
            cluster=self._cluster,
            fetch_logs=self._fetch_logs,
            drain_pending=self._drain_pending,
            node_log_fetcher=self._node_log_fetcher,
            fetch_node_logs=self._fetch_node_logs,
            log_time=log_time,
            kafka_receive_time=kafka_receive_time,
        )

        log_time_kst = log_time.astimezone(_KST)
        kafka_time_kst = kafka_receive_time.astimezone(_KST)
        result = agent.invoke({
            "messages": [(
                "user",
                (
                    f"slowlog_timestamp: {log_time_kst.strftime('%Y-%m-%d %H:%M:%S')} KST (slowlog 자체 기재 시각)\n"
                    f"kafka_receive_time: {kafka_time_kst.strftime('%Y-%m-%d %H:%M:%S')} KST (Kafka 수신 시각)\n"
                    "모든 시각은 KST 기준이다. analyze_logs 호출 시 start_iso/end_iso도 KST 기준으로 입력하라.\n"
                    "두 시각을 참고해 적절한 trigger_time을 판단하고 ES slowlog 원인을 분석하라."
                ),
            )]
        })
        narrative_model = result.get("structured_response")
        content = _last_model_text(result.get("messages"))

        # 구간 커버리지는 실행이 끝난 뒤에만 판정할 수 있다. tool은 자기 호출만
        # 알고, 분할 호출이 정당한지는 요청 구간 전체의 합집합을 봐야 안다.
        state.gaps.extend(state.coverage_gaps())

        # 실패한 구간을 다시 불러 성공했으면 진단은 성립한 것이다. 일시 오류
        # (provider 과부하 등)로 붉은 배너를 붙이고 재트리거까지 막으면 배너가
        # 거짓이 되고, 거짓 배너는 배너 전체의 신뢰를 깎는다.
        unresolved = state.unresolved_failure()
        analysis_failed = state.degraded or unresolved is not None

        # 분석이 실패해도 본문은 전달한다. 여기서 예외를 던지면 notify가
        # 호출되지 않아 리포트가 사라지고, 운영자는 logs/app.log를 뒤져야
        # 실패를 알 수 있다. 재트리거를 막는 목적은 analysis_failed가 담당한다.
        if analysis_failed:
            _logger.error(
                "분석이 실패한 채 리포트가 작성되었다 — 재트리거하지 않는다%s",
                f" (미해결 구간: {unresolved})" if unresolved else "",
            )
        if state.gaps:
            _logger.warning(
                "수집하지 못한 보조 근거 %d건: %s",
                len(state.gaps),
                " / ".join(state.gaps),
            )

        observations = state.to_observations()

        # ── 폴백 사다리 ────────────────────────────────────────────────
        # 구조화 출력은 실패 갈래를 늘린다. "리포트는 항상 전달된다"를 지키려면
        # 각 단이 무엇을 남기는지 명시해야 한다.
        narrative = None
        if narrative_model is not None:
            # 1단. 정상.
            narrative = narrative_model.to_domain()
            content = ""
            # 형식은 맞았는데 판단이 비어 있을 수 있다. 실측으로 같은 구간을
            # 두 번 돌렸을 때 한 번은 9개 필드 중 8개, 한 번은 3개만 찼다 —
            # 프롬프트에 "반드시 채운다"를 넣은 뒤에도 그렇다. 그때 리포트에는
            # 결론도 발견된 문제점도 근본 원인도 없는데 analysis_failed는
            # False라, 운영자는 "분석했더니 특별한 게 없었다"로 읽는다.
            #
            # 사다리는 이것을 잡을 수 없다. 구조화 출력은 성공했고 관측값도
            # 온전하니 1단이 맞다. 그러니 등급을 바꾸는 대신 **사실을
            # 남긴다** — gaps는 notifier가 배너로 그리므로 빠진 것이 운영자에게
            # 반드시 도달한다. 관측값 쪽 빈칸을 다루는 방식과 같다.
            if not (narrative.headline or narrative.findings or narrative.root_cause):
                _logger.warning("모델이 판단 필드를 하나도 채우지 않았다")
                state.gaps.append(
                    "모델이 결론·발견된 문제점·근본 원인을 하나도 쓰지 않았다. "
                    "이 리포트에는 관측값만 있고 판단이 없다."
                )
        elif content:
            # 2단. 모델이 tool을 부르지 않고 평문으로 끝냈다. 관측값은 온전하고
            # 판단만 형식을 어긴 것이므로 분석 실패가 아니다 — gaps로 남긴다.
            _logger.warning(
                "모델이 구조화 리포트를 제출하지 않았다 — 평문을 그대로 싣는다"
            )
            state.gaps.append(
                "모델이 구조화 리포트를 제출하지 않아 판단 부분을 평문으로 실었다."
            )
            if observations.is_empty():
                # 관측값이 하나도 없는데 평문만 있다면 analyze_logs가 한 번도
                # 성공하지 않았다는 뜻이다. 429 직후 모델이 "로그를 확인할 수
                # 없습니다" 한 줄로 끝내는 형태가 실제로 이 갈래에 떨어진다.
                # 그것은 근거가 하나도 없는 판단이므로 정상 진단으로 내보내면
                # 안 된다 — "숫자는 코드가 세고 모델은 판단만 쓴다"의 이면이다.
                # 예외까지는 올리지 않는다. 평문은 남기되 배너를 붙여, 운영자가
                # 무엇을 근거로 읽어야 할지 알게 한다.
                _logger.error(
                    "관측값이 하나도 없다 — 평문을 싣되 분석 실패로 표시한다"
                )
                analysis_failed = True
        elif observations.is_empty():
            # 4단. 관측값도 판단도 없다. 여기까지 오는 것은 analyze_logs가 한 번도
            # 성공하지 않은 실행뿐이고, 그때만 예외로 올린다.
            raise LlmResponseError("agent가 빈 응답을 반환했고 관측값도 없습니다.")
        else:
            # 3단. 모델이 아무 말도 남기지 못했다. 그래도 코드가 모은 관측값이
            # 있으므로 그 시각에 무슨 일이 있었는지는 리포트에 남는다.
            _logger.error("agent가 빈 응답을 반환했다 — 관측값만으로 리포트를 만든다")
            analysis_failed = True

        return DiagnosisResult(
            report=DiagnosisReport(
                observations=observations,
                narrative=narrative,
                narrative_text=content,
            ),
            analysis_failed=analysis_failed,
            gaps=tuple(state.gaps),
        )


