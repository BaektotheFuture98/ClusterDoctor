"""deepagents 기반 클러스터 진단 어댑터.

LlmAnalyzer 포트 구현.
- trigger_time만 받아 agent가 직접 분석 구간과 순서를 결정한다.
- fetch_logs / drain_pending은 생성자 주입 → tool 클로저로 전달.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from functools import partial

_KST = timezone(timedelta(hours=9))

_logger = logging.getLogger(__name__)

from langchain.agents.structured_output import ToolStrategy
from langchain_litellm import ChatLiteLLM
from deepagents import create_deep_agent, FilesystemPermission

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.llm_analyzer import (
    DiagnosisResult,
    LlmAnalyzer,
    LlmResponseError,
)
from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport, Observations
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import (
    coverage_gaps,
    make_tools,
    unresolved_failure,
)
from cluster_doctor.infrastructure.outbound.ssh.node_log_fetcher import NodeLogFetcher
from cluster_doctor.infrastructure.outbound.llm.deepagent.prompts import SYSTEM_PROMPT
from cluster_doctor.infrastructure.outbound.llm.deepagent.report_schema import (
    ReportNarrative,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.nodes import MinuteOutput
from cluster_doctor.infrastructure.outbound.llm.litellm_client import (
    complete,
    require_supported_provider,
)

# 리포트에 싣는 마스터 로그 줄 수 상한. 수집 쪽 상한(_MASTER_LOG_MAX_LINES=80)은
# analyze_logs 호출마다 걸리므로 최대 6회면 480줄까지 쌓인다. 한 줄이 평균 524자
# 가므로(실측) 그대로 실으면 HTML이 수백 KB가 된다.
#
# 잘린 사실은 master_log_total로 드러난다 — "N줄 중 M줄"이 헤더에 찍힌다.
_MASTER_LOG_REPORT_MAX = 120

# 마스터 로그 정렬의 기준점. SSH 폴백으로 온 줄은 timestamp를 뽑을 수 없어
# None인데, None과 datetime을 직접 비교하면 TypeError가 난다.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_DENY_FILESYSTEM = FilesystemPermission(
    operations=["read", "write"],
    paths=["/**"],
    mode="deny",
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

    tool 호출만 담긴 AIMessage는 content가 비어 있으므로 자연히 걸러진다.
    """
    for message in reversed(messages or []):
        if str(getattr(message, "type", "")) in _NOT_MODEL_TEXT:
            continue
        text = _text_of(message)
        if text:
            return text
    return ""


def _make_structured_error_handler(limit: int = 2):
    """구조화 출력 검증 실패를 몇 번까지 되돌려 볼지 정한다.

    ``handle_errors=True``(기본값)는 **횟수 제한이 없다.** 실패할 때마다 오류
    ToolMessage를 붙여 다시 시키고, deepagents의 ``recursion_limit``이 9,999라
    프레임워크도 막지 않는다. 이 저장소는 429를 최우선 제약으로 다뤄 재시도를
    일부러 0으로 둔 곳이라, 그 결정을 스키마가 우회하게 둘 수 없다.

    ``handle_errors=False``도 답이 아니다. 예외가 그대로 올라와 agent 실행이
    통째로 죽고, 그러면 코드가 모아 둔 관측값까지 함께 잃는다.

    그래서 상한을 넘으면 **평문으로 답하라고 안내한다.** 그 답은 폴백 사다리
    2단이 받아 리포트가 되므로, 재시도를 끊어도 진단은 남는다.

    스키마는 길이를 제한하지 않는다(``report_schema`` 모듈 docstring 참고 —
    한국어가 글자 단위로 끊겨 ``follower_check``가 갈린 뒤로 잘라내기를 없앴다).
    그러므로 여기까지 오는 것은 타입이 어긋난 경우뿐이고, 실측에서는 아직 본
    적이 없다.
    """
    state = {"count": 0}

    def handle(exc: Exception) -> str:
        state["count"] += 1
        _logger.warning(
            "구조화 리포트 검증 실패 %d/%d: %s", state["count"], limit, exc
        )
        if state["count"] >= limit:
            return (
                "구조화 리포트 제출이 계속 실패했다. 더 시도하지 말고 "
                "지금까지의 분석을 평문으로 작성해 답하라."
            )
        return f"리포트 형식이 스키마와 맞지 않는다: {exc}. 형식을 고쳐 다시 제출하라."

    return handle


class DeepAgentAnalyzer(LlmAnalyzer):
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
        _bound = partial(
            _litellm_call,
            provider=self._provider,
            model=self._default_model,
            api_key=self._api_key,
        )
        call_llm = _bound
        call_llm_minute = partial(_bound, response_format=MinuteOutput)

        llm = ChatLiteLLM(
            # litellm의 모델 문자열은 "<provider>/<model>" 형태다.
            # litellm_client._PROVIDER_PREFIX와 같은 규칙을 쓴다.
            model=f"{self._provider}/{self._default_model}",
            api_key=self._api_key,
            # 재시도하지 않는다. 이 경로의 실패는 대부분 429이고, 그것은 분당
            # 입력 토큰 한도 초과가 원인이다. 같은 프롬프트를 다시 보내면
            # 실패가 보장된 채 소비만 배로 늘어난다 —
            # complete()의 num_retries=0과 같은 이유다(실측 513,122 토큰 →
            # 재시도 포함 2,052,488 토큰, 한도 250,000의 821%).
            #
            # 오케스트레이터는 tool 루프라 재시도가 루프 전체로 증폭되고,
            # 트리거 서비스가 큐 잔여 시 10초 간격으로 최대 4회 연속
            # 실행하므로(_MAX_CONSECUTIVE_RETRIGGERS=3) 증폭이 한 번 더 곱해진다.
            #
            # 예전 ChatGoogleGenerativeAI에서는 1이었다. 그쪽은 0을 "SDK
            # 기본값을 쓰라"(5회)로 해석하는 특수값이었기 때문이다.
            # ChatLiteLLM에는 그 해석이 없으므로 0이 곧 재시도 없음이다.
            max_retries=0,
        )

        # tool은 실패를 예외가 아니라 문자열로 돌려준다(예외는 agent 실행
        # 전체를 죽인다). 그 사실을 여기로 실어 나르는 통로다.
        run_state = {"degraded": False, "gaps": []}
        tools = make_tools(
            cluster=self._cluster,
            fetch_logs=self._fetch_logs,
            drain_pending=self._drain_pending,
            call_llm=call_llm,
            call_llm_minute=call_llm_minute,
            node_log_fetcher=self._node_log_fetcher,
            fetch_node_logs=self._fetch_node_logs,
            run_state=run_state,
            log_time=log_time,
            kafka_receive_time=kafka_receive_time,
        )

        # 구조화 출력을 건다. langchain은 이 모델(ChatLiteLLM + nvidia_nim/gemini)에
        # 대해 native json_schema를 쓰지 않고 ToolStrategy로 떨어진다 — 스키마가
        # **평범한 tool 하나로** 바인딩된다는 뜻이다(실측 확인).
        #
        # 그래서 두 가지가 따라온다. 첫째, 모델이 그 tool을 부르지 않고 평문으로
        # 끝낼 수 있다. 둘째, 결과가 result["structured_response"]로 가고
        # messages[-1]은 ToolMessage가 된다. 아래 폴백 사다리가 둘 다 받는다.
        #
        # ToolStrategy를 명시적으로 만드는 이유는 handle_errors 때문이다. 기본값은
        # 무제한 재시도이고, 이 저장소의 "재시도하지 않는다" 결정과 정면으로
        # 어긋난다.
        agent = create_deep_agent(
            model=llm,
            tools=tools,
            system_prompt=SYSTEM_PROMPT,
            permissions=[_DENY_FILESYSTEM],
            response_format=ToolStrategy(
                schema=ReportNarrative,
                handle_errors=_make_structured_error_handler(),
            ),
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
        run_state["gaps"].extend(coverage_gaps(run_state.get("observed", {})))

        # 실패한 구간을 다시 불러 성공했으면 진단은 성립한 것이다. 일시 오류
        # (provider 과부하 등)로 붉은 배너를 붙이고 재트리거까지 막으면 배너가
        # 거짓이 되고, 거짓 배너는 배너 전체의 신뢰를 깎는다.
        unresolved = unresolved_failure(run_state.get("observed", {}))
        analysis_failed = run_state["degraded"] or unresolved is not None

        # 분석이 실패해도 본문은 전달한다. 예전에는 여기서 LlmApiError를
        # 던졌는데, 그러면 notify가 호출되지 않아 리포트가 사라졌다 —
        # 운영자는 logs/app.log를 뒤져야 실패를 알 수 있었다. 재트리거를
        # 막는 목적은 analysis_failed가 그대로 담당한다.
        if analysis_failed:
            _logger.error(
                "분석이 실패한 채 리포트가 작성되었다 — 재트리거하지 않는다%s",
                f" (미해결 구간: {unresolved})" if unresolved else "",
            )
        if run_state["gaps"]:
            _logger.warning(
                "수집하지 못한 보조 근거 %d건: %s",
                len(run_state["gaps"]),
                " / ".join(run_state["gaps"]),
            )

        observations = _build_observations(run_state)

        # ── 폴백 사다리 ────────────────────────────────────────────────
        # 구조화 출력은 실패 갈래를 늘린다. "리포트는 항상 전달된다"를 지키려면
        # 각 단이 무엇을 남기는지 명시해야 한다.
        narrative = None
        if narrative_model is not None:
            # 1단. 정상.
            narrative = narrative_model.to_domain()
            content = ""
        elif content:
            # 2단. 모델이 tool을 부르지 않고 평문으로 끝냈다. 관측값은 온전하고
            # 판단만 형식을 어긴 것이므로 분석 실패가 아니다 — gaps로 남긴다.
            _logger.warning(
                "모델이 구조화 리포트를 제출하지 않았다 — 평문을 그대로 싣는다"
            )
            run_state["gaps"].append(
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
            # 3단. 모델이 아무 말도 남기지 못했다. 예전에는 이것이 곧 "전달할 것이
            # 없다"였지만, 이제는 코드가 모은 관측값이 있다. 그 시각에 무슨 일이
            # 있었는지는 리포트에 남는다.
            _logger.error("agent가 빈 응답을 반환했다 — 관측값만으로 리포트를 만든다")
            analysis_failed = True

        return DiagnosisResult(
            report=DiagnosisReport(
                observations=observations,
                narrative=narrative,
                narrative_text=content,
            ),
            analysis_failed=analysis_failed,
            gaps=tuple(run_state["gaps"]),
        )


def _build_observations(run_state: dict) -> Observations:
    """tool들이 ``run_state``에 쌓아 둔 관측값을 도메인 객체로 굳힌다.

    수집 중에는 병합이 쉬운 dict로 들고 있다가 여기서 정렬된 tuple이 된다.
    리포트에 실린 뒤에는 바뀌지 않아야 하므로 frozen 타입으로 옮긴다.
    """
    observed = run_state.get("observed", {})
    raw = run_state.get("observations", {})

    timeline_map = raw.get("timeline", {})
    node_map = raw.get("nodes", {})
    master_map = raw.get("master_logs", {})
    candidate_map = raw.get("candidates", {})

    # timestamp가 있는 줄을 먼저, 시간순으로. SSH 폴백 줄(timestamp 없음)은
    # 뒤로 보내되 넣은 순서를 유지한다 — 원본 파일의 순서가 곧 시간순이다.
    ordered_master = sorted(
        enumerate(master_map.values()),
        key=lambda pair: (
            pair[1].timestamp is None,
            pair[1].timestamp or _EPOCH,
            pair[0],
        ),
    )
    master_events = tuple(event for _index, event in ordered_master)

    return Observations(
        time_basis=observed.get("time_basis", ""),
        first_seen=observed.get("first_seen"),
        last_seen=observed.get("last_seen"),
        requested=tuple(observed.get("requested") or ()),
        total_wait_seconds=float(raw.get("wait_seconds", 0.0)),
        wait_cap_reached=bool(raw.get("wait_cap_reached", False)),
        timeline=tuple(timeline_map[minute] for minute in sorted(timeline_map)),
        nodes=tuple(sorted(node_map.values(), key=lambda row: row.node)),
        master_events=master_events[:_MASTER_LOG_REPORT_MAX],
        master_log_total=len(master_events),
        health=tuple(raw.get("health") or ()),
        # id는 C1, C2 … 순으로 붙었으므로 발견 순서로 정렬하려면 숫자 부분을
        # 봐야 한다. 문자열 정렬이면 C10이 C2 앞에 온다. 다만 int()로 파싱하지
        # 않는다 — 이 함수는 tool이 아니라 analyze() 안이라 예외가 나면
        # _run_agent의 generic handler로 가서 notify를 건너뛰고, "리포트는 항상
        # 전달된다"가 그대로 깨진다. 길이를 먼저 보면 파싱 없이 같은 순서가
        # 나오고 어떤 문자열이 와도 터지지 않는다.
        candidates=tuple(
            sorted(
                candidate_map.values(),
                key=lambda c: (len(c.candidate_id), c.candidate_id),
            )
        ),
    )


def _litellm_call(
    messages: list[dict],
    max_tokens: int,
    *,
    provider: str,
    model: str,
    api_key: str,
    response_format=None,
) -> str:
    return complete(
        messages=messages,
        provider=provider,
        model=model,
        api_key=api_key,
        max_tokens=max_tokens,
        response_format=response_format,
    )
