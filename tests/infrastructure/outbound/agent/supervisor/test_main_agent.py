"""Main DeepAgent가 실제로 ``task``로 위임하는가, 그리고 상한이 그 위에 서는가.

**여기서 가짜는 chat model 한 겹뿐이다.** ``create_deep_agent``이 만든 그래프도,
내장 ``task`` 도구도, ``DelegationGuardrailMiddleware``도, Guardrail 도구도 전부
운영에서 도는 그대로다. 루프가 Python ``for``에서 모델의 판단으로 옮겨간 뒤에
상한이 남아 있는지는 그 배선 위에서만 물을 수 있다 — deepagents를 통째로
mock하면 "위임했다"는 문장만 남고 검증한 것은 없다.

대본은 tool call을 **실제로** 낸다. ``propose_analysis`` → ``task`` 순서를
모델이 지키는 경우와 어기는 경우를 모두 태워, 무엇이 통과하고 무엇이 막히는지
본다.
"""

from datetime import datetime, timedelta

from deepagents import CompiledSubAgent
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.runnables import RunnableLambda

from cluster_doctor.application.service.guardrails import (
    MAX_ANALYSIS_CALLS,
    MAX_ANALYZED_MINUTES,
    MAX_REJECTED_DECISIONS,
)
from cluster_doctor.domain.model.incident import IncidentStatus
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.agent.common import harness as harness_module
from cluster_doctor.infrastructure.outbound.agent.supervisor.guardrail_middleware import (
    DelegationGuardrailMiddleware,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.main_agent import (
    build_main_agent,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    CLUSTER,
    DIAGNOSIS_SUBAGENT,
    INCIDENT_ID,
    LAST_RESPONSE,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.tools import (
    TASK_TOOL_NAME,
    make_finish_incident_tool,
    make_list_candidate_windows_tool,
    make_propose_analysis_tool,
)
from cluster_doctor.storage.in_memory_incident_state_store import (
    InMemoryIncidentStateRepository,
)
from tests.contracts.test_time_range_spans import KST, label, span
from tests.infrastructure.outbound.agent.scripted_model import ScriptedChatModel, ai, say

TRIGGER = datetime(2026, 9, 18, 14, 3, tzinfo=KST)

# Main Agent가 쥘 수 있는 도구 전부. 파일시스템도 셸도 여기 없다.
EXPECTED_TOOLS = {
    "propose_analysis",
    "list_candidate_windows",
    "finish_incident",
    TASK_TOOL_NAME,
}


def iso(hour: int, minute: int) -> str:
    return datetime(2026, 9, 18, hour, minute, tzinfo=KST).isoformat()


def propose(from_h, from_m, to_h, to_m, goal: str = "확인") -> AIMessage:
    return ai(
        (
            "propose_analysis",
            {
                "start_kst": iso(from_h, from_m),
                "end_kst": iso(to_h, to_m),
                "goal": goal,
            },
        )
    )


def delegate(description: str = "이 구간에서 무엇이 먼저 무너졌는지 본다") -> AIMessage:
    return ai((TASK_TOOL_NAME, {"description": description, "subagent_type": DIAGNOSIS_SUBAGENT}))


def finish(outcome: str = "COMPLETED", reason: str = "충분하다") -> AIMessage:
    return ai(("finish_incident", {"outcome": outcome, "reason": reason}))


class RecordingSubAgent:
    """진단 SubAgent 자리에 앉아 **무엇을 받았는지** 남긴다.

    진단 파이프라인 자체는 여기서 보지 않는다(그것은
    ``diagnosis/test_subagent.py``의 일이다). 이 자리에서 물어야 하는 것은
    하나다 — ``task``가 실제로 이 runnable까지 도달했는가, 그리고 도달했을 때
    **승인된 구간**이 state로 함께 왔는가.
    """

    def __init__(self, *, handback=None) -> None:
        self.entries: list[dict] = []
        self._handback = handback

    def __call__(self, state: dict) -> dict:
        self.entries.append(dict(state))
        update = {
            "messages": [
                AIMessage(content=f"분석 종료 status=COMPLETED ({len(self.entries)}번째 위임)")
            ],
            # 승인을 소모한다. 실제 SubAgent가 지키는 계약과 같게 둔다 —
            # 여기서 남겨 두면 다음 ``task``가 승인 없이 통과한다.
            ADMITTED_WINDOW: None,
            ADMITTED_GOAL: "",
        }
        if self._handback is not None:
            update[LAST_RESPONSE] = self._handback
        return update

    @property
    def admitted_windows(self) -> list[tuple[str, str]]:
        return [
            (
                entry[ADMITTED_WINDOW]["start"][-8:-3],
                entry[ADMITTED_WINDOW]["end"][-8:-3],
            )
            for entry in self.entries
            if entry.get(ADMITTED_WINDOW)
        ]


def run(script, *, state=None, subagent=None, repository=None, recursion_limit=200):
    """대본 하나를 진짜 Main DeepAgent에 태운다."""
    state = state or IncidentState(incident_id="inc-1")
    repository = repository or InMemoryIncidentStateRepository()
    repository.create(state)
    subagent = subagent or RecordingSubAgent()
    model = ScriptedChatModel(responses=list(script))

    graph = build_main_agent(
        model=model,
        tools=[
            make_list_candidate_windows_tool(state=state),
            make_propose_analysis_tool(state=state, repository=repository),
            make_finish_incident_tool(state=state, repository=repository),
        ],
        middleware=[DelegationGuardrailMiddleware(state=state, repository=repository)],
        diagnosis_subagent=CompiledSubAgent(
            name=DIAGNOSIS_SUBAGENT,
            description="승인된 구간 하나를 조사한다",
            runnable=RunnableLambda(subagent),
        ),
    )

    result = graph.invoke(
        {
            "messages": [AIMessage(content="Incident가 열렸다")],
            INCIDENT_ID: state.incident_id,
            CLUSTER: "es-prod",
            ADMITTED_WINDOW: None,
            ADMITTED_GOAL: "",
            LAST_RESPONSE: None,
        },
        {"recursion_limit": recursion_limit},
    )
    return result, state, subagent, model


def tool_messages(result) -> list[ToolMessage]:
    return [m for m in result["messages"] if isinstance(m, ToolMessage)]


def texts(result) -> list[str]:
    return [str(m.content) for m in tool_messages(result)]


class TestDelegation:
    def test_승인받은_뒤_task로_부르면_진단_SubAgent가_실제로_돌아간다(self):
        """요구사항 2번.

        Main Agent가 "위임했다"고 말하는 것과 SubAgent runnable이 실제로
        실행되는 것은 다르다. 도구 이름만 맞고 배선이 끊겨 있으면 예산만
        차감되고 분석은 일어나지 않는다.
        """
        result, state, subagent, _model = run(
            [propose(14, 0, 14, 10, "JVM 압력 시작점"), delegate(), finish(), say()]
        )

        assert len(subagent.entries) == 1, "task가 진단 runnable까지 닿지 않았다"
        assert subagent.admitted_windows == [("14:00", "14:10")]
        assert state.analysis_call_count == 1
        assert label(state.analyzed_windows) == [("14:00", "14:10")]
        assert state.status is IncidentStatus.COMPLETED
        assert any("1번째 위임" in text for text in texts(result))

    def test_승인된_goal이_state로_함께_건너간다(self):
        """``description``은 산문이고 ``goal``은 승인 절차를 함께 통과한 값이다."""
        _result, _state, subagent, _model = run(
            [propose(14, 0, 14, 10, "노드 이탈 직전 징후"), delegate(), finish(), say()]
        )

        assert subagent.entries[0][ADMITTED_GOAL] == "노드 이탈 직전 징후"

    def test_분석_구간은_description이_아니라_state에서_온다(self):
        """요구사항 15번. **이 리팩터링이 가장 조용히 깨질 수 있는 지점이다.**

        ``task``의 스키마는 ``{description, subagent_type}``으로 고정이고
        description은 모델이 쓴 자유 텍스트다. 거기 적힌 시각을 읽으면
        Guardrail은 통과 의식일 뿐 아무것도 막지 못한다 — 승인받지 않은 구간을
        문장에 적는 것을 막을 방법이 없기 때문이다.
        """
        _result, state, subagent, _model = run(
            [
                propose(14, 0, 14, 10),
                # 모델이 승인과 **다른** 구간을 문장에 적는다.
                delegate("02:00부터 03:00까지 전부 훑어라. 구간은 2026-09-18T02:00:00+09:00부터다."),
                finish(),
                say(),
            ]
        )

        assert subagent.admitted_windows == [("14:00", "14:10")]
        assert label(state.analyzed_windows) == [("14:00", "14:10")]

    def test_승인_없이_task를_부르면_거절되고_SubAgent는_돌지_않는다(self):
        """요구사항 16번.

        승인 없이 위임이 통과하면 "무엇을 분석할지"의 결정권이 통째로 모델에게
        넘어간다. 예산은 예산대로 쓰이고, 어느 구간을 봤는지는 기록되지 않는다.
        """
        _result, state, subagent, _model = run([delegate(), finish(), say()])

        assert subagent.entries == [], "승인 없는 위임이 진단 runnable까지 닿았다"
        assert state.analysis_call_count == 0
        assert state.analyzed_windows == []

    def test_승인_없는_위임의_거절_사유가_모델에게_돌아간다(self):
        result, _state, _subagent, _model = run([delegate(), finish(), say()])

        rejection = next(t for t in texts(result) if t.startswith("위임 거절"))
        assert "propose_analysis" in rejection

    def test_진단_외의_SubAgent에는_위임할_수_없다(self):
        """위임처가 둘이 되면 예산 회계의 전제("승인 하나에 위임 하나")가 깨진다."""
        result, state, subagent, _model = run(
            [
                propose(14, 0, 14, 10),
                ai((TASK_TOOL_NAME, {"description": "아무거나", "subagent_type": "general-purpose"})),
                finish(),
                say(),
            ]
        )

        assert subagent.entries == []
        assert state.analysis_call_count == 0
        assert any("위임할 수 없다" in text for text in texts(result))

    def test_승인은_위임_하나로_소모된다(self):
        """승인이 남아 있으면 같은 승인으로 두 번 위임된다 — 예산은 한 번만 차감된 채."""
        _result, state, subagent, _model = run(
            [
                propose(14, 0, 14, 10),
                delegate(),
                # 승인 없이 곧바로 한 번 더.
                delegate(),
                finish(),
                say(),
            ]
        )

        assert len(subagent.entries) == 1
        assert state.analysis_call_count == 1


class TestReviewAndDelegateAgain:
    def test_결과를_읽고_다른_구간으로_다시_위임한다(self):
        """요구사항 4번.

        한 번의 위임으로 끝나지 않는 것이 이 구조의 존재 이유다. SubAgent의
        응답이 부모의 Context로 돌아오고, 그것을 읽은 뒤 다음 구간이 정해진다.
        """
        subagent = RecordingSubAgent(
            handback={"status": "NEED_MORE_CONTEXT", "analysis_summary": "앞 구간이 필요하다"}
        )
        result, state, subagent, _model = run(
            [
                propose(14, 0, 14, 10, "무엇이 먼저 무너졌나"),
                delegate(),
                propose(13, 50, 14, 0, "징후 시작 시점"),
                delegate(),
                finish(),
                say(),
            ],
            subagent=subagent,
        )

        assert subagent.admitted_windows == [("14:00", "14:10"), ("13:50", "14:00")]
        assert state.analysis_call_count == 2
        assert state.analyzed_minutes == 20
        # 첫 위임의 응답이 실제로 부모의 대화에 돌아왔다. 돌아오지 않으면
        # 두 번째 판단은 아무것도 보지 못한 채 내려진다.
        assert any("1번째 위임" in text for text in texts(result))

    def test_직전_응답이_graph_state에_남아_다음_위임까지_간다(self):
        subagent = RecordingSubAgent(handback={"status": "COMPLETED", "report_ref": "R-1"})
        _result, _state, subagent, _model = run(
            [
                propose(14, 0, 14, 10),
                delegate(),
                propose(13, 50, 14, 0),
                delegate(),
                finish(),
                say(),
            ],
            subagent=subagent,
        )

        assert subagent.entries[1][LAST_RESPONSE] == {
            "status": "COMPLETED",
            "report_ref": "R-1",
        }


class TestDuplicateWindows:
    def test_이미_분석한_구간을_다시_제안하면_거절된다(self):
        """요구사항 5번. 프롬프트가 아니라 런타임이 막는다."""
        result, state, subagent, _model = run(
            [
                propose(14, 0, 14, 10),
                delegate(),
                propose(14, 0, 14, 10),
                finish(),
                say(),
            ]
        )

        assert len(subagent.entries) == 1
        assert any(text.startswith("거절:") for text in texts(result))
        assert state.analyzed_minutes == 10

    def test_부분_중복은_아직_보지_않은_쪽만_승인된다(self):
        """통째로 거절하면 모델이 같은 요청을 다시 내고 사이클만 태운다."""
        _result, state, subagent, _model = run(
            [
                propose(14, 0, 14, 10),
                delegate(),
                propose(14, 5, 14, 15),
                delegate(),
                finish(),
                say(),
            ]
        )

        assert subagent.admitted_windows == [("14:00", "14:10"), ("14:10", "14:15")]
        assert state.analyzed_minutes == 15

    def test_좁혀진_사실이_모델에게_먼저_알려진다(self):
        """요청 그대로 승인됐다고 믿으면, 분석되지 않은 구간을 근거로 판단하게 된다."""
        result, _state, _subagent, _model = run(
            [
                propose(14, 0, 14, 10),
                delegate(),
                propose(14, 5, 14, 15),
                finish(),
                say(),
            ]
        )

        narrowed = next(t for t in texts(result) if "[주의]" in t)
        assert "14:10" in narrowed and "14:15" in narrowed

    def test_되돌려_읽을_수_없는_구간은_제안_단계에서_걸린다(self):
        result, state, subagent, _model = run(
            [
                ai(
                    (
                        "propose_analysis",
                        {"start_kst": "어제 오후", "end_kst": "오늘", "goal": "대충"},
                    )
                ),
                finish(),
                say(),
            ]
        )

        assert subagent.entries == []
        assert state.rejected_decision_count >= 1
        assert any("구간을 읽을 수 없다" in text for text in texts(result))


class TestBudget:
    def test_분_예산을_넘겨_분석하지_못한다(self):
        """요구사항 6번.

        예산의 단위는 호출 수가 아니라 **분**이다. 10분짜리 여섯 번이면 60분
        예산을 정확히 다 쓰고, 일곱 번째는 승인 자체가 나오지 않는다.
        """
        windows = [
            (13, 0, 13, 10),
            (13, 10, 13, 20),
            (13, 20, 13, 30),
            (13, 30, 13, 40),
            (13, 40, 13, 50),
            (13, 50, 14, 0),
        ]
        script = []
        for window in windows:
            script.extend([propose(*window), delegate()])
        script.extend([propose(14, 0, 14, 10), finish(), say()])

        result, state, subagent, _model = run(script)

        assert len(subagent.entries) == len(windows)
        assert state.analyzed_minutes == MAX_ANALYZED_MINUTES
        assert any("예산" in text and text.startswith("거절:") for text in texts(result))

    def test_남은_예산보다_긴_요청은_남은_만큼만_승인된다(self):
        """통째로 거절하면 남은 예산을 쓰지 못한 채 Incident가 끝난다."""
        script = []
        for window in [
            (13, 0, 13, 10),
            (13, 10, 13, 20),
            (13, 20, 13, 30),
            (13, 30, 13, 40),
            (13, 40, 13, 50),
            (13, 50, 13, 56),
        ]:
            script.extend([propose(*window), delegate()])
        script.extend([propose(14, 0, 14, 10), delegate(), finish(), say()])

        _result, state, subagent, _model = run(script)

        # 56분을 썼으므로 4분만 남는다.
        assert subagent.admitted_windows[-1] == ("14:00", "14:04")
        assert state.analyzed_minutes == MAX_ANALYZED_MINUTES

    def test_위임_횟수가_상한을_넘지_않는다(self):
        """요구사항 7번.

        분 예산이 주 제약이지만 그것만으로는 폭주를 막지 못한다 — 1분짜리
        요청을 반복하면 분은 거의 늘지 않으면서 호출만 쌓인다.
        """
        script = []
        for index in range(MAX_ANALYSIS_CALLS + 2):
            minute = index * 2
            script.extend(
                [propose(13, minute, 13, minute + 1), delegate()]
            )
        script.extend([finish(), say()])

        _result, state, subagent, _model = run(script, recursion_limit=400)

        assert len(subagent.entries) == MAX_ANALYSIS_CALLS
        assert state.analysis_call_count == MAX_ANALYSIS_CALLS
        assert state.analyzed_minutes == MAX_ANALYSIS_CALLS


class TestNoInfiniteRetry:
    def test_같은_제안을_되풀이해도_분석은_한_번만_돈다(self):
        """요구사항 10번의 한 갈래.

        거절은 분 예산도 호출 수도 늘리지 않는다. 그래서 거절만 반복하는
        갈래에는 원래 상한이 없다 — 연속 거절을 세는 이유가 그것이다.
        """
        repository = InMemoryIncidentStateRepository()
        script = [propose(14, 0, 14, 10), delegate()]
        # 매번 새로 만든다. 같은 메시지 객체를 되풀이하면 tool_call id가 겹쳐
        # 두 번째부터는 도구가 실행되지도 않는다.
        script.extend(propose(14, 0, 14, 10) for _ in range(5))
        script.extend([finish(), say()])

        result, _state, subagent, model = run(script, repository=repository)

        final = repository.get("inc-1")
        assert len(subagent.entries) == 1
        assert final.rejected_decision_count == MAX_REJECTED_DECISIONS
        assert any(
            f"연속 거절이 상한 {MAX_REJECTED_DECISIONS}회에 도달했다" in text
            for text in texts(result)
        )
        # **경고만 하고 끝나지 않는다.** 상한에 닿은 순간 그래프가 멈추므로
        # 대본에 남아 있던 finish/say는 아예 실행되지 않는다.
        assert model.call_count < len(script)
        assert final.status is IncidentStatus.COMPLETED

    def test_도구_오류는_예외가_아니라_문장으로_돌아온다(self):
        """예외를 그대로 올리면 모델에게는 "도구가 고장났다"로 보이고, 그러면
        같은 인자로 재시도한다. 무엇을 대신 보내야 하는지 문장으로 돌려줘야
        다음 시도가 달라진다."""
        result, _state, _subagent, _model = run(
            [
                ai(("finish_incident", {"outcome": "무엇이든", "reason": "?"})),
                finish(),
                say(),
            ]
        )

        assert any("알 수 없는 값이다" in text for text in texts(result))

    def test_승인만_받고_끝내도_Incident는_닫힌다(self):
        _result, state, subagent, _model = run(
            [propose(14, 0, 14, 10), finish("FAILED", "볼 것이 없다"), say()]
        )

        assert subagent.entries == []
        assert state.status is IncidentStatus.FAILED
        assert state.closing_reason == "볼 것이 없다"


class TestZeroDelegation:
    def test_한_번도_위임하지_않고_닫을_수_있다(self):
        """요구사항 3번. 볼 구간이 없다고 판단하는 것도 정상 종료다."""
        _result, state, subagent, _model = run([finish(), say()])

        assert subagent.entries == []
        assert state.analysis_call_count == 0
        assert state.status is IncidentStatus.COMPLETED


class TestToolSurface:
    def test_모델에게_보이는_도구는_넷뿐이다(self):
        """요구사항 17번. **이 프로세스는 장애 중인 클러스터의 SSH 자격증명을 들고 있다.**

        ``create_deep_agent``은 부르기만 하면 ``execute``와 파일 도구를 함께
        묶어 준다. 분석을 잘못하는 것과 그 자격증명을 쥔 채 셸을 여는 것은
        위험의 종류가 다르다. 기본값이 되살아나는 사고는 모델에게 실제로
        건네진 목록 말고는 드러날 자리가 없다.
        """
        _result, _state, _subagent, model = run([finish(), say()])

        assert model.bound_tool_names, "bind_tools가 한 번도 불리지 않았다"
        for bound in model.bound_tool_names:
            assert set(bound) == EXPECTED_TOOLS

    def test_파일시스템과_셸_도구가_하나도_없다(self):
        _result, _state, _subagent, model = run([finish(), say()])

        forbidden = {
            "ls",
            "read_file",
            "write_file",
            "edit_file",
            "delete",
            "glob",
            "grep",
            "execute",
        }
        for bound in model.bound_tool_names:
            assert not forbidden & set(bound)

    def test_위임처는_diagnosis_하나뿐이다(self):
        """general-purpose SubAgent가 살아 있으면 ``task``의 위임처가 둘이 되고,
        모델이 그쪽을 고르는 순간 Triage도 검증도 없는 결과가 리포트처럼 돌아온다."""
        result, _state, _subagent, _model = run(
            [
                ai((TASK_TOOL_NAME, {"description": "요약해 줘", "subagent_type": "general-purpose"})),
                finish(),
                say(),
            ]
        )

        assert any(DIAGNOSIS_SUBAGENT in text and "위임할 수 없다" in text for text in texts(result))


def test_초기_후보_구간이_있으면_list_candidate_windows가_돌려준다():
    """차집합 산수는 모델이 하지 않는다. 코드가 계산해 건넨다."""
    from cluster_doctor.application.service.window_planner import initial_windows

    state = IncidentState(incident_id="inc-1")
    state.pending_windows = initial_windows(TRIGGER, TRIGGER + timedelta(minutes=2))

    result, _state, _subagent, _model = run(
        [ai(("list_candidate_windows", {"limit": 3})), finish(), say()], state=state
    )

    listing = texts(result)[0]
    assert "후보 구간" in listing
    assert "남은 예산" in listing


class TestHardStop:
    """``after_model``이 그래프를 실제로 멈추는가.

    리팩터링으로 잃었다가 되찾은 성질이다. 예전 Python 루프에서는
    ``MAX_REJECTED_DECISIONS``가 ``break``였다. 루프가 모델에게 넘어가면서
    도구와 미들웨어는 거절을 **세고 문장으로 알려 줄** 뿐이 됐는데, 거절은 분도
    호출 수도 먹지 않으므로 두 예산 중 어느 것도 이 반복에 닿지 못했다 —
    남은 상한은 recursion limit 하나뿐이었고, 3회에서 끝나던 것이 20턴까지
    돌았다.
    """

    def test_연속_거절이_상한에_닿으면_그래프가_멈춘다(self):
        """모델이 이미 분석한 구간을 고집스럽게 다시 제안한다."""
        repository = InMemoryIncidentStateRepository()
        state = IncidentState(incident_id="inc-1")
        state.analyzed_windows.append(span(14, 0, 14, 10))
        state.analyzed_minutes = 10

        _result, _state, subagent, model = run(
            [propose(14, 0, 14, 10) for _ in range(20)],
            state=state,
            repository=repository,
        )

        final = repository.get("inc-1")
        # 거절 하나가 한 턴이다. 상한에 닿은 것을 확인하는 턴이 한 번 더 붙어
        # 네 턴에서 멈춘다 — recursion limit(수십 턴)이 아니라 상한이 끊었다는
        # 뜻이고, 그 차이가 이 테스트의 전부다.
        assert model.call_count == 4
        assert final.rejected_decision_count == MAX_REJECTED_DECISIONS
        assert subagent.entries == []

    def test_하드_스톱은_실패가_아니라_완료다(self):
        """거절은 분석이 깨진 것이 아니라 요청이 제약에 걸린 것이다. 실패로
        닫으면 리포트에 붉은 배너가 붙어 멀쩡한 내용까지 의심하게 되고,
        재트리거도 함께 막힌다."""
        repository = InMemoryIncidentStateRepository()
        state = IncidentState(incident_id="inc-1")
        state.analyzed_windows.append(span(14, 0, 14, 10))
        state.analyzed_minutes = 10

        run(
            [propose(14, 0, 14, 10) for _ in range(20)],
            state=state,
            repository=repository,
        )

        final = repository.get("inc-1")
        assert final.status is IncidentStatus.COMPLETED
        assert "제약" in final.closing_reason
        assert str(MAX_REJECTED_DECISIONS) in final.closing_reason

    def test_종료가_선언되면_그_뒤의_도구_호출은_실행되지_않는다(self):
        """``finish_incident`` 뒤에도 모델은 도구를 부를 수 있다. 그 호출은
        예산만 쓰고, 닫힌 Incident의 State를 뒤늦게 바꾼다."""
        repository = InMemoryIncidentStateRepository()

        result, _state, _subagent, model = run(
            [finish(), propose(15, 0, 15, 10), propose(16, 0, 16, 10), say()],
            repository=repository,
        )

        final = repository.get("inc-1")
        assert final.status is IncidentStatus.COMPLETED
        # 종료 도구의 응답 하나뿐이다. 뒤따르는 제안은 승인도 거절도 되지 않았다.
        assert len(tool_messages(result)) == 1
        assert final.analyzed_windows == []
        assert final.rejected_decision_count == 0
        # 종료 턴 + 종료를 확인하고 멈추는 턴.
        assert model.call_count == 2

    def test_같은_tool_call_id가_두_번_와도_그래프가_죽지_않는다(self):
        """이미 응답이 달린 tool_call만 남은 턴이 생기면 langchain의 분기는
        ``model``로 되돌아간다. ``after_model`` 훅이 하나도 없으면 그 목적지가
        분기 목록에 없어 ``KeyError: 'model'``로 죽는다 — 실제로 이 저장소의
        테스트 하네스가 그 모양으로 터졌다.

        지금은 Guardrail 미들웨어가 ``after_model``을 갖고 있어 목적지가
        등록된다. 그것이 우연이 아니라 보장이 되도록 여기서 못 박는다.
        """
        repeated = propose(14, 0, 14, 10)

        result, _state, _subagent, _model = run(
            [repeated, repeated, finish(), say()]
        )

        assert any(text.startswith("승인:") for text in texts(result))


def test_harness_profile_등록이_무력화돼도_도구_목록은_그대로다(monkeypatch):
    """차단이 두 겹인 이유.

    profile 등록은 우리가 만든 키와 deepagents가 조회하는 키가 같을 때만
    걸린다. 그 조회에는 fallback과 provider 정규화가 들어 있어, 버전이 오르며
    규칙이 바뀌면 등록이 아무도 읽지 않는 키에 얹힌다. 그때 등록은 **예외도
    로그도 없이** 무시되고 ``execute``와 ``write_file``이 모델 목록에 조용히
    돌아온다.

    여기서는 등록 자체를 no-op으로 만들어 그 상황을 그대로 만든다. 두 번째
    겹(``HideHarnessToolsMiddleware``)만 남은 채로도 목록이 같아야 한다.
    """
    monkeypatch.setattr(harness_module, "register_harness_profile", lambda *a, **k: None)

    # 전용 클래스를 쓴다. profile 키는 모델 클래스 이름에서 나오므로, 공용
    # ``ScriptedChatModel``을 쓰면 앞선 테스트가 등록해 둔 profile이 그대로
    # 걸려 "등록이 없어도 된다"를 검증하지 못한다.
    class NoProfileToolSurfaceModel(ScriptedChatModel):
        pass

    state = IncidentState(incident_id="inc-1")
    repository = InMemoryIncidentStateRepository()
    repository.create(state)
    model = NoProfileToolSurfaceModel(responses=[finish(), say()])

    graph = build_main_agent(
        model=model,
        tools=[
            make_list_candidate_windows_tool(state=state),
            make_propose_analysis_tool(state=state, repository=repository),
            make_finish_incident_tool(state=state, repository=repository),
        ],
        middleware=[DelegationGuardrailMiddleware(state=state, repository=repository)],
        diagnosis_subagent=CompiledSubAgent(
            name=DIAGNOSIS_SUBAGENT,
            description="승인된 구간 하나를 조사한다",
            runnable=RunnableLambda(RecordingSubAgent()),
        ),
    )
    graph.invoke(
        {
            "messages": [AIMessage(content="Incident가 열렸다")],
            INCIDENT_ID: "inc-1",
            CLUSTER: "es-prod",
            ADMITTED_WINDOW: None,
            ADMITTED_GOAL: "",
            LAST_RESPONSE: None,
        },
        {"recursion_limit": 50},
    )

    assert model.bound_tool_names
    for bound in model.bound_tool_names:
        assert set(bound) == EXPECTED_TOOLS


def test_profile이_빠져_general_purpose가_되살아나도_위임은_막힌다(monkeypatch):
    """Main Agent 쪽에는 겹이 하나 더 있다.

    harness profile이 걸리지 않으면 ``create_deep_agent``이 general-purpose
    SubAgent를 되살려 ``task``의 위임처가 둘이 된다. 도구 **이름**은 그대로
    ``task``라서 목록만 봐서는 드러나지 않는다 — 드러나는 자리는 호출이고,
    거기서 ``DelegationGuardrailMiddleware``가 막는다.
    """
    monkeypatch.setattr(harness_module, "register_harness_profile", lambda *a, **k: None)

    class NoProfileDelegateModel(ScriptedChatModel):
        pass

    state = IncidentState(incident_id="inc-1")
    repository = InMemoryIncidentStateRepository()
    repository.create(state)
    subagent = RecordingSubAgent()
    model = NoProfileDelegateModel(
        responses=[
            ai((TASK_TOOL_NAME, {"description": "대충 요약해", "subagent_type": "general-purpose"})),
            finish(),
            say(),
        ]
    )

    graph = build_main_agent(
        model=model,
        tools=[
            make_list_candidate_windows_tool(state=state),
            make_propose_analysis_tool(state=state, repository=repository),
            make_finish_incident_tool(state=state, repository=repository),
        ],
        middleware=[DelegationGuardrailMiddleware(state=state, repository=repository)],
        diagnosis_subagent=CompiledSubAgent(
            name=DIAGNOSIS_SUBAGENT,
            description="승인된 구간 하나를 조사한다",
            runnable=RunnableLambda(subagent),
        ),
    )
    result = graph.invoke(
        {
            "messages": [AIMessage(content="Incident가 열렸다")],
            INCIDENT_ID: "inc-1",
            CLUSTER: "es-prod",
            ADMITTED_WINDOW: None,
            ADMITTED_GOAL: "",
            LAST_RESPONSE: None,
        },
        {"recursion_limit": 50},
    )

    assert subagent.entries == []
    assert repository.get("inc-1").analysis_call_count == 0
    assert any(
        "위임할 수 없다" in str(m.content)
        for m in result["messages"]
        if isinstance(m, ToolMessage)
    )
