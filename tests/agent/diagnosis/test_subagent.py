"""Diagnosis SubAgent — ``task``가 닿는 자리.

위임 하나가 여기서 시작해 여기서 끝난다. 확인하려는 것은 진단 품질이 아니라
**경계**다.

  하나. 분석 구간은 graph state에서만 온다. 모델이 쓴 산문에서 읽지 않는다.
  둘.   부모에게 돌려보내는 payload에 원문이 없다.
  셋.   위임 하나 안에서 비싼 일이 반복되지 않는다.

chat model만 대본으로 바꾼다. 파이프라인(수집기·triage·검증)은 진짜이고,
그 아래 LLM 호출은 ``test_report_writer``가 이미 쓰고 있는 ``ScriptedLlm``이
받는다 — 같은 재료를 두 벌 만들면 한쪽만 고쳐지는 날이 온다.
"""

from cluster_doctor.incident.models import Incident
from cluster_doctor.incident.state import IncidentState
from cluster_doctor.contracts.report import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.contracts.time_range import TimeRange
from cluster_doctor.agent.common import harness as harness_module
from cluster_doctor.agent.common.kst import format_kst
from cluster_doctor.agent.diagnosis.agent import (
    build_diagnosis_subagent,
)
from cluster_doctor.agent.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    CLUSTER,
    INCIDENT_ID,
    LAST_RESPONSE,
)
from cluster_doctor.storage.in_memory_incident_state_store import (
    InMemoryIncidentStateRepository,
)
from langchain_core.messages import HumanMessage

from tests.contracts.test_time_range_spans import span
from tests.agent.diagnosis.test_report_writer import (
    GOOD_DRAFT,
    WINDOW,
    ScriptedLlm,
    build as build_seams,
)
from tests.agent.scripted_model import ScriptedChatModel, ai, say

TRIGGER = WINDOW.start


def incident(incident_id: str = "inc-1") -> Incident:
    return Incident(
        incident_id=incident_id,
        cluster="es-prod",
        trigger_time=TRIGGER,
        kafka_receive_time=TRIGGER,
    )


def admitted(window: TimeRange = WINDOW) -> dict[str, str]:
    """``propose_analysis``가 graph state에 쓰는 것과 **같은 표기**로 만든다.

    이 표기에는 UTC offset이 없다. 여기서 ``datetime.fromisoformat``으로
    되읽으면 naive가 되고, 그 순간 위임 하나가 통째로 날아간다. 그래서 테스트도
    실제와 같은 문자열을 넣는다 — 편한 표기(offset 붙은 ISO)를 넣으면 그 갈래가
    테스트에서 사라지고, 운영에서만 터진다.
    """
    return {"start": format_kst(window.start), "end": format_kst(window.end)}


def state_in(
    *,
    window=admitted(),
    goal="느린 쿼리가 언제부터인지 본다",
    description="이 구간에서 무엇이 먼저 무너졌는지 봐라",
    incident_id="inc-1",
) -> dict:
    payload = {
        "messages": [HumanMessage(content=description)],
        INCIDENT_ID: incident_id,
        CLUSTER: "es-prod",
        ADMITTED_GOAL: goal,
    }
    if window is not None:
        payload[ADMITTED_WINDOW] = window
    return payload


def build(script, *, llm=None, state=None, incident_id="inc-1", max_revisions=1):
    llm = llm or ScriptedLlm(GOOD_DRAFT)
    seams, store = build_seams(llm, max_revisions=max_revisions)
    state = state or IncidentState(incident_id=incident_id)
    repository = InMemoryIncidentStateRepository()
    repository.create(state)
    model = ScriptedChatModel(responses=list(script))
    subagent = build_diagnosis_subagent(
        seams=seams,
        state=state,
        repository=repository,
        incident=incident(incident_id),
        model=model,
    )
    return subagent["runnable"], state, store, model, llm


COLLECT = ("collect_evidence", {})


def write(focus: str = "무엇이 먼저 무너졌나"):
    return ("write_report", {"focus": focus})


class TestAdmittedWindow:
    def test_승인된_구간으로_진단이_돈다(self):
        runnable, state, store, _model, _llm = build(
            [ai(COLLECT), ai(write()), say("끝났다")]
        )

        update = runnable.invoke(state_in())

        assert state.report_refs == [state.report_refs[-1]]
        assert store.get_report(state.report_refs[-1]) is not None
        # final_report_ref는 이 시점에서 아직 채워지지 않는다 — Main Agent의
        # finalize_report(혹은 Runner의 안전망)가 부를 때만 채워진다.
        assert state.final_report_ref is None
        assert state.latest_analysis_status is LogAnalysisStatus.COMPLETED
        # 승인을 소모한다. 남겨 두면 다음 ``task``가 승인 없이 통과한다.
        assert update[ADMITTED_WINDOW] is None

    def test_다음_위임의_state_ref는_final_report_ref가_아니라_직전_report_refs를_쓴다(self):
        """Task 11의 핵심 갈래.

        ``final_report_ref``는 이제 ``finalize_report``가 불러야만 채워지므로
        두 번째 위임 시점까지 계속 ``None``이다. 그런데도 두 번째 위임의
        분석 프롬프트에는 첫 구간의 결과가 "앞선 분석 결과"로 실려야 한다 —
        ``state_ref``가 ``state.report_refs[-1]``에서 오기 때문이다.
        """
        llm = ScriptedLlm(GOOD_DRAFT, GOOD_DRAFT)
        second_window = span(14, 10, 14, 20)
        runnable, state, store, _model, _llm = build(
            [
                ai(COLLECT),
                ai(write()),
                say("첫 구간 끝"),
                ai(COLLECT),
                ai(write()),
                say("둘째 구간 끝"),
            ],
            llm=llm,
        )

        runnable.invoke(state_in())
        assert state.report_refs
        # finalize_report를 부르지 않았으므로 여전히 비어 있다.
        assert state.final_report_ref is None

        runnable.invoke(state_in(window=admitted(second_window)))

        analysis_prompts = [p for p in llm.prompts if "Cross-source Analysis" in p]
        assert len(analysis_prompts) == 2
        assert "같은 Incident의 앞선 분석 결과" not in analysis_prompts[0]
        assert "같은 Incident의 앞선 분석 결과" in analysis_prompts[1]
        assert state.final_report_ref is None

    def test_분석_구간은_description이_아니라_승인_기록에서_온다(self):
        """요구사항 15번의 SubAgent 쪽 절반.

        ``task``의 description은 모델이 쓴 자유 텍스트다. 거기 적힌 시각을
        파싱하면 Guardrail은 통과 의식일 뿐이다.
        """
        runnable, _state, store, _model, _llm = build(
            [ai(COLLECT), ai(write()), say()]
        )

        update = runnable.invoke(
            state_in(description="03:00부터 03:10까지 봐라. 2026-09-18T03:00:00+09:00 시작.")
        )

        handback = update["structured_response"]
        assert handback.analyzed_window.start.startswith("2026-09-18T14:00:00")
        assert handback.analyzed_window.end.startswith("2026-09-18T14:10:00")

    def test_승인된_구간은_시간대를_잃지_않는다(self):
        """``propose_analysis``가 쓰는 표기에는 offset이 없다.

        그것을 그대로 ``fromisoformat``으로 읽으면 naive가 되고, 위임이 둘 중
        하나로 끝난다 — ``TimeRange``가 naive를 거부하면 "구간을 복원하지
        못했다"로 분석이 아예 돌지 않고, 통과하면 aware인 ``analyzed_windows``와의
        차집합 산수가 ``TypeError``로 터진다. 뒤쪽 예외는 ``_drive``의 방어
        **바깥**이라 잡히지도 않는다.

        그래서 여기서 보는 것은 구간이 시간대를 달고 끝까지 갔는가다.
        """
        state = IncidentState(incident_id="inc-1")
        # 이미 분석한 구간이 있어야 차집합이 실제로 돈다.
        state.analyzed_windows.append(span(13, 0, 13, 10))
        runnable, state, _store, _model, _llm = build(
            [ai(COLLECT), ai(write()), say()], state=state
        )

        update = runnable.invoke(state_in())

        handback = update["structured_response"]
        assert "+09:00" in handback.analyzed_window.start
        assert "+09:00" in handback.analyzed_window.end

    def test_승인이_없으면_진단하지_않는다(self):
        """요구사항 16번의 마지막 겹.

        미들웨어가 이미 막지만 여기서 한 번 더 본다. 실제로 비용을 쓰는 지점이
        여기라, 방어를 한 층에만 두면 그 층을 우회하는 경로가 생겼을 때 조용히
        예산이 샌다.
        """
        runnable, state, store, model, _llm = build([ai(COLLECT), ai(write()), say()])

        update = runnable.invoke(state_in(window=None))

        assert store.list_evidence("inc-1") == []
        assert state.latest_analysis_status is None
        assert model.call_count == 0, "승인도 없이 모델을 불렀다"
        assert "승인된 분석 구간이 없어" in update["messages"][0].content

    def test_읽을_수_없는_승인은_함께_소모된다(self):
        """남겨 두면 같은 깨진 값으로 위임이 반복된다."""
        runnable, _state, store, model, _llm = build([ai(COLLECT), say()])

        update = runnable.invoke(state_in(window={"start": "어제", "end": "오늘"}))

        assert update[ADMITTED_WINDOW] is None
        assert store.list_evidence("inc-1") == []
        assert model.call_count == 0


class TestCostBoundary:
    def test_근거_수집은_위임당_한_번만_실제로_돈다(self):
        """모델이 같은 도구를 다시 부르는 것은 흔한 일이다. 두 번째가 그대로
        돌면 datasource 조회도 LLM 선별도 한 번 더 일어나고, 미들웨어가 선점한
        예산 하나 안에서 비용만 배가 된다."""
        once, _s1, store_once, _m1, llm_once = build([ai(COLLECT), ai(write()), say()])
        once.invoke(state_in())

        twice, _s2, store_twice, _m2, llm_twice = build(
            [ai(COLLECT), ai(COLLECT), ai(write()), say()]
        )
        twice.invoke(state_in())

        assert store_once.list_evidence("inc-1"), "근거가 하나도 모이지 않았다"
        # 수집이 실제로 두 번 돌았다면 datasource별 LLM 선별이 한 벌 더 쌓이고
        # 같은 근거가 저장소에 두 벌 들어간다.
        assert len(llm_twice.prompts) == len(llm_once.prompts)
        assert len(store_twice.list_evidence("inc-1")) == len(
            store_once.list_evidence("inc-1")
        )

    def test_리포트_재작성에는_상한이_있다(self):
        """초안이 마음에 들지 않는다고 처음부터 다시 쓰는 것을 막는다.

        리포트 **안쪽**의 수정 횟수(``MAX_REPORT_REVISIONS``)와는 다른 상한이다.
        """
        llm = ScriptedLlm(GOOD_DRAFT, GOOD_DRAFT, GOOD_DRAFT)
        runnable, _state, _store, _model, llm = build(
            [ai(COLLECT), ai(write("첫 질문")), ai(write("둘째")), ai(write("셋째")), say()],
            llm=llm,
        )

        runnable.invoke(state_in())

        assert llm.draft_calls == 2, "리포트 작성 상한을 넘겨 초안을 썼다"

    def test_근거를_모으기_전에는_리포트를_쓸_수_없다(self):
        llm = ScriptedLlm(GOOD_DRAFT)
        runnable, _state, _store, _model, llm = build(
            [ai(write()), say()], llm=llm
        )

        runnable.invoke(state_in())

        assert llm.draft_calls == 0


class TestObservations:
    def test_관측값은_리포트와_별개로_저장소에_누적된다(self):
        """이미 조회 비용을 치른 결과이고, 다음 위임이 읽는 값이다.

        리포트를 못 썼어도 남는다. 여기서 빠지면 운영자 리포트의 타임라인과
        후보 표가 통째로 비고, 그 사실은 어디에도 드러나지 않는다.
        """
        runnable, _state, store, _model, _llm = build([ai(COLLECT), say("여기까지")])

        runnable.invoke(state_in())

        observations = store.get_observations("inc-1")
        assert observations.timeline
        assert observations.candidates


class TestHandback:
    def test_돌려보내는_payload에_원문이_없다(self):
        """``ArtifactStore`` 간접참조가 존재하는 이유 전체가 이것이다.

        Main Agent의 Context에 raw 로그가 쌓이면 사이클마다 Context가 불어나고
        비용 경계가 사라진다.
        """
        runnable, _state, _store, _model, _llm = build(
            [ai(COLLECT), ai(write()), say()]
        )

        update = runnable.invoke(state_in())

        payload = update[LAST_RESPONSE]
        text = update["messages"][0].content
        assert payload["evidence_ref_count"] >= 1
        # 근거 본문(슬로우로그 쿼리 원문)은 어디에도 없어야 한다.
        assert "match_all" not in str(payload)
        assert "match_all" not in text
        assert "evidence" not in payload or isinstance(payload["evidence_ref_count"], int)

    def test_도구를_하나도_부르지_않으면_실패로_끝난다(self):
        """COMPLETED로 나가면 Main Agent가 "이 구간은 봤다"로 읽는다. 그러면
        보지 않은 시간이 조용히 덮인 것으로 남는다."""
        runnable, state, _store, _model, _llm = build([say("아무것도 하지 않았다")])

        update = runnable.invoke(state_in())

        assert update[LAST_RESPONSE]["status"] == str(LogAnalysisStatus.FAILED)
        assert state.latest_analysis_status is LogAnalysisStatus.FAILED
        assert update[LAST_RESPONSE]["unresolved_gaps"], "보지 않은 구간이 표시되지 않았다"

    def test_리포트를_못_써도_검증은_통과로_기록되지_않는다(self):
        runnable, state, _store, _model, _llm = build([ai(COLLECT), say()])

        runnable.invoke(state_in())

        assert state.latest_verification_status is VerificationStatus.NOT_VERIFIED

    def test_확장_요청은_제안일_뿐_범위를_넓히지_않는다(self):
        runnable, state, _store, _model, _llm = build(
            [
                ai(COLLECT),
                ai(
                    (
                        "report_insufficient",
                        {
                            "reason": "이 구간만으로는 시작점을 알 수 없다",
                            "suggested_windows": [
                                "2026-09-18T13:50:00+09:00/2026-09-18T14:00:00+09:00"
                            ],
                        },
                    )
                ),
                say(),
            ]
        )

        update = runnable.invoke(state_in())

        assert update[LAST_RESPONSE]["status"] == str(LogAnalysisStatus.NEED_MORE_CONTEXT)
        # 제안은 State의 pending으로만 들어간다. 분석이 거기까지 넓어지지 않는다.
        assert state.pending_windows == [span(13, 50, 14, 0)]
        assert state.analyzed_windows == []

    def test_예산은_여기서_세지_않는다(self):
        """``analysis_call_count``와 ``analyzed_minutes``는 미들웨어가 위임을
        승인하는 순간 이미 선점했다. 여기서 또 세면 한 번의 분석이 두 번으로
        회계되어 실제 예산이 절반으로 줄어든다."""
        runnable, state, _store, _model, _llm = build(
            [ai(COLLECT), ai(write()), say()]
        )

        runnable.invoke(state_in())

        assert state.analysis_call_count == 0
        assert state.analyzed_minutes == 0
        assert state.analyzed_windows == []


class TestLoopFailure:
    def test_루프가_예외로_죽어도_결과가_조립된다(self):
        """포트 계약이 "예외를 올리지 않는다"인 이유가 여기에도 있다. 루프가
        죽어도 그때까지 모은 근거는 유효하고, 버리면 이미 쓴 LLM 비용까지
        함께 버리는 것이다."""

        class ExplodingModel(ScriptedChatModel):
            def _generate(self, messages, stop=None, run_manager=None, **kwargs):
                raise RuntimeError("provider가 429를 돌려줬다")

        seams, store = build_seams(ScriptedLlm(GOOD_DRAFT))
        state = IncidentState(incident_id="inc-1")
        repository = InMemoryIncidentStateRepository()
        repository.create(state)
        subagent = build_diagnosis_subagent(
            seams=seams,
            state=state,
            repository=repository,
            incident=incident(),
            model=ExplodingModel(responses=[say()]),
        )

        update = subagent["runnable"].invoke(state_in())

        assert update[LAST_RESPONSE]["status"] == str(LogAnalysisStatus.FAILED)
        assert any("진단 루프가 실패했다" in gap for gap in update[LAST_RESPONSE]["gaps"])
        # 실패해도 승인은 소모한다.
        assert update[ADMITTED_WINDOW] is None


class TestSuggestedWindowParsing:
    """``report_insufficient``의 구간 문자열은 **모델이 쓴다.**

    거기에 offset이 붙어 올 것이라고 가정할 근거가 없다. 프롬프트가 KST를
    지시하더라도 모델은 지시를 어기고, 그 값은 도구 인자 검증을 그대로
    통과한다 — 형식이 틀린 것이 아니라 시간대가 빠진 것뿐이기 때문이다.
    """

    def test_offset_없이_적어_보낸_구간도_KST로_읽힌다(self):
        runnable, state, _store, _model, _llm = build(
            [
                ai(COLLECT),
                ai(write()),
                ai(
                    (
                        "report_insufficient",
                        {
                            "reason": "시작점을 알 수 없다",
                            # offset이 없다. 모델이 실제로 이렇게 보낸다.
                            "suggested_windows": ["2026-09-18T13:40:00/2026-09-18T13:50:00"],
                        },
                    )
                ),
                say(),
            ]
        )

        update = runnable.invoke(state_in())

        assert state.pending_windows == [span(13, 40, 13, 50)]
        for window in state.pending_windows:
            assert window.start.utcoffset() is not None
            assert window.end.utcoffset() is not None
        assert all(
            "+09:00" in ref["start"] and "+09:00" in ref["end"]
            for ref in update[LAST_RESPONSE]["suggested_windows"]
        )

    def test_offset_없는_확장_요청이_리포트를_잃게_하지_않는다(self):
        """**이것이 이 파일에서 가장 비싼 실패였다.**

        naive 구간은 aware인 ``analyzed_windows``와 비교되지 못해
        ``_apply_response``의 차집합에서 터진다. 그 예외가 나는 자리는
        ``report_refs``에 이미 채운 **뒤**, ``repository.save`` **앞**이다 —
        리포트는 ArtifactStore에 멀쩡히 있는데 저장소에도 운영자에게도 닿지
        않는다. 운영자가 보는 것은 "리포트 없음" 알림 하나뿐이다.
        """
        state = IncidentState(incident_id="inc-1")
        # 차집합이 실제로 돌려면 이미 분석한 구간이 있어야 한다.
        state.analyzed_windows.append(span(13, 0, 13, 10))
        runnable, state, store, _model, _llm = build(
            [
                ai(COLLECT),
                ai(write()),
                ai(
                    (
                        "report_insufficient",
                        {
                            "reason": "앞 구간이 필요하다",
                            "suggested_windows": ["2026-09-18T13:40:00/2026-09-18T13:50:00"],
                        },
                    )
                ),
                say(),
            ],
            state=state,
        )

        update = runnable.invoke(state_in())

        assert state.report_refs, "리포트가 저장소 갱신 전에 사라졌다"
        assert store.get_report(state.report_refs[-1]) is not None
        assert update[LAST_RESPONSE]["report_ref"] == state.report_refs[-1]
        assert update[LAST_RESPONSE]["status"] == str(LogAnalysisStatus.NEED_MORE_CONTEXT)

    def test_읽을_수_없는_구간은_거절될_뿐_위임을_깨지_않는다(self):
        runnable, state, _store, _model, _llm = build(
            [
                ai(COLLECT),
                ai(
                    (
                        "report_insufficient",
                        {"reason": "모르겠다", "suggested_windows": ["어제/오늘", "쓰레기"]},
                    )
                ),
                say(),
            ]
        )

        update = runnable.invoke(state_in())

        assert state.pending_windows == []
        assert update[LAST_RESPONSE]["status"] in {
            str(LogAnalysisStatus.FAILED),
            str(LogAnalysisStatus.COMPLETED),
            str(LogAnalysisStatus.NEED_MORE_CONTEXT),
        }


def test_harness_기본_도구는_진단_SubAgent에도_보이지_않는다(monkeypatch):
    """Main과 Diagnosis 둘 다 ``create_deep_agent``이고, 둘 다 막혀야 한다.

    한쪽만 막으면 다른 쪽이 그대로 열려 있고 실제로 한동안 그랬다. 여기서는
    harness profile 등록을 no-op으로 만들어 두 번째 겹
    (``HideHarnessToolsMiddleware``)만 남긴다 — 등록 키 규칙이 바뀌어 profile이
    조용히 무시되는 날 SubAgent 쪽이 먼저 열리기 때문이다.
    """
    monkeypatch.setattr(harness_module, "register_harness_profile", lambda *a, **k: None)

    # profile 키는 모델 클래스 이름에서 나온다. 공용 클래스를 쓰면 앞선 테스트가
    # 남긴 등록이 그대로 걸려 이 테스트가 아무것도 검증하지 못한다.
    class NoProfileDiagnosisModel(ScriptedChatModel):
        pass

    seams, _store = build_seams(ScriptedLlm(GOOD_DRAFT))
    state = IncidentState(incident_id="inc-1")
    repository = InMemoryIncidentStateRepository()
    repository.create(state)
    model = NoProfileDiagnosisModel(responses=[ai(COLLECT), say()])

    subagent = build_diagnosis_subagent(
        seams=seams,
        state=state,
        repository=repository,
        incident=incident(),
        model=model,
    )
    subagent["runnable"].invoke(state_in())

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
    assert model.bound_tool_names, "bind_tools가 한 번도 불리지 않았다"
    for bound in model.bound_tool_names:
        assert not forbidden & set(bound)
        assert {"collect_evidence", "write_report", "report_insufficient"} <= set(bound)


def test_profile이_빠지면_진단_SubAgent에_general_purpose_위임처가_되살아난다(monkeypatch):
    """**두 번째 겹이 덮지 못하는 자리.** 현재 동작을 기록해 둔다.

    ``HideHarnessToolsMiddleware``는 이름이 박힌 파일·셸 도구 여덟 개만 지운다.
    ``create_deep_agent``이 자동으로 끼워 넣는 general-purpose SubAgent는 끄지
    못한다 — 그것을 끄는 것은 harness profile 쪽 일이고, profile 키가 어긋나면
    진단 SubAgent에 ``task``가 되살아난다.

    Main Agent에서는 이 갈래가 막혀 있다. ``DelegationGuardrailMiddleware``가
    ``subagent_type != "diagnosis"``인 위임을 거절하기 때문이다. **진단
    SubAgent에는 그런 미들웨어가 없다.** 되살아난 ``task``로 넘어간 작업은
    Triage도 Evidence 선별도 검증도 거치지 않고, 예산은 이미 선점된 위임 하나
    안에서 그대로 나간다.

    profile이 정상 등록되는 한 이 갈래는 열리지 않으므로 지금 고칠 일은
    아니다. 다만 "차단은 두 겹"이라는 문장이 이 항목에는 해당하지 않는다는
    사실을 테스트로 남긴다 — 겹이 하나 빠지는 날 무엇이 열리는지 알아야 한다.
    """
    monkeypatch.setattr(harness_module, "register_harness_profile", lambda *a, **k: None)

    class GapProbeChatModel(ScriptedChatModel):
        pass

    seams, _store = build_seams(ScriptedLlm(GOOD_DRAFT))
    state = IncidentState(incident_id="inc-1")
    repository = InMemoryIncidentStateRepository()
    repository.create(state)
    model = GapProbeChatModel(responses=[say()])

    subagent = build_diagnosis_subagent(
        seams=seams,
        state=state,
        repository=repository,
        incident=incident(),
        model=model,
    )
    subagent["runnable"].invoke(state_in())

    assert "task" in set(model.bound_tool_names[0]), (
        "이 갈래가 막혔다면 위 docstring과 함께 이 테스트를 지워라"
    )
