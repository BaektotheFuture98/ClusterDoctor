"""``IncidentAnalyzer`` 포트 뒤의 Main DeepAgent 어댑터.

여기서 묻는 것은 두 가지다.

  하나. Main DeepAgent → ``task`` → **진짜 Diagnosis SubAgent**까지 배선이
        이어져 있는가. 위 계층의 어떤 테스트도 이 경로 전체를 태우지 않는다.
  둘.   Agent가 어떻게 끝나든 이 어댑터가 **예외를 올리지 않는가**. 포트
        계약이 그것이고, 계약이 깨지면 코드가 모아 둔 관측값까지 함께 사라진다.

chat model만 대본이다. 그래프도 미들웨어도 진단 파이프라인도 전부 진짜다.
"""

from cluster_doctor.domain.incident.models import Incident, IncidentStatus
from cluster_doctor.domain.incident.state import IncidentState
from cluster_doctor.domain.diagnosis.report import LogAnalysisStatus
from cluster_doctor.adapters.outbound.deepagents import adapter as agent_module
from cluster_doctor.adapters.outbound.deepagents.adapter import (
    DeepAgentIncidentAnalyzer,
)
from cluster_doctor.adapters.outbound.deepagents.supervisor.state import DIAGNOSIS_SUBAGENT
from cluster_doctor.adapters.outbound.deepagents.supervisor.tools import TASK_TOOL_NAME
from cluster_doctor.storage.in_memory_incident_state_store import (
    InMemoryIncidentStateRepository,
)

from tests.adapters.outbound.deepagents.diagnosis.pipeline.test_report_writer import (
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


def propose(start: str, end: str, goal: str = "확인"):
    return ai(("propose_analysis", {"start_kst": start, "end_kst": end, "goal": goal}))


def delegate(description: str = "무엇이 먼저 무너졌는지 본다"):
    return ai(
        (TASK_TOOL_NAME, {"description": description, "subagent_type": DIAGNOSIS_SUBAGENT})
    )


def finish(outcome: str = "COMPLETED", reason: str = "충분하다"):
    return ai(("finish_incident", {"outcome": outcome, "reason": reason}))


def finalize():
    return ai(("finalize_report", {}))


def build(script, *, monkeypatch, llm=None, recursion_limit=60):
    """대본 chat model을 물린 진짜 어댑터.

    ``build_chat_model``만 바꾼다. 그 함수는 ``ChatLiteLLM``을 만들고 provider
    검증까지 하는 자리라 테스트에서 부를 수 없지만, 그 아래 조립은 전부 실제
    코드가 해야 한다 — 어댑터가 무엇을 어떻게 묶는지가 이 파일의 대상이다.
    """
    model = ScriptedChatModel(responses=list(script))
    monkeypatch.setattr(agent_module, "build_chat_model", lambda **_kwargs: model)

    seams, store = build_seams(llm or ScriptedLlm(GOOD_DRAFT))
    repository = InMemoryIncidentStateRepository()
    adapter = DeepAgentIncidentAnalyzer(
        provider="gemini",
        model="x",
        api_key="y",
        seams=seams,
        state_repository=repository,
        recursion_limit=recursion_limit,
    )
    return adapter, repository, store, model


def run(adapter, repository, incident_id: str = "inc-1"):
    state = IncidentState(incident_id=incident_id)
    repository.create(state)
    return adapter.analyze(incident(incident_id)), state


def test_analyze_reads_only_the_matching_repository_state(monkeypatch):
    """A caller supplies an immutable Incident, never a mutable state object.

    If the adapter compiled its graph with caller-owned state, or fetched a state
    without using the incident id, closing this incident would also change the
    untouched incident below.
    """
    adapter, repository, _store, _model = build(
        [finish(), say("요약")], monkeypatch=monkeypatch
    )
    untouched = IncidentState(incident_id="inc-untouched")
    repository.create(untouched)
    repository.create(IncidentState(incident_id="inc-target"))

    result = adapter.analyze(incident("inc-target"))

    assert result.status is IncidentStatus.COMPLETED
    assert repository.get("inc-target").status is IncidentStatus.COMPLETED
    assert repository.get("inc-untouched") == untouched


class TestEndToEndDelegation:
    def test_Main에서_task로_진짜_진단_SubAgent까지_간다(self, monkeypatch):
        """요구사항 2번의 가장 넓은 확인.

        대본은 Main의 턴과 SubAgent의 턴을 **한 줄로** 잇는다. 실제 실행에서
        그 둘은 같은 chat model을 쓰고, ``task`` 도구 호출 하나가 그 사이를
        건넌다. 배선이 끊겨 있으면 SubAgent의 도구는 한 번도 불리지 않는다.
        """
        adapter, repository, store, _model = build(
            [
                propose("2026-09-18T14:00:00+09:00", "2026-09-18T14:10:00+09:00", "시작점"),
                delegate(),
                # 여기부터 SubAgent의 턴이다.
                ai(("collect_evidence", {})),
                ai(("write_report", {"focus": "무엇이 먼저 무너졌나"})),
                say("진단을 마쳤다"),
                # 다시 Main.
                finish(),
                say("요약"),
            ],
            monkeypatch=monkeypatch,
        )

        result, _state = run(adapter, repository)

        final = repository.get("inc-1")
        assert result.status is IncidentStatus.COMPLETED
        assert result.failed is False
        assert final.analysis_call_count == 1
        assert final.analyzed_minutes == 10
        assert final.latest_analysis_status is LogAnalysisStatus.COMPLETED
        # finalize_report를 부르지 않고 finish_incident로 곧장 닫았다 —
        # final_report_ref는 이 어댑터 층에서는 채워지지 않는다. 안전망은
        # IncidentRunner._deliver의 몫이다.
        assert final.final_report_ref is None
        assert final.report_refs
        assert store.get_report(final.report_refs[-1]) is not None
        assert store.list_evidence("inc-1"), "SubAgent의 근거 수집이 돌지 않았다"

    def test_finalize_report를_부르면_구간별_보고서를_모은_최종_보고서가_확정된다(
        self, monkeypatch
    ):
        """Task 11. Main Agent가 명시적으로 확정해야 ``final_report_ref``가 찬다."""
        adapter, repository, store, _model = build(
            [
                propose("2026-09-18T14:00:00+09:00", "2026-09-18T14:10:00+09:00", "시작점"),
                delegate(),
                ai(("collect_evidence", {})),
                ai(("write_report", {"focus": "무엇이 먼저 무너졌나"})),
                say("진단을 마쳤다"),
                finalize(),
                finish(),
                say("요약"),
            ],
            monkeypatch=monkeypatch,
        )

        run(adapter, repository)

        final = repository.get("inc-1")
        assert final.report_refs
        assert final.final_report_ref is not None
        # 확정된 참조는 구간별 참조 그 자체가 아니라 병합된 새 보고서다.
        assert final.final_report_ref not in final.report_refs
        merged = store.get_report(final.final_report_ref)
        assert merged is not None


class TestTermination:
    def test_종료를_선언하면_그대로_결과가_된다(self, monkeypatch):
        adapter, repository, _store, _model = build(
            [finish("FAILED", "근거를 얻지 못했다"), say()], monkeypatch=monkeypatch
        )

        result, _state = run(adapter, repository)

        assert result.status is IncidentStatus.FAILED
        assert result.reason == "근거를 얻지 못했다"
        assert result.failed is True

    def test_종료를_선언하지_않고_끝나면_실패로_보지_않는다(self, monkeypatch):
        """리포트에 붉은 배너를 다는 것은 분석이 깨졌을 때이고, 종료 선언을
        빠뜨린 것은 그것과 다르다."""
        adapter, repository, _store, _model = build([say("다 봤다")], monkeypatch=monkeypatch)

        result, _state = run(adapter, repository)

        assert result.status is IncidentStatus.COMPLETED
        assert result.failed is False


class TestRunaway:
    def test_폭주하는_모델은_재귀_상한에서_끊긴다(self, monkeypatch):
        """요구사항 10번.

        거절은 분 예산도 호출 수도 늘리지 않는다. 그래서 같은 제안을 되풀이하는
        모델에는 **예산 상한이 닿지 않는다** — 그 갈래를 끊는 것은 재귀 상한
        하나뿐이고, 그것이 없으면 Incident 하나가 영원히 돈다.
        """
        script = [
            propose("2026-09-18T14:00:00+09:00", "2026-09-18T13:00:00+09:00", "잘못된 구간")
            for _ in range(40)
        ]
        adapter, repository, _store, model = build(
            script, monkeypatch=monkeypatch, recursion_limit=8
        )

        result, _state = run(adapter, repository)

        assert result.status is IncidentStatus.FAILED
        assert result.failed is True
        assert "GraphRecursionError" in result.reason
        # 상한이 실제로 끊었다는 증거. 대본을 다 태우지 않았다.
        assert model.call_count < len(script)

    def test_분석이_한_번이라도_돌았으면_그래프가_깨져도_실패로_적지_않는다(
        self, monkeypatch
    ):
        """모델 쪽 사고를 분석 실패로 기록하면 재트리거가 막힌다. 확보한 근거와
        관측값이 있으면 리포트는 나가야 한다."""
        script = [
            propose("2026-09-18T14:00:00+09:00", "2026-09-18T14:01:00+09:00"),
            delegate(),
            say("진단 종료"),
        ]
        # 위임 뒤로 대본이 끝나면 모델이 도구 없는 답만 반복한다 — 그래도
        # Main은 종료를 선언하지 않은 채 멈춘다.
        adapter, repository, _store, _model = build(
            script + [propose("2026-09-18T14:02:00+09:00", "2026-09-18T14:01:00+09:00")] * 1,
            monkeypatch=monkeypatch,
            recursion_limit=60,
        )

        result, _state = run(adapter, repository)

        assert repository.get("inc-1").analysis_call_count == 1
        assert result.failed is False

    def test_예외가_밖으로_새지_않는다(self, monkeypatch):
        """포트 계약이다. 여기서 예외가 새면 Runner가 잡더라도 그 뒤의 회계가
        모두 추측이 된다."""

        class ExplodingModel(ScriptedChatModel):
            def _generate(self, messages, stop=None, run_manager=None, **kwargs):
                raise RuntimeError("provider가 500을 돌려줬다")

        model = ExplodingModel(responses=[say()])
        monkeypatch.setattr(agent_module, "build_chat_model", lambda **_kwargs: model)
        seams, _store = build_seams(ScriptedLlm(GOOD_DRAFT))
        repository = InMemoryIncidentStateRepository()
        adapter = DeepAgentIncidentAnalyzer(
            provider="gemini",
            model="x",
            api_key="y",
            seams=seams,
            state_repository=repository,
        )

        result, _state = run(adapter, repository)

        assert result.status is IncidentStatus.FAILED
        assert result.failed is True
        assert "RuntimeError" in result.reason


def test_그래프는_Incident마다_새로_만들어진다(monkeypatch):
    """도구와 미들웨어가 그 Incident의 ``IncidentState``를 클로저로 쥔다.

    전역에 하나를 만들어 두고 incident_id를 인자로 받게 하면 모델이 그 인자를
    채우게 되고, 남의 Incident 예산을 쓰는 길이 열린다.
    """
    script = []
    for incident_index in range(2):
        script.extend(
            [
                propose("2026-09-18T14:00:00+09:00", "2026-09-18T14:10:00+09:00"),
                ai((TASK_TOOL_NAME, {"description": "본다", "subagent_type": DIAGNOSIS_SUBAGENT})),
                ai(("collect_evidence", {})),
                say("근거만 모았다"),
                finish(),
                say("요약"),
            ]
        )
    adapter, repository, _store, _model = build(
        script, monkeypatch=monkeypatch, llm=ScriptedLlm(GOOD_DRAFT, GOOD_DRAFT)
    )

    run(adapter, repository, "inc-1")
    run(adapter, repository, "inc-2")

    first = repository.get("inc-1")
    second = repository.get("inc-2")
    assert first.analysis_call_count == 1
    assert second.analysis_call_count == 1
    # 같은 구간을 다시 분석한다 — 앞 Incident의 중복 판정이 새 Incident로
    # 새어 나가지 않는다.
    assert first.analyzed_windows == second.analyzed_windows
