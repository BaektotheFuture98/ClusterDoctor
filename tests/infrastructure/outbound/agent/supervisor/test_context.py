"""Supervisor에게 보여 주는 것.

이 스냅샷이 Supervisor Context의 전부다. 여기 없는 것은 모델에게 닿지 않는다 —
그것이 "Raw Data를 Context에 누적하지 않는다"의 코드 표현이다.
"""

from datetime import datetime, timedelta

from cluster_doctor.domain.model.incident import Incident
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisResponse,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.context import (
    build_decision_prompt,
)
from tests.domain.model.test_time_range_spans import KST, span

TRIGGER = datetime(2026, 9, 18, 14, 3, tzinfo=KST)
INCIDENT = Incident(
    incident_id="inc-1",
    cluster="es-prod",
    trigger_time=TRIGGER,
    kafka_receive_time=TRIGGER + timedelta(seconds=5),
)


def prompt(state: IncidentState, *, last_response=None, candidates=()) -> str:
    return build_decision_prompt(
        INCIDENT,
        state,
        last_response=last_response,
        candidate_windows=candidates,
        budget_note="남은 분석 호출: 5회",
    )


def analyzed_state() -> IncidentState:
    return IncidentState(
        incident_id="inc-1",
        analyzed_windows=[span(14, 0, 14, 10)],
        analysis_call_count=1,
        latest_report_ref="RPT-inc-1-1",
        latest_analysis_status=LogAnalysisStatus.NEED_MORE_CONTEXT,
        latest_verification_status=VerificationStatus.PASSED,
        evidence_refs=["E-inc-1-1", "E-inc-1-2"],
    )


class TestWhatIsShown:
    def test_분석한_구간을_싣는다(self):
        assert "14:00:00 ~ 2026-09-18T14:10:00" in prompt(analyzed_state())

    def test_참조와_건수만_싣고_Evidence_본문은_싣지_않는다(self):
        """참조가 흐르고 실체는 저장소에 남는다."""
        text = prompt(analyzed_state())

        assert "RPT-inc-1-1" in text
        assert "evidence 수: 2" in text
        assert "E-inc-1-1" not in text

    def test_코드가_계산한_후보를_싣는다(self):
        """차집합 산수를 모델에게 시키지 않는다."""
        text = prompt(analyzed_state(), candidates=(span(13, 50, 14, 0),))

        assert "13:50:00" in text
        assert "코드가 analyzed_windows를 빼고 계산했다" in text

    def test_후보가_없으면_없다고_말한다(self):
        assert "새로 분석할 구간이 남아 있지 않다" in prompt(analyzed_state())

    def test_예산을_싣는다(self):
        assert "남은 분석 호출: 5회" in prompt(analyzed_state())

    def test_직전_응답이_있으면_요약을_싣는다(self):
        response = LogAnalysisResponse(
            status=LogAnalysisStatus.NEED_MORE_CONTEXT,
            analyzed_window=span(14, 0, 14, 10),
            suggested_windows=(span(13, 50, 14, 0),),
            analysis_summary="14:00 시점에 이미 JVM pressure가 있었다",
        )

        text = prompt(analyzed_state(), last_response=response)

        assert "14:00 시점에 이미 JVM pressure가 있었다" in text
        assert "NEED_MORE_CONTEXT" in text

    def test_첫_사이클에는_응답_블록이_없다(self):
        assert "<log_analysis_response>" not in prompt(IncidentState(incident_id="inc-1"))


class TestWhatIsNotShown:
    def test_원문도_중간_결과도_싣지_않는다(self):
        """Supervisor Context에 raw 로그가 쌓이지 않는다는 것이 이 설계의
        전제다. 스냅샷에 그런 자리가 아예 없어야 한다."""
        text = prompt(analyzed_state())

        for forbidden in ("took=", "ToolMessage", "[WARN ]", "minute_results"):
            assert forbidden not in text

    def test_사이클마다_새로_그린다(self):
        """이전 사이클의 대화를 이어 붙이지 않는다. 같은 상태면 같은 스냅샷이
        나오고, Context가 분석 횟수만큼 불어나지 않는다."""
        state = analyzed_state()

        assert prompt(state) == prompt(state)
