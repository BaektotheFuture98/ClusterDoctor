"""Supervisor 판단 스키마.

``prompt.py``의 ``<decision_output>``이 요구하는 다섯 칸과 정확히 같아야 한다.
한쪽만 고치면 모델은 지시대로 쓰는데 파서가 못 읽는 상태가 되고, 그때 증상은
"Supervisor가 늘 종료를 고른다"로만 나타나 원인을 짚기 어렵다.
"""

from cluster_doctor.domain.model.supervisor_decision import SupervisorAction
from cluster_doctor.infrastructure.outbound.agent.supervisor.prompt import SYSTEM_PROMPT
from cluster_doctor.infrastructure.outbound.agent.supervisor.schema import (
    DecisionOutput,
    WindowOutput,
)


def request_output(start="2026-09-18T13:50:00+09:00", end="2026-09-18T14:00:00+09:00"):
    return DecisionOutput(
        action="REQUEST_ANALYSIS",
        analysis_window=WindowOutput(start=start, end=end),
        analysis_goal="JVM pressure 시작 시점 확인",
        reason="14:00에 이미 징후가 있었다",
        based_on=["analyzed_windows", "suggested_windows"],
    )


class TestPromptAndSchemaAgree:
    def test_프롬프트가_말하는_action이_전부_스키마에_있다(self):
        for action in SupervisorAction:
            assert action.value in SYSTEM_PROMPT

    def test_프롬프트가_말하는_필드가_전부_스키마에_있다(self):
        for field in ("action", "analysis_window", "analysis_goal", "reason", "based_on"):
            assert field in SYSTEM_PROMPT
            assert field in DecisionOutput.model_fields


class TestParsing:
    def test_분석_요청을_도메인_타입으로_옮긴다(self):
        decision = request_output().to_domain()

        assert decision.action is SupervisorAction.REQUEST_ANALYSIS
        assert decision.analysis_window.start.strftime("%H:%M") == "13:50"
        assert decision.based_on == ("analyzed_windows", "suggested_windows")

    def test_action은_대소문자를_가리지_않는다(self):
        decision = DecisionOutput(action=" complete_incident ").to_domain()

        assert decision.action is SupervisorAction.COMPLETE_INCIDENT

    def test_종료에는_구간이_없어도_된다(self):
        decision = DecisionOutput(action="COMPLETE_INCIDENT", reason="충분하다").to_domain()

        assert decision.is_request() is False
        assert decision.analysis_window is None


class TestUnreadableDecisions:
    def test_모르는_action은_None이다(self):
        """예외를 올리지 않는다. 모델의 형식 이탈 하나로 Incident를 죽이지
        않고, None을 받은 orchestrator가 안전한 쪽을 고른다."""
        assert DecisionOutput(action="MAYBE_LATER").to_domain() is None

    def test_분석을_요청했는데_구간이_없으면_None이다(self):
        """구간 없는 REQUEST_ANALYSIS는 실행할 수 없다. 조용히 종료로 바꾸면
        모델이 무엇을 요청했는지가 사라진다."""
        assert DecisionOutput(action="REQUEST_ANALYSIS").to_domain() is None

    def test_읽을_수_없는_시각이면_None이다(self):
        assert request_output(start="어제 오후").to_domain() is None

    def test_뒤집힌_구간이면_None이다(self):
        assert request_output(
            start="2026-09-18T14:00:00+09:00", end="2026-09-18T13:50:00+09:00"
        ).to_domain() is None


class TestNoRetryLoop:
    def test_아무것도_안_채워도_스키마_검증은_통과한다(self):
        """required 필드 하나가 빠지면 구조화 출력 검증이 실패하고, 그 실패는
        재시도 루프가 된다."""
        assert DecisionOutput().action == ""
