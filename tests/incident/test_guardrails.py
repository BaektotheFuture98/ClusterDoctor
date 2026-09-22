"""런타임이 강제하는 상한.

요구사항 2번(중복 차단)과 9번(분석 호출 상한)의 판정부가 여기 있다.
프롬프트가 아니라 코드가 막는다는 것을 확인한다.
"""

import time

import pytest

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.incident.guardrails import (
    MAX_ANALYSIS_CALLS,
    MAX_ANALYZED_MINUTES,
    MAX_SINGLE_WAIT_SECONDS,
    MAX_TOTAL_WAIT_SECONDS,
    CancellationToken,
    Deadline,
    check_analysis_budget,
    check_not_duplicate,
    clamp_evidence,
    clamp_wait,
    fit_to_budget,
    remaining_minutes,
    truncate_raw,
    window_minutes,
)
from cluster_doctor.incident.state import IncidentState
from cluster_doctor.agent.contracts import LogAnalysisRequest
from tests.contracts.test_time_range_spans import span


def request_for(window, incident_id="inc-1", cluster="es-prod") -> LogAnalysisRequest:
    return LogAnalysisRequest(
        incident_id=incident_id, cluster=cluster, analysis_window=window
    )


class TestDuplicateWindow:
    def test_이미_분석한_구간을_다시_요청하면_거절한다(self):
        """요구사항 2번."""
        state = IncidentState(
            incident_id="inc-1", analyzed_windows=[span(14, 0, 14, 10)]
        )

        with pytest.raises(GuardrailViolation):
            check_not_duplicate(span(14, 0, 14, 10), state)

    def test_덮인_부분_구간도_거절한다(self):
        """완전히 같은 구간만 막으면 14:02~14:05가 통과한다. 새로 얻는 것이
        없으면서 조회와 분당 LLM 호출 비용은 그대로 다시 든다."""
        state = IncidentState(
            incident_id="inc-1", analyzed_windows=[span(14, 0, 14, 10)]
        )

        with pytest.raises(GuardrailViolation):
            check_not_duplicate(span(14, 2, 14, 5), state)

    def test_새_구간은_통과한다(self):
        state = IncidentState(
            incident_id="inc-1", analyzed_windows=[span(14, 0, 14, 10)]
        )

        check_not_duplicate(span(13, 50, 14, 0), state)


class TestAnalysisBudget:
    def test_예산은_분으로_센다(self):
        """비용 동인은 호출이 아니라 분이다 — 조회가 분 단위로 쪼개지고,
        비어 있지 않은 분마다 datasource별로 LLM이 한 번씩 돈다."""
        assert window_minutes(span(14, 0, 14, 10)) == 10
        assert window_minutes(span(14, 0, 14, 1)) == 1

    def test_분_예산을_다_쓰면_더_부를_수_없다(self):
        """요구사항 9번. NEED_MORE_CONTEXT가 반복돼도 예산은 코드가 막는다."""
        state = IncidentState(
            incident_id="inc-1", analyzed_minutes=MAX_ANALYZED_MINUTES
        )

        with pytest.raises(GuardrailViolation):
            check_analysis_budget(state)

    def test_예산이_남으면_통과한다(self):
        check_analysis_budget(
            IncidentState(incident_id="inc-1", analyzed_minutes=MAX_ANALYZED_MINUTES - 1)
        )

    def test_호출_수_상한도_2차로_남아_있다(self):
        """1분짜리 요청을 수십 번 하는 폭주는 분 예산만으로는 늦게 잡힌다."""
        state = IncidentState(
            incident_id="inc-1", analysis_call_count=MAX_ANALYSIS_CALLS
        )

        with pytest.raises(GuardrailViolation):
            check_analysis_budget(state)

    def test_예산에_맞게_구간을_줄인다(self):
        """통째로 거절하면 남은 예산을 쓰지 못한 채 끝난다."""
        state = IncidentState(
            incident_id="inc-1", analyzed_minutes=MAX_ANALYZED_MINUTES - 4
        )

        fitted = fit_to_budget(span(14, 0, 14, 10), state)

        assert window_minutes(fitted) == 4
        # 앞쪽을 남긴다 — Supervisor가 시작 시각을 의도해서 고른다.
        assert fitted.start == span(14, 0, 14, 10).start

    def test_예산_안에_들어오면_그대로_둔다(self):
        window = span(14, 0, 14, 10)
        state = IncidentState(incident_id="inc-1")

        assert fit_to_budget(window, state) is window

    def test_예산이_없으면_줄이지_않고_거절한다(self):
        state = IncidentState(
            incident_id="inc-1", analyzed_minutes=MAX_ANALYZED_MINUTES
        )

        with pytest.raises(GuardrailViolation):
            fit_to_budget(span(14, 0, 14, 10), state)

    def test_남은_예산을_알려준다(self):
        assert remaining_minutes(IncidentState(incident_id="inc-1")) == (
            MAX_ANALYZED_MINUTES
        )


class TestWaitBudget:
    def test_1회_상한으로_줄인다(self):
        state = IncidentState(incident_id="inc-1")

        assert clamp_wait(600, state) == MAX_SINGLE_WAIT_SECONDS

    def test_누적_예산을_넘기지_않는다(self):
        state = IncidentState(
            incident_id="inc-1", total_wait_seconds=MAX_TOTAL_WAIT_SECONDS - 5
        )

        assert clamp_wait(60, state) == 5

    def test_예산을_다_쓰면_0이다(self):
        state = IncidentState(
            incident_id="inc-1", total_wait_seconds=MAX_TOTAL_WAIT_SECONDS
        )

        assert clamp_wait(60, state) == 0


class TestVolumeCaps:
    def test_Evidence를_상한으로_자른다(self):
        assert len(clamp_evidence(list(range(100)), 10, what="test")) == 10

    def test_상한_이하면_그대로_둔다(self):
        items = list(range(3))

        assert clamp_evidence(items, 10, what="test") is items

    def test_원문을_자를_때_잘린_사실을_적는다(self):
        """조용히 자르면 검증이 "인용이 원문에 없다"고 잘못 말한다."""
        result = truncate_raw("가" * 100, limit=10)

        assert result.startswith("가" * 10)
        assert "잘랐다" in result


class TestCancellation:
    def test_취소되면_예외를_올린다(self):
        token = CancellationToken()
        token.cancel("운영자 중단")

        assert token.is_cancelled
        with pytest.raises(GuardrailViolation, match="운영자 중단"):
            token.raise_if_cancelled()

    def test_취소_전에는_조용하다(self):
        CancellationToken().raise_if_cancelled()


class TestDeadline:
    def test_시간이_남으면_통과한다(self):
        Deadline(60).raise_if_expired("분석")

    def test_시간이_다하면_거절한다(self):
        deadline = Deadline(0.01)
        time.sleep(0.02)

        assert deadline.expired
        with pytest.raises(GuardrailViolation):
            deadline.raise_if_expired("분석")
