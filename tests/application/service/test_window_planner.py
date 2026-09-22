"""SubAgent의 제안을 실제로 새로 필요한 구간으로 바꾸는 산수.

요구사항 3번이 여기 있다 — 13:50~14:05가 제안되고 14:00~14:10이 이미 분석됐다면
13:50~14:00만 고른다.
"""

from datetime import timedelta

from cluster_doctor.application.service.window_planner import (
    initial_windows,
    plan_new_windows,
)
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.contracts.time_range import TimeRange, split_span
from cluster_doctor.infrastructure.outbound.agent.diagnosis.schema import (
    DraftReport,
    WindowSuggestion,
)
from tests.contracts.test_time_range_spans import at, label, span


def state_with(*analyzed: TimeRange) -> IncidentState:
    return IncidentState(incident_id="inc-1", analyzed_windows=list(analyzed))


class TestOverlapIsSubtracted:
    def test_제안이_이미_분석한_구간과_겹치면_새_부분만_남는다(self):
        """요구사항 3번. SubAgent가 13:50~14:05를 제안했고 14:00~14:10은 이미
        분석했다면, 새로 필요한 것은 13:50~14:00뿐이다."""
        suggested = split_span(at(13, 50), at(14, 5))

        fresh = plan_new_windows(suggested, state_with(span(14, 0, 14, 10)))

        assert label(fresh) == [("13:50", "14:00")]

    def test_모델이_쓴_제안_문자열에서도_같은_답이_나온다(self):
        """실제 경로를 그대로 탄다 — 모델이 쓴 ISO 문자열 → 분할 → 차집합."""
        draft = DraftReport(
            needs_more_context=True,
            suggested_windows=[
                WindowSuggestion(
                    start_iso="2026-09-18T13:50:00+09:00",
                    end_iso="2026-09-18T14:05:00+09:00",
                )
            ],
        )

        fresh = plan_new_windows(draft.parsed_windows(), state_with(span(14, 0, 14, 10)))

        assert label(fresh) == [("13:50", "14:00")]

    def test_완전히_덮인_제안은_후보가_되지_않는다(self):
        fresh = plan_new_windows([span(14, 2, 14, 5)], state_with(span(14, 0, 14, 10)))

        assert fresh == []

    def test_겹치는_제안_둘은_먼저_합친다(self):
        """각각 빼기만 하면 서로 겹친 후보 둘이 남아 같은 분을 두 번 분석한다."""
        fresh = plan_new_windows(
            [span(13, 50, 13, 58), span(13, 55, 14, 0)], state_with()
        )

        assert label(fresh) == [("13:50", "14:00")]


class TestNoiseIsDropped:
    def test_1분_미만_꼬리는_후보로_올리지_않는다(self):
        """분 경계 반올림 때문에 몇십 초짜리 조각이 정상적으로 생긴다. 그것
        하나에 분석 호출을 쓰면 예산 여섯 번 중 한 번이 사라진다."""
        tail = TimeRange(start=at(13, 59) + timedelta(seconds=30), end=at(14, 0))

        assert plan_new_windows([tail], state_with()) == []

    def test_후보가_많으면_가까운_과거부터_남긴다(self):
        suggested = [
            span(13, 0, 13, 10),
            span(13, 10, 13, 20),
            span(13, 30, 13, 40),
            span(13, 50, 14, 0),
        ]

        fresh = plan_new_windows(suggested, state_with(), limit=2)

        assert label(fresh) == [("13:50", "14:00"), ("13:30", "13:40")]

    def test_제안이_없으면_후보도_없다(self):
        assert plan_new_windows([], state_with()) == []


class TestInitialWindows:
    def test_유입_앞으로_5분을_더_본다(self):
        """원인은 사고 구간이 아니라 그 앞에서 만들어지는 경우가 많다."""
        windows = initial_windows(at(14, 0), at(14, 3))

        assert windows[0].start == at(13, 55)

    def test_유입_끝_다음_분까지_감싼다(self):
        windows = initial_windows(at(14, 0), at(14, 3))

        assert windows[-1].end == at(14, 4)

    def test_긴_구간은_상한_이하로_쪼갠다(self):
        windows = initial_windows(at(14, 0), at(14, 30))

        assert all(w.end - w.start <= timedelta(minutes=10) for w in windows)
        assert len(windows) > 1
