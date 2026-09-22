"""구간 대수. 중복 분석을 막는 산수가 전부 여기 있다."""

from datetime import datetime, timedelta

from cluster_doctor.contracts.time_range import (
    MAX_TIME_RANGE_DURATION,
    TimeRange,
    is_covered,
    merge_spans,
    split_span,
    subtract_spans,
)
from cluster_doctor.infrastructure.outbound.agent.common.kst import KST


def at(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 18, hour, minute, tzinfo=KST)


def span(from_h, from_m, to_h, to_m) -> TimeRange:
    return TimeRange(start=at(from_h, from_m), end=at(to_h, to_m))


def label(windows) -> list[tuple[str, str]]:
    return [(w.start.strftime("%H:%M"), w.end.strftime("%H:%M")) for w in windows]


class TestSubtract:
    def test_겹치지_않으면_그대로_남는다(self):
        assert label(subtract_spans(span(13, 0, 13, 10), [span(14, 0, 14, 10)])) == [
            ("13:00", "13:10")
        ]

    def test_앞쪽만_새로_필요한_경우(self):
        """이 저장소가 가장 자주 만나는 모양이다.

        14:00~14:10을 분석한 뒤 "그 전부터 징후가 있었다"는 답이 오면 제안은
        13:50~14:05처럼 겹쳐서 온다. 새로 볼 것은 13:50~14:00뿐이다.
        """
        assert label(subtract_spans(span(13, 50, 14, 0), [span(14, 0, 14, 10)])) == [
            ("13:50", "14:00")
        ]

    def test_가운데가_덮이면_양쪽이_남는다(self):
        remaining = subtract_spans(span(14, 0, 14, 10), [span(14, 3, 14, 6)])
        assert label(remaining) == [("14:00", "14:03"), ("14:06", "14:10")]

    def test_통째로_덮이면_아무것도_남지_않는다(self):
        assert subtract_spans(span(14, 2, 14, 5), [span(14, 0, 14, 10)]) == []

    def test_길이가_0이_되는_조각은_버린다(self):
        """``TimeRange``가 거부하기도 하지만, 그보다 "새로 볼 것이 없다"는 뜻이다."""
        assert subtract_spans(span(14, 0, 14, 10), [span(14, 0, 14, 10)]) == []


class TestIsCovered:
    def test_부분_구간도_덮인_것으로_본다(self):
        """완전히 같은 구간만 막으면 14:02~14:05가 통과한다. 그 호출은 새로
        얻는 것이 없으면서 분당 LLM 호출 비용을 그대로 다시 쓴다."""
        assert is_covered(span(14, 2, 14, 5), [span(14, 0, 14, 10)])

    def test_쪼개진_분석의_합집합도_덮는다(self):
        assert is_covered(
            span(14, 0, 14, 10), [span(14, 0, 14, 5), span(14, 5, 14, 10)]
        )


class TestMergeSpans:
    def test_맞닿은_구간을_합친다(self):
        merged = merge_spans([span(14, 0, 14, 5), span(14, 5, 14, 10)])
        assert merged == [(at(14, 0), at(14, 10))]

    def test_합친_결과는_10분_상한에_걸리지_않는다(self):
        """``TimeRange``가 아니라 쌍을 돌려주는 이유다. 10분 상한은 한 번의
        조회가 부담하는 비용에서 온 것이지 커버리지 산수의 제약이 아니다."""
        merged = merge_spans([span(14, 0, 14, 10), span(14, 10, 14, 20)])
        assert merged[0][1] - merged[0][0] > MAX_TIME_RANGE_DURATION


class TestSplitSpan:
    def test_상한을_넘는_제안을_쪼갠다(self):
        """15분 제안을 거절하면 모델이 정당하게 요청한 범위가 통째로 사라진다."""
        assert label(split_span(at(13, 50), at(14, 5))) == [
            ("13:50", "14:00"),
            ("14:00", "14:05"),
        ]

    def test_모든_조각이_상한_이하다(self):
        pieces = split_span(at(13, 0), at(14, 0))
        assert all(p.end - p.start <= MAX_TIME_RANGE_DURATION for p in pieces)

    def test_뒤집힌_구간은_빈_목록이다(self):
        """모델이 쓴 제안과 합집합 계산의 중간값 둘 다 뒤집혀 올 수 있다.
        예외를 올리면 제안 하나가 분석 전체를 죽인다."""
        assert split_span(at(14, 0), at(13, 0)) == []

    def test_길이가_0이면_빈_목록이다(self):
        assert split_span(at(14, 0), at(14, 0)) == []


def test_짧은_조각도_정확히_계산된다():
    remaining = subtract_spans(span(13, 55, 14, 1), [span(14, 0, 14, 10)])
    assert label(remaining) == [("13:55", "14:00")]
    assert remaining[0].end - remaining[0].start == timedelta(minutes=5)
