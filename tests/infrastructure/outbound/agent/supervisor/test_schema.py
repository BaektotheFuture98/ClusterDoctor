"""ReportNarrative 스키마 검증.

이 스키마의 성질 둘이 중요하다 — 모델이 쓴 것을 **자르지 않고**, 검증 실패로
재시도 루프를 만들지 않는다. 둘 다 이 저장소의 기존 결정(429 때문에 재시도를
0으로 둔 것, 관측값을 코드가 세게 한 것)에서 나온 제약이다.
"""

from cluster_doctor.infrastructure.outbound.agent.supervisor.schema import (
    ReportNarrative,
)


class TestNothingIsTruncated:
    def test_안내보다_길어도_자르지_않는다(self):
        """자르면 한국어가 글자 단위로 끊겨 단어가 쪼개진다.

        실측에서 follower_check가 follower / _check로 갈렸다. 리포트는 운영자가
        읽는 것이고, 끊긴 문장은 읽을 수 없다.
        """
        long_text = "가" * 3000

        narrative = ReportNarrative(root_cause=long_text)

        assert narrative.root_cause == long_text

    def test_항목이_많아도_버리지_않는다(self):
        narrative = ReportNarrative(
            findings=[{"title": f"문제 {i}"} for i in range(12)],
            recommendations=[f"조치 {i}" for i in range(20)],
        )

        assert len(narrative.findings) == 12
        assert len(narrative.recommendations) == 20


class TestNoRetryLoop:
    """검증 실패는 ToolStrategy의 재시도가 된다. recursion_limit이 9,999라
    프레임워크가 막지 않으므로 스키마가 애초에 실패하지 않아야 한다."""

    def test_아무것도_안_채워도_통과한다(self):
        narrative = ReportNarrative()

        assert narrative.headline == ""
        assert narrative.findings == []

    def test_모르는_severity는_예외가_아니라_분류_없음이다(self):
        narrative = ReportNarrative(findings=[{"severity": "중간", "title": "x"}])

        assert narrative.findings[0].severity == ""


class TestSeverityIsNotFabricated:
    def test_채우지_않은_severity는_Info가_되지_않는다(self):
        """기본값이 "Info"였을 때 25초 지연과 노드 19대 타임아웃이 전부 Info로
        나왔다. 모델이 그 필드를 채우지 않아 기본값이 그대로 실린 것이었다.

        빈 칸이 그럴듯한 값으로 채워지는 것은 관측값 쪽에서 이미 한 번 당한
        실패다(slowlog=264). 판단 쪽에도 같은 함정을 두지 않는다.
        """
        narrative = ReportNarrative(findings=[{"title": "노드 19대 타임아웃"}])

        assert narrative.findings[0].severity == ""

    def test_채운_severity는_그대로_간다(self):
        narrative = ReportNarrative(
            findings=[{"severity": "critical", "title": "x"}]
        )

        assert narrative.findings[0].severity == "Critical"


class TestToDomain:
    def test_도메인으로_옮기면_전부_tuple이_된다(self):
        """도메인은 dataclass만 쓴다. list를 그대로 넘기면 frozen이어도 해시가
        깨져 중복 제거·집합 연산에 쓸 수 없다."""
        narrative = ReportNarrative(
            headline="결론",
            findings=[{"severity": "Info", "title": "t", "evidence": ["e"]}],
            suspect_picks=[{"candidate_id": "C1", "reason": "r"}],
            recommendations=["a"],
        ).to_domain()

        assert isinstance(narrative.findings, tuple)
        assert isinstance(narrative.findings[0].evidence, tuple)
        assert isinstance(narrative.suspect_picks, tuple)
        assert isinstance(narrative.recommendations, tuple)
        assert narrative.suspect_picks[0].candidate_id == "C1"
