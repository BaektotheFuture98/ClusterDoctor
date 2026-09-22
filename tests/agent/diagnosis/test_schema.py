"""DraftReport 스키마.

이 스키마의 성질 둘이 중요하다 — 모델이 쓴 것을 **자르지 않고**, 검증 실패로
재시도 루프를 만들지 않는다. 둘 다 이 저장소의 기존 결정(429 때문에 재시도를
0으로 둔 것, 관측값을 코드가 세게 한 것)에서 나온 제약이다.
"""

from cluster_doctor.agent.contracts import VerificationStatus
from cluster_doctor.agent.diagnosis.schema import (
    DraftReport,
    parse_draft,
)
from tests.contracts.test_time_range_spans import label, span

WINDOW = span(14, 0, 14, 10)


def to_domain(draft: DraftReport, refs=("E-1",)):
    return draft.to_domain(incident_id="inc-1", window=WINDOW, evidence_refs=refs)


class TestNothingIsTruncated:
    def test_안내보다_길어도_자르지_않는다(self):
        """자르면 한국어가 글자 단위로 끊겨 단어가 쪼개진다. 실측에서
        follower_check가 follower / _check로 갈렸다."""
        long_text = "가" * 3000

        draft = DraftReport(summary=long_text)

        assert draft.summary == long_text

    def test_항목이_많아도_버리지_않는다(self):
        draft = DraftReport(
            findings=[{"title": f"문제 {i}"} for i in range(12)],
            recommendations=[f"조치 {i}" for i in range(20)],
        )

        assert len(draft.findings) == 12
        assert len(draft.recommendations) == 20


class TestNoRetryLoop:
    def test_아무것도_안_채워도_통과한다(self):
        draft = DraftReport()

        assert draft.summary == ""
        assert draft.findings == []

    def test_모르는_severity는_예외가_아니라_분류_없음이다(self):
        draft = DraftReport(findings=[{"severity": "중간", "title": "x"}])

        assert draft.findings[0].severity == ""

    def test_모르는_confidence도_분류_없음이다(self):
        draft = DraftReport(root_causes=[{"statement": "x", "confidence": "아마도"}])

        assert draft.root_causes[0].confidence == ""

    def test_읽을_수_없는_응답은_빈_초안이_된다(self):
        """형식 이탈이 분석 전체를 죽이지 않는다. 빈 초안은 Validator가
        잡고, 그 사실이 gap으로 남는다."""
        assert parse_draft("리포트를 작성하겠습니다").summary == ""


class TestSeverityIsNotFabricated:
    def test_채우지_않은_severity는_Info가_되지_않는다(self):
        """기본값이 "Info"였을 때 25초 지연과 노드 19대 타임아웃이 전부 Info로
        나왔다. 빈 칸이 그럴듯한 값으로 채워지는 것은 관측값 쪽에서 이미 한 번
        당한 실패다(slowlog=264)."""
        draft = DraftReport(findings=[{"title": "노드 19대 타임아웃"}])

        assert draft.findings[0].severity == ""

    def test_채운_severity는_그대로_간다(self):
        draft = DraftReport(findings=[{"severity": "critical", "title": "x"}])

        assert draft.findings[0].severity == "Critical"


class TestToDomain:
    def test_도메인으로_옮기면_전부_tuple이_된다(self):
        report = to_domain(
            DraftReport(
                summary="결론",
                findings=[{"severity": "Info", "title": "t", "evidence_refs": ["E-1"]}],
                root_causes=[{"statement": "c", "supporting_evidence_refs": ["E-1"]}],
                recommendations=["a"],
            )
        )

        assert isinstance(report.findings, tuple)
        assert isinstance(report.findings[0].evidence_refs, tuple)
        assert isinstance(report.root_causes, tuple)
        assert isinstance(report.recommendations, tuple)

    def test_검증하기_전에는_NOT_VERIFIED다(self):
        """검증을 돌리지 못한 것과 통과한 것을 구별한다."""
        assert to_domain(DraftReport()).verification_status is (
            VerificationStatus.NOT_VERIFIED
        )

    def test_잘못된_참조를_지우지_않는다(self):
        """없는 id를 인용한 것은 Validator가 잡아야 할 사실이다. 여기서
        조용히 걸러 내면 검증이 늘 통과한다."""
        report = to_domain(
            DraftReport(findings=[{"title": "t", "evidence_refs": ["E-999"]}])
        )

        assert report.findings[0].evidence_refs == ("E-999",)

    def test_시각을_못_읽은_타임라인_줄은_버린다(self):
        """시각 없는 타임라인 항목은 타임라인이 아니고, 임의의 값을 채우면
        리포트가 거짓을 말한다."""
        report = to_domain(
            DraftReport(
                timeline=[
                    {"at": "어제 오후", "description": "d"},
                    {"at": "2026-09-18T14:02:00+09:00", "description": "d"},
                ]
            )
        )

        assert len(report.timeline) == 1

    def test_인용한_참조를_한자리에서_모은다(self):
        report = to_domain(
            DraftReport(
                findings=[{"title": "t", "evidence_refs": ["E-1"]}],
                root_causes=[
                    {
                        "statement": "c",
                        "supporting_evidence_refs": ["E-2"],
                        "counter_evidence_refs": ["E-3"],
                    }
                ],
            )
        )

        assert report.cited_refs() == {"E-1", "E-2", "E-3"}


class TestSuggestedWindows:
    def test_상한을_넘는_제안은_쪼갠다(self):
        """15분 제안을 거절하면 모델이 정당하게 요청한 범위가 통째로 사라진다."""
        draft = DraftReport(
            suggested_windows=[
                {
                    "start_iso": "2026-09-18T13:50:00+09:00",
                    "end_iso": "2026-09-18T14:05:00+09:00",
                }
            ]
        )

        assert label(draft.parsed_windows()) == [
            ("13:50", "14:00"),
            ("14:00", "14:05"),
        ]

    def test_읽지_못한_제안은_버린다(self):
        draft = DraftReport(suggested_windows=[{"start_iso": "언젠가", "end_iso": ""}])

        assert draft.parsed_windows() == []


class TestSuspectPicks:
    """코드가 고른 느린 요청 후보 중 모델이 지목한 것.

    이 저장소의 전사 오류 방지 장치다 — 코드가 후보에 id를 붙여 목록을 주고
    모델은 id와 이유만 돌려준다. 수치와 쿼리 원문은 코드가 조인해 붙이므로
    ``took=미확인`` 같은 실측 사고가 구조적으로 불가능해진다.
    """

    def test_id와_이유만_담는다(self):
        report = to_domain(
            DraftReport(suspect_picks=[{"candidate_id": "C1", "reason": "가장 느림"}])
        )

        assert report.suspect_picks[0].candidate_id == "C1"
        assert report.suspect_picks[0].reason == "가장 느림"

    def test_id_주변_공백을_씻는다(self):
        report = to_domain(DraftReport(suspect_picks=[{"candidate_id": " C2 "}]))

        assert report.suspect_picks[0].candidate_id == "C2"

    def test_id가_없는_지목은_버린다(self):
        """id가 없으면 코드가 수치를 조인할 수 없다. 이유만 남은 지목은
        근거 없는 문장이 된다."""
        report = to_domain(DraftReport(suspect_picks=[{"reason": "느려 보임"}]))

        assert report.suspect_picks == ()

    def test_고르지_않아도_된다(self):
        assert to_domain(DraftReport()).suspect_picks == ()
