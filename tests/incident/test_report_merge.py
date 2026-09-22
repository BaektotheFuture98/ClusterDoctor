"""구간별 보고서를 Incident 전체의 최종 보고서로 합치는 규칙을 검증한다.

``merge_window_reports``는 시간순 인터리빙·중복 제거 규칙이 요점이고,
``finalize_incident_report``는 저장소 조회 실패를 조용히 흡수하는 것이 요점이다.
"""

from datetime import datetime, timezone

import pytest

from cluster_doctor.contracts.observations import SuspectPick
from cluster_doctor.contracts.report import (
    LogAnalysisReport,
    RootCause,
    TimelineEvent,
    VerificationStatus,
)
from cluster_doctor.incident.report_merge import (
    finalize_incident_report,
    merge_window_reports,
)
from cluster_doctor.storage.in_memory_artifact_store import InMemoryArtifactStore


def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, 22, hour, minute, tzinfo=timezone.utc)


def _report(
    *,
    analyzed_from: datetime,
    analyzed_to: datetime,
    summary: str = "",
    timeline: tuple = (),
    findings: tuple = (),
    root_causes: tuple = (),
    unresolved_questions: tuple = (),
    recommendations: tuple = (),
    suspect_picks: tuple = (),
    evidence_refs: tuple = (),
    verification_status: VerificationStatus = VerificationStatus.PASSED,
    verification_issues: tuple = (),
    revision_count: int = 0,
) -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id="inc-1",
        analyzed_from=analyzed_from,
        analyzed_to=analyzed_to,
        summary=summary,
        timeline=timeline,
        findings=findings,
        root_causes=root_causes,
        unresolved_questions=unresolved_questions,
        recommendations=recommendations,
        suspect_picks=suspect_picks,
        evidence_refs=evidence_refs,
        verification_status=verification_status,
        verification_issues=verification_issues,
        revision_count=revision_count,
    )


class TestMergeWindowReports:
    def test_빈_목록이면_ValueError(self):
        with pytest.raises(ValueError):
            merge_window_reports([])

    def test_analyzed_from_to는_전체_구간의_최소_최대(self):
        early = _report(analyzed_from=_dt(13, 0), analyzed_to=_dt(13, 30))
        late = _report(analyzed_from=_dt(14, 0), analyzed_to=_dt(14, 30))

        merged = merge_window_reports([late, early])

        assert merged.analyzed_from == _dt(13, 0)
        assert merged.analyzed_to == _dt(14, 30)

    def test_타임라인은_구간별로_이어붙이지_않고_시간순으로_섞인다(self):
        # window A는 13:00~13:30, window B는 13:15~13:45 — 겹치는 시간대에서
        # 서로 인터리빙되어야 concatenate와 구별된다.
        window_a = _report(
            analyzed_from=_dt(13, 0),
            analyzed_to=_dt(13, 30),
            timeline=(
                TimelineEvent(at=_dt(13, 0), description="A0"),
                TimelineEvent(at=_dt(13, 20), description="A20"),
            ),
        )
        window_b = _report(
            analyzed_from=_dt(13, 15),
            analyzed_to=_dt(13, 45),
            timeline=(
                TimelineEvent(at=_dt(13, 10), description="B10"),
                TimelineEvent(at=_dt(13, 25), description="B25"),
            ),
        )

        merged = merge_window_reports([window_a, window_b])

        assert [event.description for event in merged.timeline] == [
            "A0",
            "B10",
            "A20",
            "B25",
        ]

    def test_findings와_root_causes는_중복제거_없이_이어붙인다(self):
        cause = RootCause(statement="같은 원인", confidence="high")
        window_a = _report(
            analyzed_from=_dt(13, 0), analyzed_to=_dt(13, 30), root_causes=(cause,)
        )
        window_b = _report(
            analyzed_from=_dt(14, 0), analyzed_to=_dt(14, 30), root_causes=(cause,)
        )

        merged = merge_window_reports([window_a, window_b])

        assert merged.root_causes == (cause, cause)

    def test_unresolved_questions와_recommendations는_첫등장_순서로_중복제거(self):
        window_a = _report(
            analyzed_from=_dt(13, 0),
            analyzed_to=_dt(13, 30),
            unresolved_questions=("Q1", "Q2"),
            recommendations=("R1",),
        )
        window_b = _report(
            analyzed_from=_dt(14, 0),
            analyzed_to=_dt(14, 30),
            unresolved_questions=("Q2", "Q3"),
            recommendations=("R1", "R2"),
        )

        merged = merge_window_reports([window_a, window_b])

        assert merged.unresolved_questions == ("Q1", "Q2", "Q3")
        assert merged.recommendations == ("R1", "R2")

    def test_suspect_picks는_candidate_id로_중복제거_첫값을_남긴다(self):
        window_a = _report(
            analyzed_from=_dt(13, 0),
            analyzed_to=_dt(13, 30),
            suspect_picks=(SuspectPick(candidate_id="C1", reason="첫 이유"),),
        )
        window_b = _report(
            analyzed_from=_dt(14, 0),
            analyzed_to=_dt(14, 30),
            suspect_picks=(
                SuspectPick(candidate_id="C1", reason="나중 이유"),
                SuspectPick(candidate_id="C2", reason="새 후보"),
            ),
        )

        merged = merge_window_reports([window_a, window_b])

        assert [p.candidate_id for p in merged.suspect_picks] == ["C1", "C2"]
        assert merged.suspect_picks[0].reason == "첫 이유"

    def test_verification_status는_하나라도_MISMATCH면_MISMATCH(self):
        passed = _report(
            analyzed_from=_dt(13, 0),
            analyzed_to=_dt(13, 30),
            verification_status=VerificationStatus.PASSED,
        )
        mismatch = _report(
            analyzed_from=_dt(14, 0),
            analyzed_to=_dt(14, 30),
            verification_status=VerificationStatus.MISMATCH,
        )

        merged = merge_window_reports([passed, mismatch])

        assert merged.verification_status == VerificationStatus.MISMATCH

    def test_verification_status는_MISMATCH_없으면_NOT_VERIFIED가_섞이면_NOT_VERIFIED(self):
        passed = _report(
            analyzed_from=_dt(13, 0),
            analyzed_to=_dt(13, 30),
            verification_status=VerificationStatus.PASSED,
        )
        not_verified = _report(
            analyzed_from=_dt(14, 0),
            analyzed_to=_dt(14, 30),
            verification_status=VerificationStatus.NOT_VERIFIED,
        )

        merged = merge_window_reports([passed, not_verified])

        assert merged.verification_status == VerificationStatus.NOT_VERIFIED

    def test_revision_count는_합산(self):
        window_a = _report(
            analyzed_from=_dt(13, 0), analyzed_to=_dt(13, 30), revision_count=1
        )
        window_b = _report(
            analyzed_from=_dt(14, 0), analyzed_to=_dt(14, 30), revision_count=2
        )

        merged = merge_window_reports([window_a, window_b])

        assert merged.revision_count == 3


class TestFinalizeIncidentReport:
    def test_report_refs가_비어있으면_None(self):
        store = InMemoryArtifactStore()

        result = finalize_incident_report("inc-1", [], store)

        assert result is None

    def test_모든_참조가_저장소에_없으면_None(self):
        store = InMemoryArtifactStore()

        result = finalize_incident_report(
            "inc-1", ["RPT-inc-1-999", "RPT-inc-1-998"], store
        )

        assert result is None

    def test_유효한_참조들을_모아_병합된_보고서를_저장하고_새_참조를_돌려준다(self):
        store = InMemoryArtifactStore()
        report_a = _report(analyzed_from=_dt(13, 0), analyzed_to=_dt(13, 30), summary="A 구간")
        report_b = _report(analyzed_from=_dt(14, 0), analyzed_to=_dt(14, 30), summary="B 구간")
        ref_a = store.put_report("inc-1", report_a)
        ref_b = store.put_report("inc-1", report_b)

        result_ref = finalize_incident_report("inc-1", [ref_a, ref_b], store)

        assert result_ref is not None
        assert result_ref not in (ref_a, ref_b)
        merged = store.get_report(result_ref)
        assert merged is not None
        assert merged.analyzed_from == _dt(13, 0)
        assert merged.analyzed_to == _dt(14, 30)
        assert "A 구간" in merged.summary
        assert "B 구간" in merged.summary
