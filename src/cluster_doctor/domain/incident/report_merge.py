"""여러 분석 구간의 개별 보고서를 Incident 전체의 최종 보고서 하나로 합친다.

Main Agent의 ``finalize_report`` Tool과 ``IncidentRunner``의 전달 직전 안전망이
이 모듈을 함께 쓴다 — 병합 규칙이 두 벌이 되면 한쪽만 고쳐지는 날이 온다.

병합은 결정적 Python 코드다. 원문을 다시 요약하거나 새 원인을 추론하지
않는다 — 이미 검증을 거친 구간별 보고서의 필드를 시간순으로 이어 붙이고
합칠 뿐이다.
"""

from __future__ import annotations

from cluster_doctor.domain.diagnosis.report import LogAnalysisReport, VerificationStatus


def merge_window_reports(reports: list[LogAnalysisReport]) -> LogAnalysisReport:
    """구간별 보고서를 시간순으로 병합해 하나의 ``LogAnalysisReport``로.

    비어 있으면 ``ValueError`` — application service가 빈 목록일 때 아예 부르지
    않는 것이 정상 경로이므로, 여기 들어오는 빈 목록은 호출자의 버그다.
    """
    if not reports:
        raise ValueError("병합할 보고서가 없다")

    ordered = sorted(reports, key=lambda r: r.analyzed_from)

    def _dedupe(items: tuple) -> tuple:
        seen: set = set()
        out = []
        for item in items:
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return tuple(out)

    timeline = tuple(
        sorted(
            (event for report in ordered for event in report.timeline),
            key=lambda event: event.at,
        )
    )
    findings = tuple(f for report in ordered for f in report.findings)
    root_causes = tuple(c for report in ordered for c in report.root_causes)
    unresolved_questions = _dedupe(
        tuple(q for report in ordered for q in report.unresolved_questions)
    )
    recommendations = _dedupe(
        tuple(r for report in ordered for r in report.recommendations)
    )
    seen_candidates: set[str] = set()
    suspect_picks = []
    for report in ordered:
        for pick in report.suspect_picks:
            if pick.candidate_id in seen_candidates:
                continue
            seen_candidates.add(pick.candidate_id)
            suspect_picks.append(pick)

    evidence_refs: list[str] = []
    seen_evidence: set[str] = set()
    for report in ordered:
        for ref in report.evidence_refs:
            if ref not in seen_evidence:
                seen_evidence.add(ref)
                evidence_refs.append(ref)

    # 검증 상태는 가장 나쁜 것을 따른다. 한 구간이라도 어긋났다면 전체를
    # 어긋난 것으로 본다 — 낙관 쪽으로 반올림하면 운영자가 놓친 불일치를
    # 신뢰된 결론으로 읽는다.
    statuses = {report.verification_status for report in ordered}
    if VerificationStatus.MISMATCH in statuses:
        verification_status = VerificationStatus.MISMATCH
    elif VerificationStatus.NOT_VERIFIED in statuses:
        verification_status = VerificationStatus.NOT_VERIFIED
    else:
        verification_status = VerificationStatus.PASSED

    verification_issues = tuple(
        issue for report in ordered for issue in report.verification_issues
    )

    summary = "\n\n".join(
        f"[{report.analyzed_from:%H:%M}~{report.analyzed_to:%H:%M}] {report.summary}"
        for report in ordered
        if report.summary
    )

    return LogAnalysisReport(
        incident_id=ordered[0].incident_id,
        analyzed_from=min(report.analyzed_from for report in ordered),
        analyzed_to=max(report.analyzed_to for report in ordered),
        summary=summary,
        timeline=timeline,
        findings=findings,
        root_causes=root_causes,
        unresolved_questions=unresolved_questions,
        recommendations=recommendations,
        suspect_picks=tuple(suspect_picks),
        evidence_refs=tuple(evidence_refs),
        verification_status=verification_status,
        verification_issues=verification_issues,
        revision_count=sum(report.revision_count for report in ordered),
    )
