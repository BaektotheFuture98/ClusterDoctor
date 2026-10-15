"""검증을 통과하지 못한 AI 분석을 숨기는 이유와 초안을 운영자에게 보이는 투영."""
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import display_narrative, is_validation_diagnostic

SECTION_TITLE = '검증되지 않은 AI 분석'
DRAFT_WARNING = '근거 검증을 통과하지 못한 초안입니다. 판단 근거로 쓰지 말고 분석 품질 확인용으로만 보세요.'
NO_PROBLEMS = '기록된 검증 문제가 없습니다.'
_SECTION_POINTER = f" 아래 '{SECTION_TITLE}'에서 사유와 초안을 볼 수 있습니다."
_REASONS = {
    'MISMATCH': 'AI 분석이 근거 검증을 통과하지 못해 결론에 표시하지 않았습니다.',
    'NOT_VERIFIED': 'AI 분석의 근거 검증을 마치지 못해 결론에 표시하지 않았습니다.',
}


def verification_problems(report, gaps=()) -> tuple[str, ...]:
    """report의 검증 문제와 gap에 실린 검증 진단. 접두어는 떼고 중복은 뺀다."""
    diagnostics = (gap.split(':', 1)[1].strip() for gap in gaps if is_validation_diagnostic(gap))
    return tuple(dict.fromkeys(p for p in (*report.verification_issues, *diagnostics) if p))


def unverified_draft(report, *, analysis_failed):
    """결론에 쓰이지 않은 원본 narrative. 표시된 것과 같으면 None."""
    if report.narrative and display_narrative(report, analysis_failed=analysis_failed) is not report.narrative:
        return report.narrative
    return None


def has_unverified_section(report, gaps=(), *, analysis_failed) -> bool:
    return unverified_draft(report, analysis_failed=analysis_failed) is not None or bool(verification_problems(report, gaps))


def verification_notice(report, *, analysis_failed, gaps=()) -> str:
    if report.verification_status == 'PASSED' and report.narrative and not analysis_failed:
        return ''
    if not report.narrative:
        reason = 'AI 분석 결과가 없어 관측값만 표시합니다.'
    elif report.verification_status == 'PASSED':
        reason = '구간 분석 중 실패가 있어 AI 분석을 결론에 표시하지 않았습니다.'
    else:
        reason = _REASONS.get(report.verification_status, _REASONS['NOT_VERIFIED'])
    return reason + (_SECTION_POINTER if has_unverified_section(report, gaps, analysis_failed=analysis_failed) else '')
