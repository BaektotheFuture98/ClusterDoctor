"""위키 배지에 쓰는 이름. 사건 종합이 고른 패턴의 제목을 붙인다."""

from cluster_doctor.incident_analysis_agent.model.report import WikiPatternRef


def wiki_label(wiki_ref: str, patterns: tuple[WikiPatternRef, ...]) -> str:
    """``wiki_ref``가 비면 빈 문자열, 제목을 알면 ``id — 제목``, 모르면 id."""
    ref = wiki_ref.strip()
    if not ref:
        return ""
    for pattern in patterns:
        if pattern.pattern_id == ref and pattern.title and pattern.title != ref:
            return f"{ref} — {pattern.title}"
    return ref
