"""``LogAnalysisReport``를 운영자가 보는 리포트 타입으로 옮긴다.

표현 계층(HTML 렌더러, report_text)은 ``DiagnosisReport``를 읽는다. 그쪽을
새 타입에 맞춰 다시 쓰지 않는 이유는 그 코드가 잘 돌고 있고, 바꿔야 할 이유가
"모양이 달라서"뿐이기 때문이다. 대신 경계에 매핑 함수 하나를 둔다 — 이 저장소가
pydantic 스키마와 도메인 dataclass 사이에 ``to_domain``을 둔 것과 같은 자리다.

application 계층에 있는 이유: 들어오는 것도 나가는 것도 전부 도메인 타입이라
인프라 의존이 하나도 없고, 이것을 부르는 것은 Incident Lifecycle이다.

**근거 참조를 사람이 읽을 인용으로 바꾸는 것이 이 함수의 일이다.** 리포트에
``E-abc-3``이라고 적히면 운영자는 그것이 무엇인지 알 수 없다. 참조를 실제 근거
줄로 푸는 것은 코드가 해야 하고, 모델에게 시키면 옮겨 적다 틀린다.
"""

from __future__ import annotations

from cluster_doctor.domain.model.diagnosis_report import (
    DiagnosisReport,
    Finding,
    Narrative,
    Observations,
)
from cluster_doctor.domain.model.evidence import Evidence
from cluster_doctor.domain.model.log_analysis_report import LogAnalysisReport


def to_diagnosis_report(
    report: LogAnalysisReport | None,
    observations: Observations,
    evidence: list[Evidence],
) -> DiagnosisReport:
    """최종 전달용 리포트를 만든다.

    ``report``가 ``None``이어도 관측값 섹션은 그대로 렌더된다. 이 저장소의
    "리포트는 항상 전달된다" 원칙이 그것을 요구한다 — 모델이 아무 말도 남기지
    못해도 그 시각에 무슨 일이 있었는지는 남아야 한다.
    """
    if report is None:
        return DiagnosisReport(observations=observations)

    cite = {item.evidence_id: item for item in evidence}
    return DiagnosisReport(
        observations=observations,
        narrative=Narrative(
            headline=report.summary,
            context=tuple(
                f"{event.at:%H:%M:%S} {event.description}" for event in report.timeline
            ),
            findings=tuple(
                Finding(
                    severity=item.severity,
                    title=item.title,
                    evidence=_quote(item.evidence_refs, cite, extra=item.detail),
                )
                for item in report.findings
            ),
            root_cause=_root_cause_text(report),
            supporting=_quote(
                tuple(
                    ref
                    for cause in report.root_causes
                    for ref in cause.supporting_evidence_refs
                ),
                cite,
            ),
            contradicting=_quote(
                tuple(
                    ref
                    for cause in report.root_causes
                    for ref in cause.counter_evidence_refs
                ),
                cite,
            ),
            unverified=report.unresolved_questions,
            suspect_picks=report.suspect_picks,
            recommendations=report.recommendations,
        ),
    )


def _root_cause_text(report: LogAnalysisReport) -> str:
    """원인 후보를 한 문단으로. confidence를 함께 싣는다.

    확신도를 빼면 근거가 얇은 결론과 두꺼운 결론이 리포트에서 똑같아 보인다.
    """
    if not report.root_causes:
        return ""
    parts = []
    for cause in report.root_causes:
        confidence = f" (확신도 {cause.confidence})" if cause.confidence else ""
        parts.append(f"{cause.statement}{confidence}")
    return " / ".join(parts)


def _quote(
    refs: tuple[str, ...], cite: dict[str, Evidence], *, extra: str = ""
) -> tuple[str, ...]:
    """근거 참조를 원문 인용으로 푼다. 없는 참조는 그 사실을 적는다.

    **조용히 버리지 않는다.** 모델이 없는 id를 인용한 것은 운영자가 알아야 할
    사실이고, 그것이 사라지면 리포트가 근거를 댄 것처럼 보인다.
    """
    lines = [extra.strip()] if extra.strip() else []
    seen: set[str] = set()
    for ref in refs:
        if ref in seen:
            continue
        seen.add(ref)
        found = cite.get(ref)
        lines.append(found.cite() if found else f"[{ref}] (존재하지 않는 근거 참조)")
    return tuple(lines)
