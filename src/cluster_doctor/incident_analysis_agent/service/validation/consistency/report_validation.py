"""Check reference integrity only; semantic judgments belong to the model prompt."""
from __future__ import annotations
from dataclasses import dataclass, field
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport


@dataclass
class ValidationResult:
    issues: list[str] = field(default_factory=list)


def validate_report(report: LogAnalysisReport, evidence: list[Evidence], *, candidate_ids: set[str] | None = None) -> ValidationResult:
    result=ValidationResult()
    _check_unknown_refs(report,{item.evidence_id:item for item in evidence},result)
    _check_candidate_ids(report,candidate_ids or set(),result)
    return result


def _check_unknown_refs(
    report: LogAnalysisReport, known: dict[str, Evidence], result: ValidationResult
) -> None:
    """없는 근거를 인용했는가.

    가장 무거운 위반이다. 존재하지 않는 id를 인용한 주장은 근거가 없는 것을
    넘어, 근거가 있는 것처럼 보이게 만든다.
    """
    unknown = sorted(report.cited_refs() - set(known))
    if unknown:
        result.issues.append(
            f"존재하지 않는 근거를 인용했다: {', '.join(unknown)}. "
            "실제 Evidence id로 고치거나, 근거를 댈 수 없으면 그 주장을 빼라."
        )

def _check_candidate_ids(
    report: LogAnalysisReport, known: set[str], result: ValidationResult
) -> None:
    """지목한 느린 요청 후보가 실제로 제시된 id인가.

    근거 참조와 같은 규칙이다. 코드가 id를 붙여 목록을 주고 모델은 그중에서만
    고르게 해야, 수치와 쿼리 원문을 코드가 조인해 붙일 수 있다.
    """
    unknown = sorted({item.candidate_id for item in report.suspect_picks} - known)
    if unknown:
        result.issues.append(
            f"제시되지 않은 느린 요청 후보를 지목했다: {', '.join(unknown)}. "
            "주어진 후보 id 중에서만 고르거나 그 지목을 빼라."
        )
