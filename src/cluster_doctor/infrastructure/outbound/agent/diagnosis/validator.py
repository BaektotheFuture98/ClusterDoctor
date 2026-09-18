"""선별된 Evidence와 Draft Report가 맞는지 본다.

**별도의 Agent가 아니다.** 검증에 필요한 것은 판단이 아니라 대조이고, 대조는
코드가 더 정확하다. 모델에게 "이 리포트가 근거와 맞습니까"를 물으면 같은 모델이
자기 출력을 채점하게 되고, 실측에서 그런 채점은 거의 통과한다.

여기 있는 규칙은 전부 **구조화된 필드**에서 나온다. 모델이 쓴 산문을 파싱하지
않는다 — 읽기 시작하면 그것이 모델 산문 파싱이고, 정확히 이 저장소가 두 번 당한
실패다. 딱 한 군데 본문을 보는 곳이 있는데(노드 이름), 그것도 **이미 아는
이름의 집합**과만 대조하므로 모르는 단어를 해석하지 않는다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta

from cluster_doctor.domain.model.evidence import Evidence
from cluster_doctor.domain.model.log_analysis_report import LogAnalysisReport

_logger = logging.getLogger(__name__)

# 타임라인 시각이 근거 시각과 얼마나 벌어져도 되는가. 모델은 분 단위로 반올림해
# 쓰는 일이 흔하고(14:03:12 → 14:03), 그것까지 불일치로 잡으면 지적이 잡음이
# 된다. 잡음이 된 지적은 revision을 의미 없이 태운다.
TIMESTAMP_TOLERANCE = timedelta(seconds=60)

# 확정적 표현. confidence가 낮거나 근거가 얇은데 이런 말을 쓰면 지적한다.
_DEFINITIVE_MARKERS = (
    "때문이다",
    "확실하다",
    "확실히",
    "명백하다",
    "명백히",
    "틀림없",
    "임이 증명",
    "원인이다",
)

# High를 주장하려면 이만큼의 근거는 있어야 한다.
_MIN_REFS_FOR_HIGH_CONFIDENCE = 2


@dataclass
class ValidationResult:
    issues: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.issues


def validate_report(
    report: LogAnalysisReport,
    evidence: list[Evidence],
    *,
    candidate_ids: set[str] | None = None,
) -> ValidationResult:
    """리포트와 근거의 불일치를 모두 찾는다.

    첫 불일치에서 멈추지 않는다. 한 번의 revision에 지적을 전부 실어야 모델이
    한 번에 고칠 수 있고, 하나씩 알려 주면 허용된 revision 횟수 안에 끝나지
    않는다.
    """
    result = ValidationResult()
    known = {item.evidence_id: item for item in evidence}
    known_nodes = {
        name
        for item in evidence
        for name in (item.node_name, item.node_id)
        if name
    }

    _check_unknown_refs(report, known, result)
    _check_candidate_ids(report, candidate_ids or set(), result)
    _check_unsupported_claims(report, result)
    _check_timestamps(report, known, result)
    _check_nodes(report, known, known_nodes, result)
    _check_ordering(report, result)
    _check_overclaiming(report, result)
    _check_causality(report, known, result)

    if result.issues:
        _logger.info("[validator] 불일치 %d건", len(result.issues))
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
    unknown = sorted(
        {item.candidate_id for item in report.suspect_picks} - known
    )
    if unknown:
        result.issues.append(
            f"제시되지 않은 느린 요청 후보를 지목했다: {', '.join(unknown)}. "
            "주어진 후보 id 중에서만 고르거나 그 지목을 빼라."
        )


def _check_unsupported_claims(report: LogAnalysisReport, result: ValidationResult) -> None:
    """근거 참조가 하나도 없는 주장이 있는가."""
    for item in report.findings:
        if not item.evidence_refs:
            result.issues.append(
                f"발견된 문제 '{item.title or '(제목 없음)'}'에 근거 참조가 없다."
            )
    for item in report.root_causes:
        if not item.supporting_evidence_refs:
            result.issues.append(
                f"원인 후보 '{_excerpt(item.statement)}'에 뒷받침하는 근거가 없다."
            )
    for item in report.timeline:
        if not item.evidence_refs:
            result.issues.append(
                f"타임라인 {item.at:%H:%M:%S} '{_excerpt(item.description)}'에 근거 참조가 없다."
            )


def _check_timestamps(
    report: LogAnalysisReport, known: dict[str, Evidence], result: ValidationResult
) -> None:
    """타임라인 시각이 인용한 근거의 시각과 맞는가."""
    for item in report.timeline:
        cited = [known[ref] for ref in item.evidence_refs if ref in known]
        if not cited:
            continue
        if not any(
            abs(evidence.event_time - item.at) <= TIMESTAMP_TOLERANCE for evidence in cited
        ):
            observed = ", ".join(f"{e.evidence_id}={e.event_time:%H:%M:%S}" for e in cited)
            result.issues.append(
                f"타임라인이 {item.at:%H:%M:%S}라고 썼지만 인용한 근거의 시각은 "
                f"{observed}이다. 근거의 시각을 그대로 써라."
            )


def _check_nodes(
    report: LogAnalysisReport,
    known: dict[str, Evidence],
    known_nodes: set[str],
    result: ValidationResult,
) -> None:
    """주장이 지목한 노드가 그 주장의 근거에 실제로 있는가.

    **이미 아는 노드 이름하고만 대조한다.** 본문에서 노드 이름을 추출하려 들면
    임의의 단어를 노드로 오인하고, 그 오탐이 revision을 태운다.
    """
    for item in report.findings:
        text = f"{item.title} {item.detail}"
        cited_nodes = {
            name
            for ref in item.evidence_refs
            if ref in known
            for name in (known[ref].node_name, known[ref].node_id)
            if name
        }
        mentioned = {name for name in known_nodes if name in text}
        stray = sorted(mentioned - cited_nodes)
        if stray and cited_nodes:
            result.issues.append(
                f"'{item.title or '(제목 없음)'}'이 {', '.join(stray)} 노드를 지목했지만 "
                f"인용한 근거는 {', '.join(sorted(cited_nodes))}의 것이다. "
                "해당 노드의 근거를 인용하거나 노드 지목을 빼라."
            )


def _check_ordering(report: LogAnalysisReport, result: ValidationResult) -> None:
    """타임라인이 시간순인가.

    정렬해서 조용히 고치지 않는다. 순서가 어긋났다는 것은 모델이 전개를 잘못
    읽었다는 신호이고, 그 신호는 원인 판단에도 남아 있다.
    """
    times = [item.at for item in report.timeline]
    if times != sorted(times):
        result.issues.append("타임라인이 시간순이 아니다. 시각 오름차순으로 다시 써라.")


def _check_overclaiming(report: LogAnalysisReport, result: ValidationResult) -> None:
    """근거보다 확정적으로 말하는가."""
    for item in report.root_causes:
        refs = len(item.supporting_evidence_refs)
        if item.confidence == "High" and refs < _MIN_REFS_FOR_HIGH_CONFIDENCE:
            result.issues.append(
                f"원인 후보 '{_excerpt(item.statement)}'의 confidence가 High인데 근거가 "
                f"{refs}건뿐이다. 근거를 더 인용하거나 confidence를 낮춰라."
            )
        definitive = [word for word in _DEFINITIVE_MARKERS if word in item.statement]
        if definitive and (item.confidence in ("", "Low") or refs < _MIN_REFS_FOR_HIGH_CONFIDENCE):
            result.issues.append(
                f"원인 후보 '{_excerpt(item.statement)}'가 확정적으로 쓰였지만"
                f"({', '.join(definitive)}) 근거는 {refs}건이고 confidence는 "
                f"{item.confidence or '미기재'}다. 표현을 낮춰라."
            )


def _check_causality(
    report: LogAnalysisReport, known: dict[str, Evidence], result: ValidationResult
) -> None:
    """원인으로 든 근거가 결과보다 늦지 않은가.

    원인은 결과보다 먼저 관측되어야 한다. 이것은 의미 판단이 아니라 시각 비교라
    코드가 할 수 있고, 실제로 자주 어긋난다 — 사고가 눈에 띄는 것은 결과 쪽이라
    모델이 그쪽 근거를 원인으로 집는다.
    """
    finding_times = [
        known[ref].event_time
        for item in report.findings
        for ref in item.evidence_refs
        if ref in known
    ]
    if not finding_times:
        return
    earliest_effect = min(finding_times)

    for cause in report.root_causes:
        cause_times = [
            known[ref].event_time
            for ref in cause.supporting_evidence_refs
            if ref in known
        ]
        if not cause_times:
            continue
        if min(cause_times) > earliest_effect + TIMESTAMP_TOLERANCE:
            result.issues.append(
                f"원인 후보 '{_excerpt(cause.statement)}'의 근거가 "
                f"{min(cause_times):%H:%M:%S}로, 문제로 지목된 관측"
                f"({earliest_effect:%H:%M:%S})보다 늦다. 원인은 결과보다 먼저 "
                "관측되어야 한다. 더 이른 근거를 찾거나 인과 서술을 고쳐라."
            )


def _excerpt(text: str, limit: int = 40) -> str:
    """지적 문장에 넣을 짧은 인용. 지적문 자체가 읽을 수 있어야 한다."""
    text = (text or "").strip().replace("\n", " ")
    return text if len(text) <= limit else text[:limit] + "…"
