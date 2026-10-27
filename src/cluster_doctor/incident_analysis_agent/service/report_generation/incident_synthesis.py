"""누적된 사실로 최종 incident 서술을 만든다. 수집과 위임은 하지 않는다."""
import logging
from collections.abc import Callable

from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus
from cluster_doctor.incident_analysis_agent.model.validation import VerificationIssueType
from cluster_doctor.incident_analysis_agent.service.observation.correlation import concrete_targets
from cluster_doctor.incident_analysis_agent.service.report_generation.wiki_refs import (
    index_pattern_ids,
    keep_known_wiki_refs,
)
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator

_logger = logging.getLogger(__name__)


_WIKI_PAGE_SECTIONS = ("증상", "원인", "해결방법")


def _wiki_access(seams) -> tuple[str, Callable[[tuple[str, ...]], dict[str, str]] | None]:
    """위키 색인을 한 번 읽고, 고른 패턴 페이지를 여는 함수를 돌려준다.

    위키가 없거나 색인 조회가 실패하거나 색인에서 패턴 id를 읽지 못하면 ("", None)이라 종합은 위키 없이
    이어 간다. 페이지는 id마다 따로 열고, 하나가 실패하면 나머지는 열지 않고 그때까지 연 것만 돌려준다.
    """
    client = seams.wiki_client
    if client is None:
        return "", None
    try:
        index = client.get_index()
    except Exception as exc:
        _logger.warning("사건 종합의 위키 색인 조회가 실패해 위키 없이 종합한다: %s", exc)
        return "", None
    if not index.strip():
        return "", None
    if not index_pattern_ids(index):
        _logger.warning("위키 색인에서 패턴 id를 읽지 못해 위키 없이 종합한다. 색인 형식이 바뀌었는지 확인한다.")
        return "", None

    def open_pages(ids: tuple[str, ...]) -> dict[str, str]:
        pages: dict[str, str] = {}
        for pattern_id in ids:
            try:
                pages[pattern_id] = client.get_pattern(pattern_id, sections=_WIKI_PAGE_SECTIONS)
            except Exception as exc:
                # 색인에 있는 id가 열리지 않으면 위키 쪽 문제일 가능성이 높다. 나머지도 같은 대기를 겪지 않게 멈춘다.
                _logger.warning("위키 패턴 %s 페이지를 열지 못해 나머지 패턴은 열지 않는다: %s", pattern_id, exc)
                break
        return pages

    return index, open_pages


def synthesize_incident(*, seams, incident, window_reports, observations, evidence):
    wiki_index, open_wiki_pages = _wiki_access(seams)
    report = seams.report_writer.draft_incident(
        incident_id=incident.incident_id, cluster=incident.cluster,
        window_reports=window_reports, observations=observations, evidence=evidence,
        wiki_index=wiki_index, open_wiki_pages=open_wiki_pages,
    )
    grounding = GroundingValidator(call_llm=seams.call_llm, metric_thresholds=seams.metric_thresholds)
    targets = concrete_targets(observations)
    revisions = 0
    for round_no in range(3):
        structural = validate_report(report, evidence,
            candidate_ids={c.candidate_id for c in observations.candidates}, targets=targets)
        found = grounding.validate(report, evidence, observations=observations,
                                   candidates=observations.candidates)
        conflicts = structural.issues + [i.reason for i in found
            if i.issue_type is not VerificationIssueType.UNVERIFIABLE]
        unavailable = [i.reason for i in found
            if i.issue_type is VerificationIssueType.UNVERIFIABLE]
        # 권고는 재수정에만 쓰고 최종 판정에는 넣지 않는다. 남아도 원인 판단 전체를 불일치로 만들지 않기 위해서다.
        # 남은 권고는 verification_issues에도 싣지 않는다(실제 근거 충돌이 아니므로).
        requests = conflicts + structural.advisories
        if not requests or round_no == 2:
            break
        revised = seams.report_writer.revise_report(report, tuple(requests), evidence,
                                                    observations=observations, incident_final=True)
        if revised is None:
            break
        report = revised
        revisions += 1
    # 재수정 응답은 색인을 보지 않고 wiki_ref를 다시 쓸 수 있어, 최종 리포트도 색인 id로 한 번 더 제한한다.
    report = keep_known_wiki_refs(report, index_pattern_ids(wiki_index))
    status = (VerificationStatus.MISMATCH if conflicts else
              VerificationStatus.NOT_VERIFIED if unavailable else VerificationStatus.PASSED)
    return report.model_copy(update={
        'verification_status': status, 'verification_issues': tuple(conflicts + unavailable),
        'revision_count': revisions,
    })
