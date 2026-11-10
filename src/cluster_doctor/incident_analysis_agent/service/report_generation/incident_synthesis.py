"""누적된 사실로 최종 incident 서술을 만든다. 수집과 위임은 하지 않는다."""
import logging

from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus, WikiPatternRef
from cluster_doctor.incident_analysis_agent.model.validation import VerificationIssueType
from cluster_doctor.incident_analysis_agent.service.observation.correlation import concrete_targets
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import build_analysis_context
from cluster_doctor.incident_analysis_agent.service.report_generation.wiki_refs import keep_known_wiki_refs
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator
from cluster_doctor.wiki_investigation_agent.agent.findings import WikiFindings

_logger = logging.getLogger(__name__)


# 위키 조사 에이전트가 읽는 관측 JSON의 상한. 색인과 패턴 페이지가 같은 대화에 실리므로 사건 종합보다 작게 둔다.
_WIKI_CONTEXT_MAX_CHARS = 24000


def _investigate_wiki(seams, observations, evidence, onset) -> WikiFindings:
    """위키 조사 에이전트를 사건당 한 번 돌린다. 위키가 없거나 조사가 실패하면 빈 결과라 종합은 위키 없이 이어 간다."""
    investigator = seams.wiki_investigator
    if investigator is None:
        return WikiFindings()
    try:
        context = build_analysis_context(
            observations, evidence, max_chars=_WIKI_CONTEXT_MAX_CHARS,
            thresholds=seams.metric_thresholds, onset=onset)
        return investigator.investigate(context)
    except Exception as exc:
        _logger.warning("위키 조사가 실패해 위키 없이 종합한다: %s", type(exc).__name__)
        return WikiFindings()


def synthesize_incident(*, seams, incident, window_reports, observations, evidence, onset=None):
    """``onset``은 최초 관측 시각이다. 주어지면 초안·교정·재수정 입력에 함께 싣는다."""
    wiki = _investigate_wiki(seams, observations, evidence, onset)
    report = seams.report_writer.draft_incident(
        incident_id=incident.incident_id, cluster=incident.cluster,
        window_reports=window_reports, observations=observations, evidence=evidence,
        onset=onset, wiki=wiki,
    )
    grounding = GroundingValidator(call_llm=seams.call_llm, metric_thresholds=seams.metric_thresholds)
    targets = concrete_targets(observations)
    revisions = 0
    for round_no in range(3):
        structural = validate_report(report, evidence,
            candidate_ids={c.candidate_id for c in observations.candidates}, targets=targets)
        found, covered = grounding.validate_with_coverage(report, evidence, observations=observations,
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
                                                    observations=observations, incident_final=True,
                                                    onset=onset)
        if revised is None:
            break
        report = revised
        revisions += 1
    # 재수정 응답은 위키 조사 결과를 보지 않고 wiki_ref를 다시 쓸 수 있어, 최종 리포트도 에이전트가 연 패턴 id로 한 번 더 제한한다.
    report = keep_known_wiki_refs(report, wiki.known_ids)
    # 주장이 있는데 한 건도 대조하지 못했으면 해석을 믿을 근거가 없다.
    unchecked = covered == 0 and grounding.has_claims(report)
    status = (VerificationStatus.UNCHECKED if unchecked else
              VerificationStatus.MISMATCH if conflicts else
              VerificationStatus.NOT_VERIFIED if unavailable else VerificationStatus.PASSED)
    return report.model_copy(update={
        'verification_status': status, 'verification_issues': tuple(conflicts + unavailable),
        'revision_count': revisions,
        'wiki_patterns': tuple(WikiPatternRef(pattern.pattern_id, pattern.title) for pattern in wiki.patterns),
    })
