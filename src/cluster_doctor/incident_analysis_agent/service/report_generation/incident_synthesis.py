"""누적된 사실로 최종 incident 서술을 만든다. 수집과 위임은 하지 않는다."""
import logging

from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.validation import VerificationIssueType
from cluster_doctor.incident_analysis_agent.service.observation.builder import ObservationBuilder
from cluster_doctor.incident_analysis_agent.service.observation.correlation import concrete_targets
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator

_logger = logging.getLogger(__name__)


def _wiki_context(seams, window_reports, observations) -> str:
    """누적 관측으로 위키에 증상을 한 번 묻는다. 관측이 없거나 조회가 실패해도 종합은 위키 없이 이어 간다."""
    if seams.wiki_client is None:
        return ""
    builder = ObservationBuilder.from_observations(
        TimeRange(window_reports[0].analyzed_from, window_reports[-1].analyzed_to), observations)
    if not (builder.candidates or builder.timeline or builder.master_logs):
        return ""
    symptoms = "\n\n".join(part for part in (
        builder.candidates_for_prompt(), builder.summary_for_prompt(), builder.master_logs_for_prompt(),
    ) if part)
    try:
        return seams.wiki_client.ask(symptoms)
    except Exception as exc:
        _logger.warning("사건 종합의 위키 조회가 실패해 위키 없이 종합한다: %s", exc)
        return ""


def synthesize_incident(*, seams, incident, window_reports, observations, evidence):
    report = seams.report_writer.draft_incident(
        incident_id=incident.incident_id, cluster=incident.cluster,
        window_reports=window_reports, observations=observations, evidence=evidence,
        wiki_context=_wiki_context(seams, window_reports, observations),
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
    status = (VerificationStatus.MISMATCH if conflicts else
              VerificationStatus.NOT_VERIFIED if unavailable else VerificationStatus.PASSED)
    return report.model_copy(update={
        'verification_status': status, 'verification_issues': tuple(conflicts + unavailable),
        'revision_count': revisions,
    })
