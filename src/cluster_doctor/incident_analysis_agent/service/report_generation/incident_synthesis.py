"""Final incident narrative from accumulated facts; no collection or delegation."""
from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus
from cluster_doctor.incident_analysis_agent.model.validation import VerificationIssueType
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator


def synthesize_incident(*, seams, incident, window_reports, observations, evidence):
    report = seams.report_writer.draft_incident(
        incident_id=incident.incident_id, cluster=incident.cluster,
        window_reports=window_reports, observations=observations, evidence=evidence,
    )
    grounding = GroundingValidator(call_llm=seams.call_llm)
    revisions = 0
    for round_no in range(3):
        structural = validate_report(report, evidence,
            candidate_ids={c.candidate_id for c in observations.candidates})
        found = grounding.validate(report, evidence, observations=observations,
                                   candidates=observations.candidates)
        conflicts = structural.issues + [i.reason for i in found
            if i.issue_type is not VerificationIssueType.UNVERIFIABLE]
        unavailable = [i.reason for i in found
            if i.issue_type is VerificationIssueType.UNVERIFIABLE]
        if not conflicts or round_no == 2:
            break
        revised = seams.report_writer.revise_report(report, tuple(conflicts), evidence,
                                                    observations=observations)
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
