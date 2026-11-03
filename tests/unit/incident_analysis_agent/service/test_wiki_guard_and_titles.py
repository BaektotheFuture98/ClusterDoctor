import json
import re
from datetime import datetime
from types import SimpleNamespace

from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.findings import (
    WikiFindings,
    WikiPattern,
)
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import DEFAULT_THRESHOLDS
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportRecommendation,
    RootCause,
    VerificationStatus,
    WikiPatternRef,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis import (
    synthesize_incident,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.wiki_refs import (
    keep_known_wiki_refs,
)

AT = datetime(2026, 1, 1, 14, 0, tzinfo=KST)
PATTERN_ID = "massive-agg-query-timeout"
TITLE = "대량의 Aggregation 쿼리로 인한 클러스터 통신 타임아웃"
PAGE = f"---\nid: {PATTERN_ID}\n---\n# {TITLE}\n\n## 원인\n내용"


def _report(*, causes, recommendations=()):
    return LogAnalysisReport(
        incident_id="i-1", analyzed_from=AT, analyzed_to=AT,
        summary="요약", summary_evidence_refs=("E-1",),
        root_causes=tuple(causes), recommendations=tuple(recommendations),
    )


def test_known_cause_ref_with_observation_support_is_kept():
    report = _report(causes=[RootCause(statement="a", supporting_evidence_refs=("E-1",),
                                       wiki_ref=f" {PATTERN_ID} ")])
    kept = keep_known_wiki_refs(report, (PATTERN_ID,))
    assert kept.root_causes[0].wiki_ref == PATTERN_ID


def test_cause_ref_without_observation_support_is_cleared():
    report = _report(causes=[RootCause(statement="a", wiki_ref=PATTERN_ID)])
    assert keep_known_wiki_refs(report, (PATTERN_ID,)).root_causes[0].wiki_ref == ""


def test_unknown_cause_ref_is_cleared():
    report = _report(causes=[RootCause(statement="a", supporting_evidence_refs=("E-1",),
                                       wiki_ref="invented")])
    assert keep_known_wiki_refs(report, (PATTERN_ID,)).root_causes[0].wiki_ref == ""


def test_recommendation_refs_keep_their_existing_rules():
    report = _report(causes=[], recommendations=[
        ReportRecommendation(text="a", wiki_ref=PATTERN_ID),
        ReportRecommendation(text="b", wiki_ref="invented"),
    ])
    kept = keep_known_wiki_refs(report, (PATTERN_ID,))
    assert [item.wiki_ref for item in kept.recommendations] == [PATTERN_ID, ""]


def _seams(report, findings):
    class Writer:
        def draft_incident(self, **kwargs):
            return report

        def revise_report(self, *args, **kwargs):
            return None

    def call_llm(messages, **kwargs):
        ids = re.findall(r'"claim_id": "([^"]+)"', messages[0]["content"])
        return json.dumps([{"claim_id": i, "status": "PASSED", "reason": "",
                            "affected_evidence_refs": []} for i in ids], ensure_ascii=False)

    return SimpleNamespace(
        wiki_investigator=SimpleNamespace(investigate=lambda context: findings),
        report_writer=Writer(), call_llm=call_llm, metric_thresholds=DEFAULT_THRESHOLDS)


def test_synthesis_carries_selected_pattern_titles_and_guards_refs():
    report = _report(
        causes=[RootCause(statement="a", supporting_evidence_refs=("E-1",), wiki_ref=PATTERN_ID),
                RootCause(statement="b", wiki_ref=PATTERN_ID)],
        recommendations=[ReportRecommendation(text="조치", evidence_refs=("E-1",), wiki_ref="invented")])
    findings = WikiFindings(patterns=(WikiPattern(PATTERN_ID, "이유", PAGE),))
    evidence = [Evidence(evidence_id="E-1", event_time=AT, source=EvidenceSource.SLOWLOG, message="m")]

    final = synthesize_incident(
        seams=_seams(report, findings),
        incident=SimpleNamespace(incident_id="i-1", cluster="c"),
        window_reports=[report], observations=Observations(), evidence=evidence)

    assert final.wiki_patterns == (WikiPatternRef(PATTERN_ID, TITLE),)
    assert [cause.wiki_ref for cause in final.root_causes] == [PATTERN_ID, ""]
    assert final.recommendations[0].wiki_ref == ""
    assert final.verification_status is VerificationStatus.PASSED


def test_synthesis_without_wiki_has_no_patterns():
    report = _report(causes=[RootCause(statement="a", supporting_evidence_refs=("E-1",))])
    evidence = [Evidence(evidence_id="E-1", event_time=AT, source=EvidenceSource.SLOWLOG, message="m")]
    final = synthesize_incident(
        seams=_seams(report, WikiFindings()),
        incident=SimpleNamespace(incident_id="i-1", cluster="c"),
        window_reports=[report], observations=Observations(), evidence=evidence)
    assert final.wiki_patterns == ()
