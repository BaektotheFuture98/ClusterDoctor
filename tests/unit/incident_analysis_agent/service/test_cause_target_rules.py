import json
from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, ReportRecommendation
from cluster_doctor.incident_analysis_agent.service.observation.correlation import (
    build_cross_links,
    concrete_targets,
    cross_links_payload,
)
from cluster_doctor.incident_analysis_agent.service.report_generation import prompts
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import build_analysis_context
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report


def _query(i: int, seconds: str, *, company="acme", user="kim", host="10.0.0.5", days=181, indexes="a,b,c",
           keywords=("k",), omitted=0, cmd="agg") -> QueryLogEntry:
    return QueryLogEntry(
        reg_date=datetime(2026, 10, 11, 19, i // 60, i % 60, tzinfo=KST), host=host, run_time=Decimal(seconds),
        success="Y", s_date=0, e_date=0, date_range=days, keyword=keywords, target_host="192.168.1.30",
        index_name=indexes, conditions=(), cmd=cmd, service="", env="", project="", company=company, user=user,
        search_count=0, etc="", cluster="", keyword_omitted=omitted,
    )


def _observations() -> Observations:
    rows = [_query(i, "12.0", keywords=("a", "b", "c", "d", "e"), omitted=56) for i in range(3)]
    rows += [_query(10 + i, "11.0", company="other", user="lee", host="10.0.0.9") for i in range(1)]
    rows += [_query(20 + i, "0.5") for i in range(5)]
    rows += [_query(30 + i, "0.4", company="other", user="lee", host="10.0.0.9") for i in range(5)]
    return Observations(query_requests=tuple(rows), nodes=(NodeMetricRow(node="es-data-01", node_ip="192.168.1.30"),))


def test_synthesis_and_review_share_the_cause_target_rules():
    assert prompts._CAUSE_TARGET_RULES in prompts._INCIDENT_SYNTHESIS_INSTRUCTIONS
    assert prompts._CAUSE_TARGET_RULES in prompts._INCIDENT_REVIEW_INSTRUCTIONS
    assert prompts._CAUSE_TARGET_RULES not in prompts._ANALYSIS_RULES


def test_cause_target_rules_cover_each_requirement():
    rules = prompts._CAUSE_TARGET_RULES
    assert "더 나쁘면" in rules and "속성" in rules
    assert "임계치" in rules and "시각이 맞지 않" in rules
    assert "구체 대상" in rules
    assert "반복하지 않는다" in rules


def test_concentration_counts_slow_requests_by_requester_and_host():
    block = cross_links_payload(build_cross_links(_observations()), {})["slow_concentration"]
    assert len(block) == 1
    row = block[0]
    assert (row["cmd"], row["date_range"], row["index_count"], row["slow"]) == ("agg", 181, 3, 4)
    assert row["requesters"][0] == {"company": "acme", "user": "kim", "slow": 3, "total_in_shape": 8}
    assert row["requesters"][1]["company"] == "other"
    assert row["request_hosts"][0] == {"host": "10.0.0.5", "slow": 3, "total_in_shape": 8}
    assert row["keyword_count_max_slow"] == 61
    assert row["keyword_count_max_not_slow"] == 1


def test_concentration_is_bounded():
    rows = [_query(i, "12.0", company=f"c{i}", user=f"u{i}", host=f"10.0.0.{i}", days=i + 1) for i in range(20)]
    block = cross_links_payload(build_cross_links(Observations(query_requests=tuple(rows))), {})["slow_concentration"]
    assert len(block) == 3
    assert all(len(r["requesters"]) <= 3 and len(r["request_hosts"]) <= 3 for r in block)


def test_no_slow_requests_gives_empty_concentration():
    rows = tuple(_query(i, "0.2") for i in range(5))
    assert cross_links_payload(build_cross_links(Observations(query_requests=rows)), {})["slow_concentration"] == []


def test_concentration_reaches_the_model_context():
    data = json.loads(build_analysis_context(_observations(), []))
    assert data["cross_links"]["slow_concentration"][0]["requesters"][0]["company"] == "acme"


def test_concentration_is_dropped_before_primary_data_when_over_budget():
    data = json.loads(build_analysis_context(_observations(), [], max_chars=1500))
    assert "slow_executions" in data
    assert data["context_omissions"]


def _report(*texts: str) -> LogAnalysisReport:
    return LogAnalysisReport(incident_id="i", analyzed_from=datetime(2026, 10, 11, tzinfo=KST),
                             analyzed_to=datetime(2026, 10, 12, tzinfo=KST),
                             recommendations=tuple(ReportRecommendation(text=t) for t in texts))


def test_recommendation_without_concrete_target_is_flagged():
    targets = concrete_targets(_observations())
    result = validate_report(_report("다수 인덱스를 참조하는 agg 쿼리의 대상 범위를 최적화하십시오."), [], targets=targets)
    assert result.issues == []
    assert any("구체 대상" in issue and "1번" in issue for issue in result.advisories)


def test_recommendation_naming_shape_company_host_or_node_passes():
    targets = concrete_targets(_observations())
    for text in ("acme 사용자 kim의 agg·181일 요청 키워드 수를 줄이십시오.", "요청 호스트 10.0.0.5의 호출을 확인하십시오.",
                 "es-data-01 노드의 검색 큐를 확인하십시오.", "agg · 181일 · 인덱스 3개 요청의 범위를 줄이십시오."):
        assert validate_report(_report(text), [], targets=targets).advisories == [], text


def test_target_check_is_skipped_without_observed_targets():
    assert validate_report(_report("일반 조치"), []).advisories == []
    assert validate_report(_report("일반 조치"), [], targets=frozenset()).advisories == []


def test_short_names_do_not_disable_the_check():
    obs = Observations(query_requests=(_query(0, "1.0", company="a", user="b", host="c", days=1, indexes="x"),))
    assert not any(len(t) < 3 for t in concrete_targets(obs))


class _Writer:
    def __init__(self, draft, revisions):
        self.draft, self.revisions, self.requests = draft, list(revisions), []

    def draft_incident(self, **kwargs):
        return self.draft

    def revise_report(self, report, issues, evidence, **kwargs):
        self.requests.append(issues)
        return self.revisions.pop(0) if self.revisions else None


def _synthesize(monkeypatch, writer, grounding_issues=()):
    from types import SimpleNamespace
    from cluster_doctor.incident_analysis_agent.service.report_generation import incident_synthesis

    class _Grounding:
        def __init__(self, **kwargs): ...

        def validate(self, *args, **kwargs):
            return list(grounding_issues)

    monkeypatch.setattr(incident_synthesis, "GroundingValidator", _Grounding)
    return incident_synthesis.synthesize_incident(
        seams=SimpleNamespace(report_writer=writer, call_llm=None), incident=SimpleNamespace(incident_id="i", cluster="c"),
        window_reports=[], observations=_observations(), evidence=[])


def test_advisory_triggers_revision_but_leftover_advisory_stays_passed(monkeypatch):
    from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus
    bad = _report("대상 범위를 최적화하십시오.")
    writer = _Writer(bad, [bad, bad])
    result = _synthesize(monkeypatch, writer)
    assert writer.requests and any("구체 대상" in i for i in writer.requests[0])
    assert result.verification_status is VerificationStatus.PASSED
    assert result.verification_issues == ()


def test_advisory_resolved_by_revision(monkeypatch):
    from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus
    writer = _Writer(_report("대상 범위를 최적화하십시오."), [_report("acme 사용자 kim의 요청을 줄이십시오.")])
    result = _synthesize(monkeypatch, writer)
    assert len(writer.requests) == 1
    assert result.verification_status is VerificationStatus.PASSED


def test_real_conflict_still_becomes_mismatch(monkeypatch):
    from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus
    from cluster_doctor.incident_analysis_agent.model.validation import VerificationIssueType
    from types import SimpleNamespace
    conflict = SimpleNamespace(issue_type=VerificationIssueType.REPORT_MISMATCH, reason="근거 불일치")
    good = _report("acme 사용자 kim의 요청을 줄이십시오.")
    result = _synthesize(monkeypatch, _Writer(good, [good, good]), grounding_issues=[conflict])
    assert result.verification_status is VerificationStatus.MISMATCH
    assert "근거 불일치" in result.verification_issues
