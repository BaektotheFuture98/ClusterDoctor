"""사건 종합이 위키 조사 에이전트를 한 번 부르고 그 결과를 draft_incident에 넘기는지 검증한다.

writer·LLM·조사기는 모두 가짜다. 실제 위키 서버나 LLM을 부르지 않는다.
"""

import json
import logging
from datetime import datetime
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import DEFAULT_THRESHOLDS
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport, ReportRecommendation, VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis import (
    synthesize_incident,
)
from cluster_doctor.wiki_investigation_agent.agent.findings import (
    WikiFindings,
    WikiPattern,
)

START = datetime(2026, 10, 23, 18, 54, tzinfo=KST)
END = datetime(2026, 10, 23, 19, 4, tzinfo=KST)
ONSET = datetime(2026, 1, 1, 14, 0, 37, tzinfo=KST)
EVIDENCE = [
    Evidence(evidence_id="E-1", event_time=datetime(2026, 1, 1, 13, 58, tzinfo=KST),
             source=EvidenceSource.SLOWLOG, message="m")
]
SUMMARY_PASSED = [{"claim_id": "summary", "status": "PASSED", "reason": "", "affected_evidence_refs": []}]


def _findings(*pattern_ids: str) -> WikiFindings:
    return WikiFindings(patterns=tuple(WikiPattern(i, "일치 이유", f"# 페이지 {i}") for i in pattern_ids))


class RecordingInvestigator:
    def __init__(self, findings=None, error=None, events=None):
        self.findings = findings or WikiFindings()
        self.error = error
        self.events = events if events is not None else []
        self.contexts = []

    def investigate(self, analysis_context):
        self.events.append("investigate")
        self.contexts.append(analysis_context)
        if self.error:
            raise self.error
        return self.findings


class FakeWriter:
    """draft_incident에 받은 인자를 기록하고, 정해 둔 wiki_ref를 단 조치가 있는 초안을 돌려준다."""

    def __init__(self, wiki_refs: tuple[str, ...] = (), revised_wiki_refs: tuple[str, ...] | None = None,
                 events=None) -> None:
        self.kwargs: dict = {}
        self.wiki_refs = wiki_refs
        self.revised_wiki_refs = revised_wiki_refs
        self.revise_calls = 0
        self.events = events if events is not None else []

    @staticmethod
    def _report(wiki_refs: tuple[str, ...], cause_index: int | None = None) -> LogAnalysisReport:
        return LogAnalysisReport(
            incident_id="inc-1", analyzed_from=START, analyzed_to=END, summary="19:03에 최대 19.04초 지연이 관측됐다.",
            recommendations=tuple(
                ReportRecommendation(text=f"조치 {n}", wiki_ref=ref, cause_index=cause_index)
                for n, ref in enumerate(wiki_refs)
            ),
        )

    def draft_incident(self, **kwargs) -> LogAnalysisReport:
        self.events.append("draft")
        self.kwargs = kwargs
        # 재수정을 지정하면 원인 범위를 벗어난 cause_index를 달아 구조 검증이 재수정을 요구하게 한다.
        return self._report(self.wiki_refs, cause_index=0 if self.revised_wiki_refs is not None else None)

    def revise_report(self, *args, **kwargs):
        self.revise_calls += 1
        return None if self.revised_wiki_refs is None else self._report(self.revised_wiki_refs)


def _run(investigator, writer: FakeWriter, *, evidence=(), onset=ONSET) -> LogAnalysisReport:
    llm = lambda messages, *, response_format=None: json.dumps(SUMMARY_PASSED)  # noqa: E731
    seams = SimpleNamespace(report_writer=writer, call_llm=llm,
                            metric_thresholds=DEFAULT_THRESHOLDS, wiki_investigator=investigator)
    return synthesize_incident(
        seams=seams, incident=SimpleNamespace(incident_id="inc-1", cluster="es"),
        window_reports=[LogAnalysisReport(incident_id="inc-1", analyzed_from=START, analyzed_to=END)],
        observations=Observations(), evidence=list(evidence), onset=onset,
    )


def _synthesize(investigator, wiki_refs: tuple[str, ...] = ()):
    writer = FakeWriter(wiki_refs)
    report = _run(investigator, writer)
    return writer.kwargs, report


def test_without_an_investigator_the_draft_gets_empty_findings():
    kwargs, _ = _synthesize(None)

    assert kwargs["wiki"].known_ids == ()
    assert kwargs["wiki"].as_prompt_block() == ""


def test_investigator_failure_gives_the_draft_empty_findings_and_is_logged(caplog):
    with caplog.at_level(logging.WARNING):
        kwargs, _ = _synthesize(RecordingInvestigator(error=RuntimeError("boom")))

    assert kwargs["wiki"].known_ids == ()
    assert "위키 조사가 실패해 위키 없이 종합한다" in caplog.text


def test_findings_and_onset_reach_the_draft_as_they_are():
    wanted = _findings("pat-a", "pat-b")

    kwargs, _ = _synthesize(RecordingInvestigator(findings=wanted))

    assert kwargs["wiki"] is wanted
    assert kwargs["onset"] == ONSET


def test_the_investigation_runs_once_and_before_the_draft():
    events: list[str] = []
    investigator = RecordingInvestigator(findings=_findings("pat-a"), events=events)

    _run(investigator, FakeWriter(events=events))

    assert events == ["investigate", "draft"]


@pytest.mark.parametrize("onset", [ONSET, None], ids=["with-onset", "without-onset"])
def test_investigator_context_has_phase_marks_only_when_the_onset_is_known(onset):
    investigator = RecordingInvestigator()

    _run(investigator, FakeWriter(), evidence=EVIDENCE, onset=onset)

    data = json.loads(investigator.contexts[0])
    if onset is None:
        assert "onset_kst" not in data
        assert "phase" not in data["evidence"][0]
    else:
        assert data["onset_kst"] == onset.isoformat()
        assert data["evidence"][0]["phase"] == "before"


def test_investigator_context_is_capped_at_the_wiki_budget():
    investigator = RecordingInvestigator()
    evidence = [
        Evidence(evidence_id=f"E-{n}", event_time=datetime(2026, 1, 1, 13, 58, tzinfo=KST),
                 source=EvidenceSource.SLOWLOG, message="Ω" * 400)
        for n in range(200)
    ]

    _run(investigator, FakeWriter(), evidence=evidence)

    assert len(investigator.contexts[0]) <= 24000


@pytest.mark.parametrize(
    "investigator",
    [
        RecordingInvestigator(findings=_findings("pat-a")),
        RecordingInvestigator(error=RuntimeError("down")),
        None,
    ],
    ids=["found", "failed", "absent"],
)
def test_verification_status_does_not_depend_on_the_wiki_outcome(investigator):
    _, report = _synthesize(investigator)

    assert report.verification_status is VerificationStatus.PASSED


@pytest.mark.parametrize(
    ("investigator", "wiki_refs", "kept"),
    [
        (RecordingInvestigator(findings=_findings("pat-a")),
         ("pat-a", "지어낸 패턴", " pat-a ", ""), ["pat-a", "", "pat-a", ""]),
        (None, ("pat-a",), [""]),
    ],
    ids=["only-opened-patterns", "no-wiki"],
)
def test_final_report_keeps_only_wiki_refs_of_the_opened_patterns(investigator, wiki_refs, kept):
    _, report = _synthesize(investigator, wiki_refs=wiki_refs)

    assert [item.wiki_ref for item in report.recommendations] == kept


def test_wiki_ref_rewritten_by_a_revision_is_limited_to_the_opened_patterns_again():
    writer = FakeWriter(wiki_refs=("pat-a",), revised_wiki_refs=("지어낸 패턴", "pat-a"))

    report = _run(RecordingInvestigator(findings=_findings("pat-a")), writer)

    assert writer.revise_calls >= 1
    assert [item.wiki_ref for item in report.recommendations] == ["", "pat-a"]
