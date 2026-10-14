import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import NodeMetricThresholds
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.report_generation import prompts, report_writer
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import build_analysis_context
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator

CONFIGURED = NodeMetricThresholds(heap_warn_percent=91, queue_warn=377)
EXPECTED = {"heap_warn_percent": 91, "queue_warn": 377}


def _thresholds(context: str) -> dict:
    return json.loads(context)["cross_links"]["node_metric_thresholds"]


def test_context_carries_configured_thresholds_without_any_node_crossing():
    assert _thresholds(build_analysis_context(Observations(), [], thresholds=CONFIGURED)) == EXPECTED


def test_context_defaults_to_default_thresholds():
    assert _thresholds(build_analysis_context(Observations(), [])) == {"heap_warn_percent": 85, "queue_warn": 100}


def test_thresholds_survive_budget_shrinking():
    assert _thresholds(build_analysis_context(Observations(), [], max_chars=10, thresholds=CONFIGURED)) == EXPECTED


def _capture_thresholds(monkeypatch) -> list:
    seen = []
    original = report_writer.build_analysis_context
    monkeypatch.setattr(report_writer, "build_analysis_context",
                        lambda *args, **kwargs: seen.append(kwargs.get("thresholds")) or original(*args, **kwargs))
    return seen


def test_revision_path_passes_configured_thresholds(monkeypatch):
    seen = _capture_thresholds(monkeypatch)
    monkeypatch.setattr(report_writer, "build_revision_prompt", lambda **_: "prompt")
    writer = ReportWriter(call_llm=lambda *_, **__: "not json", metric_thresholds=CONFIGURED)
    writer.revise_report(SimpleNamespace(), (), [])
    assert seen == [CONFIGURED]


def test_incident_synthesis_path_passes_configured_thresholds(monkeypatch):
    seen = _capture_thresholds(monkeypatch)

    def stop(*_, **__):
        raise RuntimeError("stop after context is built")

    writer = ReportWriter(call_llm=stop, metric_thresholds=CONFIGURED)
    window = SimpleNamespace(analyzed_from=datetime(2026, 1, 1), analyzed_to=datetime(2026, 1, 1, 0, 10))
    with pytest.raises(RuntimeError):
        writer.draft_incident(incident_id="i", cluster="c", window_reports=[window],
                              observations=Observations(), evidence=[])
    assert seen == [CONFIGURED]


def test_grounding_validator_uses_configured_thresholds():
    assert GroundingValidator(call_llm=None, metric_thresholds=CONFIGURED)._thresholds == CONFIGURED


def test_prompt_rule_names_thresholds_and_overlap_condition():
    rules = prompts._CAUSE_TARGET_RULES
    assert "node_metric_thresholds" in rules
    assert "기준 미만" in rules and "표본이 없었" in rules
    assert "겹친다" in rules
    assert "근거도 없었다는 근거도 아니다" not in rules
