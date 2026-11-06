"""근거 대조와 재수정이 관측이 큰 사건에서도 context 예산 안에서 돈다."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, Observations, TimelineRow
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, ReportFinding
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import build_analysis_context
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator

_T0 = datetime(2026, 11, 6, 8, 0, tzinfo=timezone(timedelta(hours=9)))


def _large_observations() -> Observations:
    timeline = tuple(
        TimelineRow(minute=_T0 + timedelta(minutes=n),
                    counts={'slowlog': n, 'es_query_log': n * 3}, took_max=f'{n}.1s', took_max_ms=n * 100,
                    jvm_heap_max=70 + n % 20, jvm_heap_max_node=f'node-{n % 50}')
        for n in range(1200))
    nodes = tuple(NodeMetricRow(node=f'node-{n}', samples=10, jvm_heap_max=80, cpu_max=50) for n in range(300))
    return Observations(timeline=timeline, nodes=nodes)


def _report() -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id='inc-1', analyzed_from=_T0, analyzed_to=_T0 + timedelta(minutes=10),
        summary='검색 지연이 관측됐다.',
        findings=(ReportFinding(title='지연', detail='힙 사용률이 높았다.', severity='warning'),),
    )


def test_기본_context는_배치_상한을_채울_만큼_크다():
    assert len(build_analysis_context(_large_observations(), [])) > 50_000


def test_큰_관측에서도_근거_대조가_호출된다():
    prompts: list[str] = []

    def call_llm(messages, **_):
        prompts.append(messages[0]['content'])
        return json.dumps({'results': []})

    issues = GroundingValidator(call_llm=call_llm).validate(_report(), [], observations=_large_observations())

    assert prompts
    assert all(len(p) <= 60_000 for p in prompts)
    assert not any('배치 상한을 초과' in i.reason for i in issues)


def test_큰_관측에서도_사건_재수정이_호출된다():
    calls: list[list[dict]] = []

    def call_llm(messages, **_):
        calls.append(messages)
        return json.dumps({'summary': '수정된 요약'})

    ReportWriter(call_llm=call_llm).revise_report(
        _report(), ('문제',), [], observations=_large_observations(), incident_final=True)

    assert calls
    assert sum(len(m['content']) for m in calls[0]) <= 60_000
