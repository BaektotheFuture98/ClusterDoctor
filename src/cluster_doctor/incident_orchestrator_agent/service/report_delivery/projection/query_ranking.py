"""Compatibility entry points for individual execution ranking."""
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp


def query_ranking(requests):
    return rank_query_requests(tuple(requests))


def ranking_lines(requests):
    return [f'{stamp(row.record.timestamp)} · {row.record.cmd} · {str(row.execution_seconds)+"s" if row.execution_seconds is not None else "미확인"} · {" · ".join(row.record.keyword)}' for row in query_ranking(requests)[:10]]
