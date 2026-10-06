"""Compatibility entry points for individual execution ranking."""
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests


def query_ranking(requests):
    return rank_query_requests(tuple(requests))
