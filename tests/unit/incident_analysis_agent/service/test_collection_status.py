from datetime import UTC, datetime, timedelta
from cluster_doctor.incident_analysis_agent.model.log_fetch import LogFetchResult, LogSourceFailure
from cluster_doctor.incident_analysis_agent.model.observations import merge_observations
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.observation.builder import ObservationBuilder
from cluster_doctor.incident_analysis_agent.service.evidence_collection.collector import EvidenceCollector

T0=datetime(2026,10,1,tzinfo=UTC)
WINDOW=TimeRange(T0,T0+timedelta(minutes=1))


def collect_status(result):
    b=ObservationBuilder(WINDOW)
    collector=EvidenceCollector(new_evidence_id=lambda:'E',fetch_logs=lambda _:result,
        fetch_node_logs=lambda *a:[],cluster=None,node_resolver=None,node_log_fetcher=None,
        call_llm=lambda *a,**k:'{}')
    collector._fetch_and_bucket(WINDOW,b)
    return b


def test_empty_success_and_failure_remain_distinct_after_roundtrip_and_merge():
    b=collect_status(LogFetchResult(failures=(LogSourceFailure('es_query_log',WINDOW,'unavailable'),)))
    states=b.to_observations().source_statuses
    query=next(x for x in states if x.source=='es_query_log')
    slow=next(x for x in states if x.source=='slowlog')
    assert (query.status,query.row_count)==('failed',None)
    assert (slow.status,slow.row_count)==('ok',0)
    restored=ObservationBuilder.from_state(WINDOW,b.state_update()).to_observations()
    merged=merge_observations(b.to_observations(),restored)
    assert merged.source_statuses==states


def test_repository_exception_marks_all_three_sources_failed():
    def fail(_): raise OSError('down')
    b=ObservationBuilder(WINDOW)
    c=EvidenceCollector(new_evidence_id=lambda:'E',fetch_logs=fail,fetch_node_logs=lambda *a:[],
        cluster=None,node_resolver=None,node_log_fetcher=None,call_llm=lambda *a,**k:'{}')
    c._fetch_and_bucket(WINDOW,b)
    assert {x.source for x in b.to_observations().source_statuses}=={'slowlog','es_query_log','node_metric'}
    assert all(x.status=='failed' and x.row_count is None for x in b.to_observations().source_statuses)
