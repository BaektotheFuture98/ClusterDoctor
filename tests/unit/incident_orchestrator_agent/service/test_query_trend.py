from datetime import UTC, datetime, timedelta
from decimal import Decimal
from cluster_doctor.incident_analysis_agent.model.observations import Observations, SourceWindowStatus

T0 = datetime(2026,10,1,tzinfo=UTC)


def test_successful_empty_and_failed_minutes_are_distinct():
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import project_query_trend
    obs = Observations(requested=((T0,T0+timedelta(minutes=2)),), source_statuses=(
        SourceWindowStatus('es_query_log',T0,T0+timedelta(minutes=1),'ok',0,T0),
        SourceWindowStatus('es_query_log',T0+timedelta(minutes=1),T0+timedelta(minutes=2),'failed',None,T0)))
    points=project_query_trend(obs)
    assert [(p.count,p.maximum_seconds,p.status) for p in points] == [(0,None,'ok'),(None,None,'failed')]


def test_partial_minute_keeps_actual_window_bounds():
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import project_query_trend
    start=T0+timedelta(seconds=30);end=T0+timedelta(seconds=50)
    obs=Observations(requested=((start,end),), source_statuses=(SourceWindowStatus('es_query_log',start,end,'ok',0,T0),))
    point=project_query_trend(obs)[0]
    assert point.start == start and point.end == end and point.count == 0
