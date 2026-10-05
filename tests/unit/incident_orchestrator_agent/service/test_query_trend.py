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


def test_separate_charts_share_time_positions_and_expose_values():
    from xml.etree import ElementTree as ET
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_trend import render_query_trend
    points = tuple(QueryTrendPoint(T0+timedelta(minutes=i), count, Decimal(runtime), 'ok',
        T0+timedelta(minutes=i), T0+timedelta(minutes=i+1))
        for i, (count, runtime) in enumerate(((8, '0.515'), (24, '17.6'))))
    root = ET.fromstring(render_query_trend(points))
    figures = root.findall('figure')
    assert [f.find('figcaption').text for f in figures] == ['요청 기록 수 (건)', '분별 최대 실행시간 (초)']
    counts, runtime = [f.find('svg') for f in figures]
    bars = counts.findall('rect')
    circles = runtime.findall('circle')
    assert [round(float(b.attrib['x'])+float(b.attrib['width'])/2, 2) for b in bars] == [float(c.attrib['cx']) for c in circles]
    assert [t.text for t in counts.findall("text[@class='trend-value']")] == ['8', '24']
    assert [t.text for t in runtime.findall("text[@class='trend-value']")] == ['0.515', '17.6']
    assert [t.text for t in counts.findall("text[@class='trend-time']")] == [t.text for t in runtime.findall("text[@class='trend-time']")]


def test_missing_runtime_breaks_line_and_empty_count_stays_zero():
    from xml.etree import ElementTree as ET
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_trend import render_query_trend
    values = ((1, Decimal('1.1'), 'ok'), (0, None, 'ok'), (None, None, 'failed'), (1, Decimal('2.2'), 'limited'))
    points = tuple(QueryTrendPoint(T0+timedelta(minutes=i), count, runtime, status,
        T0+timedelta(minutes=i), T0+timedelta(minutes=i+1))
        for i, (count, runtime, status) in enumerate(values))
    root = ET.fromstring(render_query_trend(points))
    counts, runtime = [f.find('svg') for f in root.findall('figure')]
    assert len(counts.findall('rect')) == 3
    assert [t.text for t in counts.findall("text[@class='trend-value']")] == ['1', '0', '1']
    assert len(runtime.findall('circle')) == 2
    assert not runtime.findall('polyline')  # Do not join observations across missing minutes.
    assert '실패' in ''.join(counts.itertext())
