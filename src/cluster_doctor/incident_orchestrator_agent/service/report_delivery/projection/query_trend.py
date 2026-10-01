"""Per-minute execution counts with explicit collection coverage."""
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import valid_runtime

@dataclass(frozen=True)
class QueryTrendPoint:
    minute: datetime
    count: int | None
    maximum_seconds: Decimal | None
    status: Literal['ok','failed','limited','unknown']
    start: datetime
    end: datetime


def project_query_trend(observations: Observations) -> tuple[QueryTrendPoint, ...]:
    statuses = [s for s in observations.source_statuses if s.source == 'es_query_log']
    windows = observations.requested or tuple((s.start,s.end) for s in statuses)
    if not windows and observations.query_requests:
        windows = ((min(r.timestamp for r in observations.query_requests).replace(second=0,microsecond=0),
                    max(r.timestamp for r in observations.query_requests).replace(second=0,microsecond=0)+timedelta(minutes=1)),)
    minutes = {}
    for start,end in windows:
        minute = start.replace(second=0,microsecond=0)
        while minute < end:
            left,right=max(start,minute),min(end,minute+timedelta(minutes=1))
            if minute in minutes:
                left,right=min(left,minutes[minute][0]),max(right,minutes[minute][1])
            minutes[minute]=(left,right)
            minute+=timedelta(minutes=1)
    result=[]
    for minute,(start,end) in sorted(minutes.items()):
        intersect = [s for s in statuses if s.start < end and s.end > start]
        boundaries=sorted({start,end,*[max(start,s.start) for s in intersect],*[min(end,s.end) for s in intersect]})
        states=[]
        for left,right in zip(boundaries,boundaries[1:]):
            active=[s for s in intersect if s.start <= left and s.end >= right]
            states.append(max(enumerate(active),key=lambda pair:(pair[1].collected_at,pair[0]))[1].status if active else 'unknown')
        status = 'failed' if 'failed' in states else ('unknown' if 'unknown' in states else ('limited' if 'limited' in states else 'ok'))
        records=[r for r in observations.query_requests if start <= r.timestamp < end]
        values=[v for r in records if (v:=valid_runtime(r.run_time)) is not None]
        result.append(QueryTrendPoint(minute,len(records) if status in ('ok','limited') else None,
            max(values) if values and status in ('ok','limited') else None,status,start,end))
    return tuple(result)
