"""Code-owned facts shared by drafting, revision and grounding."""
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict
import json

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry, record_json
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_analysis_agent.service.observation.log_format import format_log_line
from cluster_doctor.incident_analysis_agent.service.evidence_collection.limits import MAX_EVIDENCE_PER_SOURCE, MAX_EVIDENCE_TOTAL, MAX_RAW_LOG_CHARS, truncate_raw


def build_analysis_context(observations: Observations, evidence: list[Evidence]) -> str:
    ranked = rank_query_requests(observations.query_requests)
    data = {
        'query_execution_count': len(ranked),
        'maximum_execution_seconds': str(ranked[0].execution_seconds) if ranked and ranked[0].execution_seconds is not None else None,
        'execution_unit': 'seconds',
        'slow_executions': [dict(record_key=row.record_key, event_time=row.record.timestamp.isoformat(),
            execution_seconds=str(row.execution_seconds) if row.execution_seconds is not None else None,
            cmd=row.record.cmd, request_host=row.record.host, target_host=row.target_host,
            index_name=row.index_name, conditions=row.conditions, keywords=row.record.keyword,
            keyword_omitted=row.record.keyword_omitted) for row in ranked[:10]],
        'source_statuses': [asdict(status) for status in observations.source_statuses],
        'node_metrics': [asdict(row) for row in observations.nodes],
        'evidence': [dict(evidence_id=e.evidence_id, event_time=e.event_time.isoformat(),
            source=e.source, time_origin=e.time_origin, raw_truncated=e.raw_truncated,
            provenance=e.provenance.model_dump(mode='json') if e.provenance else None,
            message=e.message[:400], raw=None, context_raw_truncated=bool(e.raw)) for e in evidence],
    }
    def encode():
        return json.dumps(data, ensure_ascii=False, default=str)
    # The stored Evidence original stays untouched; only the drafting view is excerpted.
    for index, (view, original) in enumerate(zip(data['evidence'], evidence)):
        if not original.raw:
            continue
        remaining = max(0, MAX_RAW_LOG_CHARS - len(encode()) - 100)
        allowance = min(6000, remaining // max(1, len(evidence) - index))
        excerpt = original.raw[:allowance]
        while excerpt and len(json.dumps(excerpt, ensure_ascii=False)) > allowance:
            excerpt = excerpt[:max(0,len(excerpt)//2)]
        view['raw'] = excerpt or None
        view['context_raw_truncated'] = len(excerpt) < len(original.raw)
    return encode()


def required_query_evidence(requests: tuple[QueryLogEntry, ...], *, new_evidence_id: Callable[[], str], limit: int = 5) -> list[Evidence]:
    result = []
    for row in rank_query_requests(requests)[:limit]:
        raw = record_json(row.record)
        result.append(Evidence(evidence_id=new_evidence_id(), event_time=row.record.timestamp,
            source=EvidenceSource.QUERY_LOG, message=format_log_line(row.record), raw=truncate_raw(raw),
            raw_kind='record', raw_truncated=len(raw) > MAX_RAW_LOG_CHARS,
            provenance=row.record.provenance, selection_reason='실행시간 상위 개별 로그 (코드 선정)'))
    return result


def preserve_required_evidence(required: list[Evidence], selected: list[Evidence]) -> list[Evidence]:
    """Reserve top executions within the existing source and overall caps."""
    result = []
    counts = Counter()
    used = set()
    for item in required + selected:
        key = (item.source, item.event_time, item.raw, item.provenance)
        if key in used or counts[item.source] >= MAX_EVIDENCE_PER_SOURCE or len(result) >= MAX_EVIDENCE_TOTAL:
            continue
        existing = next((e for e in selected if (e.source, e.event_time, e.raw, e.provenance) == key), item)
        result.append(existing)
        used.add(key)
        counts[item.source] += 1
    return sorted(result, key=lambda item: item.event_time)
