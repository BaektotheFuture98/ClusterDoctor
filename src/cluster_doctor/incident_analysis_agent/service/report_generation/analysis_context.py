"""Code-owned facts shared by drafting, revision and grounding."""
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict
import json

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_analysis_agent.service.observation.log_format import format_log_line
from cluster_doctor.incident_analysis_agent.service.evidence_collection.limits import MAX_EVIDENCE_PER_SOURCE, MAX_EVIDENCE_TOTAL, MAX_RAW_LOG_CHARS


def build_analysis_context(observations: Observations, evidence: list[Evidence]) -> str:
    ranked = rank_query_requests(observations.query_requests)
    data = {
        'query_execution_count': len(ranked),
        'maximum_execution_seconds': str(ranked[0].execution_seconds) if ranked and ranked[0].execution_seconds is not None else None,
        'execution_unit': 'seconds',
        'context_omissions': {},
        'slow_executions': [dict(record_key=row.record_key, event_time=row.record.timestamp.isoformat(),
            execution_seconds=str(row.execution_seconds) if row.execution_seconds is not None else None,
            cmd=row.record.cmd, request_host=row.record.host, target_host=row.target_host,
            index_name=row.index_name, conditions=row.conditions, keywords=row.record.keyword,
            keyword_omitted=row.record.keyword_omitted) for row in ranked[:10]],
        'source_statuses': [asdict(status) for status in observations.source_statuses],
        'node_metrics': [asdict(row) for row in observations.nodes],
        'evidence': [dict(evidence_id=e.evidence_id, event_time=e.event_time.isoformat(),
            source=e.source, time_origin=e.time_origin,
            provenance=e.provenance.model_dump(mode='json') if e.provenance else None,
            message=e.message[:400]) for e in evidence],
    }
    def encode():
        return json.dumps(data, ensure_ascii=False, default=str)
    # Protect the complete JSON budget. All source DTOs
    # remain intact; compaction is explicitly visible to the model.
    omissions = data['context_omissions']
    nodes = [row for row in observations.nodes if row.samples > 0]
    data['node_metric_maxima'] = {
        field: {'value': max(getattr(n, field) for n in nodes),
                'nodes': [n.node for n in nodes if getattr(n, field) == max(getattr(x, field) for x in nodes)]}
        for field in ('cpu_max', 'jvm_heap_max', 'search_queue_max', 'write_queue_max',
                      'search_rejected_max', 'write_rejected_max')
    } if nodes else {}
    if len(encode()) > MAX_RAW_LOG_CHARS:
        if len(data['node_metrics']) > 10:
            omissions['node_metrics'] = len(data['node_metrics']) - 10
            data['node_metrics'] = data['node_metrics'][:10]
        for item in data['evidence']:
            if len(item['message']) > 128:
                item['message'] = item['message'][:128]
                omissions['messages_excerpted'] = omissions.get('messages_excerpted', 0) + 1
    def compact(value):
        if isinstance(value, str) and len(value) > 400:
            omissions['metadata_strings_excerpted'] = omissions.get('metadata_strings_excerpted', 0) + 1
            return value[:400] + '…'
        if isinstance(value, dict):
            return {key: compact(child) for key, child in value.items()}
        if isinstance(value, (list, tuple)):
            return [compact(child) for child in value]
        return value
    if len(encode()) > MAX_RAW_LOG_CHARS:
        for key in ('slow_executions', 'source_statuses', 'node_metric_maxima', 'evidence'):
            data[key] = compact(data[key])
        for row in data['slow_executions']:
            if len(row['conditions']) > 20:
                omissions['conditions'] = omissions.get('conditions', 0) + len(row['conditions']) - 20
                row['conditions'] = row['conditions'][:20]
    # Drop optional context rows only when compaction is still insufficient.
    # Exact count/maxima above survive; absent metadata never means normal/zero.
    for field in ('node_metrics', 'evidence', 'source_statuses', 'slow_executions'):
        while data[field] and len(encode()) > MAX_RAW_LOG_CHARS - 100:
            data[field].pop()
            omissions[field] = omissions.get(field, 0) + 1
    while len(encode()) > MAX_RAW_LOG_CHARS - 100:
        # A cluster can have enormous tie lists; values remain code-computed.
        ties = [entry['nodes'] for entry in data['node_metric_maxima'].values() if entry['nodes']]
        if not ties:
            break
        max(ties, key=len).pop()
        omissions['maximum_node_names'] = omissions.get('maximum_node_names', 0) + 1
    return encode()


def required_query_evidence(requests: tuple[QueryLogEntry, ...], *, new_evidence_id: Callable[[], str], limit: int = 5) -> list[Evidence]:
    return [Evidence(evidence_id=new_evidence_id(), event_time=row.record.timestamp,
            source=EvidenceSource.QUERY_LOG, message=format_log_line(row.record),
            record_key=row.record_key, provenance=row.record.provenance,
            selection_reason='실행시간 상위 개별 로그 (코드 선정)')
        for row in rank_query_requests(requests)[:limit]]


def _evidence_key(item: Evidence) -> tuple:
    if item.record_key:
        return (item.source, item.record_key)
    return (item.source, item.event_time, item.message, item.provenance)


def preserve_required_evidence(required: list[Evidence], selected: list[Evidence]) -> list[Evidence]:
    """Reserve top executions within the existing source and overall caps."""
    result = []
    counts = Counter()
    used = set()
    for item in required + selected:
        key = _evidence_key(item)
        if key in used or counts[item.source] >= MAX_EVIDENCE_PER_SOURCE or len(result) >= MAX_EVIDENCE_TOTAL:
            continue
        existing = next((e for e in selected if _evidence_key(e) == key), item)
        result.append(existing)
        used.add(key)
        counts[item.source] += 1
    return sorted(result, key=lambda item: item.event_time)
