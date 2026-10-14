"""초안 작성·수정·grounding이 공유하는, 코드가 소유한 사실."""
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime
import json

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import DEFAULT_THRESHOLDS, NodeMetricThresholds
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links, cross_links_payload
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_analysis_agent.service.observation.log_format import format_log_line
from cluster_doctor.incident_analysis_agent.service.evidence_collection.limits import MAX_EVIDENCE_PER_SOURCE, MAX_EVIDENCE_TOTAL, MAX_RAW_LOG_CHARS


def build_analysis_context(observations: Observations, evidence: list[Evidence], *, max_chars: int = MAX_RAW_LOG_CHARS,
                           thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS) -> str:
    ranked = rank_query_requests(observations.query_requests)
    data = {
        'report_timezone': 'Asia/Seoul',
        'query_execution_count': len(ranked),
        'maximum_execution_seconds': str(ranked[0].execution_seconds) if ranked and ranked[0].execution_seconds is not None else None,
        'execution_unit': 'seconds',
        'context_omissions': {},
        'slow_executions': [dict(record_key=row.record_key, event_time=row.record.timestamp.astimezone(KST).isoformat(),
            execution_seconds=str(row.execution_seconds) if row.execution_seconds is not None else None,
            cmd=row.record.cmd, request_host=row.record.host, target_host=row.target_host,
            index_name=row.index_name, conditions=row.conditions, keywords=row.record.keyword,
            keyword_omitted=row.record.keyword_omitted) for row in ranked[:10]],
        'minute_observations': [asdict(row) for row in observations.timeline],
        'source_statuses': [asdict(status) for status in observations.source_statuses],
        'node_metrics': [asdict(row) for row in observations.nodes],
        'cross_links': cross_links_payload(
            build_cross_links(observations),
            {e.record_key: e.evidence_id for e in evidence if e.record_key},
            thresholds),
        'evidence': [dict(evidence_id=e.evidence_id, event_time=e.event_time.astimezone(KST).isoformat(),
            source=e.source, time_origin=e.time_origin,
            provenance=e.provenance.model_dump(mode='json') if e.provenance else None,
            message=e.message[:400]) for e in evidence],
    }
    def encode():
        return json.dumps(data, ensure_ascii=False,
            default=lambda value: value.astimezone(KST).isoformat() if isinstance(value, datetime) else str(value))
    # JSON 전체가 max_chars를 넘지 않도록 줄인다. 원본 DTO는 건드리지 않고,
    # 줄인 사실은 context_omissions로 모델에 드러낸다.
    omissions = data['context_omissions']
    nodes = [row for row in observations.nodes if row.samples > 0]
    data['node_metric_maxima'] = {
        field: {'value': max(getattr(n, field) for n in nodes),
                'nodes': [n.node for n in nodes if getattr(n, field) == max(getattr(x, field) for x in nodes)]}
        for field in ('cpu_max', 'jvm_heap_max', 'search_queue_max', 'write_queue_max',
                      'search_rejected_max', 'write_rejected_max')
    } if nodes else {}
    if len(encode()) > max_chars:
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
    if len(encode()) > max_chars:
        for key in ('slow_executions', 'source_statuses', 'node_metric_maxima', 'evidence'):
            data[key] = compact(data[key])
        for row in data['slow_executions']:
            if len(row['conditions']) > 20:
                omissions['conditions'] = omissions.get('conditions', 0) + len(row['conditions']) - 20
                row['conditions'] = row['conditions'][:20]
    # 위 압축으로도 넘칠 때만 선택 행을 뒤에서부터 버린다. 정확한 건수·최댓값은 남기며,
    # 메타데이터가 없다고 정상이나 0으로 읽히면 안 된다.
    def over_budget() -> bool:
        return len(encode()) > max_chars - 100

    def shrink_cross_links() -> None:
        # cross_links는 원본에서 파생한 요약이다. 먼저 줄이지 않으면 큰 cross_links 때문에
        # 파생이 아닌 1차 데이터(evidence, slow_executions)가 먼저 비워진다.
        # verdict는 판정 자체이므로 어떤 경우에도 버리지 않는다.
        links = data['cross_links']

        def drop(field: str, index: int) -> None:
            links[field].pop(index)
            omissions[f'cross_links.{field}'] = omissions.get(f'cross_links.{field}', 0) + 1

        # 조용한 분(느린 쿼리·마스터 대기 모두 0)은 정보가 가장 적어 먼저 버린다.
        minutes = links['minutes']
        for index in range(len(minutes) - 1, -1, -1):
            if not over_budget():
                return
            if minutes[index]['slow_queries'] == 0 and minutes[index]['master_waits'] == 0:
                drop('minutes', index)
        for field in ('minutes', 'master_wait_overlaps', 'node_cards', 'slow_query_profile',
                      'slow_concentration'):
            while links[field] and over_budget():
                drop(field, len(links[field]) - 1)

    if over_budget():
        shrink_cross_links()
    for field in ('node_metrics', 'evidence', 'source_statuses', 'slow_executions', 'minute_observations'):
        while data[field] and over_budget():
            data[field].pop()
            omissions[field] = omissions.get(field, 0) + 1
    while over_budget():
        # 동률 노드 목록은 클러스터 규모만큼 길어질 수 있다. 목록만 줄이고 최댓값은 코드가 계산한 그대로 둔다.
        ties = [entry['nodes'] for entry in data['node_metric_maxima'].values() if entry['nodes']]
        if not ties:
            break
        max(ties, key=len).pop()
        omissions['maximum_node_names'] = omissions.get('maximum_node_names', 0) + 1
    # 모든 줄이기가 끝난 뒤의 evidence 기준으로, 남아 있지 않은 근거 id를 cross_links가 인용하지 않게 지운다.
    # (id를 null로 바꾸면 글자 수가 줄기만 하므로 예산을 다시 넘지 않는다.)
    kept_ids = {item['evidence_id'] for item in data['evidence']}
    for overlap in data['cross_links']['master_wait_overlaps']:
        for key in ('overlap_under_both_assumptions', 'overlap_under_one_assumption'):
            for interval in overlap[key]:
                if interval['evidence_id'] not in kept_ids:
                    interval['evidence_id'] = None
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
    """기존 source 상한과 전체 상한 안에서 상위 execution을 먼저 확보한다."""
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
