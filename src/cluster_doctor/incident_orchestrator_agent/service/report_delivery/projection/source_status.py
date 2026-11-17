"""source별 수집 상태. HTML과 텍스트 renderer가 공유한다.

실패는 상태만 적고 사유는 적지 않는다. 수집 실패는 수집기가 gap도 함께 남기고
gap은 보고서 맨 아래 "수집·분석 누락"에 사유와 함께 실리므로, 여기서 사유를
다시 적으면 같은 실패가 두 번 보인다.
"""
from dataclasses import dataclass
from typing import Literal

from cluster_doctor.incident_analysis_agent.model.observations import SourceWindowStatus

QUERY_LOG_MISSING = '쿼리 실행 로그가 수집되지 않았습니다. 수집 상태를 확인하세요'


@dataclass(frozen=True)
class SourceSummary:
    state: Literal['ok', 'failed', 'skipped', 'unknown']
    rows: int | None


def summarize_source(statuses: tuple[SourceWindowStatus, ...], source: str) -> SourceSummary:
    mine = [s for s in statuses if s.source == source]
    if not mine:
        return SourceSummary('unknown', None)
    counts = [s.row_count for s in mine if s.row_count is not None]
    rows = sum(counts) if counts else None
    if any(s.status == 'failed' for s in mine):
        return SourceSummary('failed', rows)
    if all(s.status == 'skipped' for s in mine):
        return SourceSummary('skipped', rows)
    return SourceSummary('ok', rows)


def master_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'master_log')
    if summary.state == 'ok':
        return '특이사항 없음'
    if summary.state == 'failed':
        return '수집 실패'
    return '수집 상태 미확인'


def slowlog_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'slowlog')
    if summary.state == 'ok':
        return f'수집 {summary.rows}건 중 선별된 항목 없음' if summary.rows else '특이사항 없음'
    if summary.state == 'failed':
        return '수집 실패'
    return '수집 상태 미확인'


def ssh_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'node_log')
    if summary.state == 'skipped':
        if summarize_source(observations.source_statuses, 'master_log').state == 'failed':
            return '마스터 로그 수집 실패로 조사하지 않음'
        return '마스터 로그에서 조사 대상 노드가 없어 수집하지 않음'
    if summary.state == 'failed':
        return 'SSH 수집 실패'
    if summary.state == 'ok':
        return f'수집 {summary.rows}줄 중 선별된 항목 없음' if summary.rows else '해당 구간 로그 없음'
    return '수집 상태 미확인'


def query_log_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'es_query_log')
    if summary.state == 'failed':
        return '수집 실패'
    return QUERY_LOG_MISSING
