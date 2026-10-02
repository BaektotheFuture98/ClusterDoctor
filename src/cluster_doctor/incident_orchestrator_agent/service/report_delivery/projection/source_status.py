"""Collection state per source, shared by the HTML and text renderers."""
from dataclasses import dataclass
from typing import Literal

from cluster_doctor.incident_analysis_agent.model.observations import SourceWindowStatus

QUERY_LOG_MISSING = '쿼리 실행 로그가 수집되지 않았습니다. 수집 상태를 확인하세요'


def _failed(prefix, reason):
    return f'{prefix}: {reason}' if reason else prefix


@dataclass(frozen=True)
class SourceSummary:
    state: Literal['ok', 'failed', 'skipped', 'unknown']
    rows: int | None
    reason: str


def summarize_source(statuses: tuple[SourceWindowStatus, ...], source: str) -> SourceSummary:
    mine = [s for s in statuses if s.source == source]
    if not mine:
        return SourceSummary('unknown', None, '')
    counts = [s.row_count for s in mine if s.row_count is not None]
    rows = sum(counts) if counts else None
    failed = [s for s in mine if s.status == 'failed']
    if failed:
        return SourceSummary('failed', rows, next((s.error for s in failed if s.error), ''))
    if all(s.status == 'skipped' for s in mine):
        return SourceSummary('skipped', rows, next((s.error for s in mine if s.error), ''))
    return SourceSummary('ok', rows, '')


def master_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'master_log')
    if summary.state == 'ok':
        return '특이사항 없음'
    if summary.state == 'failed':
        return _failed('수집 실패', summary.reason)
    return '수집 상태 미확인'


def slowlog_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'slowlog')
    if summary.state == 'ok':
        return f'수집 {summary.rows}건 중 선별된 항목 없음' if summary.rows else '특이사항 없음'
    if summary.state == 'failed':
        return _failed('수집 실패', summary.reason)
    return '수집 상태 미확인'


def ssh_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'node_log')
    if summary.state == 'skipped':
        if summarize_source(observations.source_statuses, 'master_log').state == 'failed':
            return '마스터 로그 수집 실패로 조사하지 않음'
        return '마스터 로그에서 조사 대상 노드가 없어 수집하지 않음'
    if summary.state == 'failed':
        return _failed('SSH 수집 실패', summary.reason)
    if summary.state == 'ok':
        return f'수집 {summary.rows}줄 중 선별된 항목 없음' if summary.rows else '해당 구간 로그 없음'
    return '수집 상태 미확인'


def query_log_note(observations) -> str:
    summary = summarize_source(observations.source_statuses, 'es_query_log')
    if summary.state == 'failed':
        return _failed('수집 실패', summary.reason)
    return QUERY_LOG_MISSING
