"""코드가 계산한 소스 간 연결을 그대로 보여준다. 해석은 결론과 원인 섹션의 몫이다."""
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.service.observation.correlation import CrossLinks, NodeCard, QueryInterval
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc


_OVERLAP_ROWS = 20


def _time(moment, fmt='%H:%M:%S.%f'):
    text = moment.astimezone(KST).strftime(fmt)
    return text[:-3] if fmt.endswith('%f') else text


def _queries(items: tuple[QueryInterval, ...]) -> str:
    if not items:
        return '없음'
    return '<br>'.join(f'{esc(_time(q.logged_at, "%H:%M:%S"))} · {esc(q.cmd)} · {esc(str(q.seconds))}s' for q in items)


def _node_log(card: NodeCard) -> str:
    if not card.node_log:
        return '조사 대상 아님'
    texts = []
    for status in card.node_log:
        # row_count가 비어 있는 것을 0줄로 읽으면 안 된다. 성공이 확인된 0줄만 그렇게 쓴다.
        if status.status == 'failed':
            texts.append(f'수집 실패 · {status.error}')
        elif status.status == 'ok' and status.row_count == 0:
            texts.append('조회 성공 · 0줄')
        elif status.row_count:
            texts.append(f'{status.row_count}줄 수집')
        else:
            texts.append('상태 미확인')
    return ' / '.join(esc(t) for t in texts)


def _minute_start(query: QueryInterval):
    return query.logged_at.replace(second=0, microsecond=0)


def render_cross_source(links: CrossLinks, *, master_note_text: str = '') -> str:
    out = ['<section id="cross-source"><h2>소스 간 연결</h2>',
           '<p class="hint">쿼리 로그 시각이 요청 시작인지 종료인지 확정되지 않아 두 경우를 모두 계산했습니다.</p>']
    # 겹치는 쿼리가 없는 대기는 표에서 정보가 없다. 건수만 적고 행은 줄인다.
    shown = [o for o in links.overlaps if o.both or o.one_assumption]
    empty_count = len(links.overlaps) - len(shown)
    if shown:
        out.append('<h3>마스터 대기 경고와 느린 쿼리의 겹침</h3><table><thead><tr><th>마스터 경고</th>'
                   '<th>응답 대기 구간</th><th>두 가정 모두 겹침</th><th>한 가정에서만 겹침</th></tr></thead><tbody>')
        for overlap in shown[:_OVERLAP_ROWS]:
            wait = overlap.wait
            timeout = f' · 제한 {wait.timeout_ms / 1000:.1f}s' if wait.timeout_ms is not None else ''
            out.append(
                f'<tr><td>{esc(wait.target_node or wait.target_node_id)} · {esc(wait.action)}{esc(timeout)}</td>'
                f'<td>{esc(_time(wait.sent_at))} ~ {esc(_time(wait.received_at))}</td>'
                f'<td>{_queries(overlap.both)}</td><td>{_queries(overlap.one_assumption)}</td></tr>')
        out.append('</tbody></table>')
        if len(shown) > _OVERLAP_ROWS:
            out.append(f'<p class="hint">외 {len(shown) - _OVERLAP_ROWS}건</p>')
    if empty_count:
        out.append(f'<p class="hint">겹치는 느린 쿼리가 없는 마스터 대기 경고 {empty_count}건</p>')
    if not links.overlaps:
        out.append(f'<p>마스터 로그 {esc(master_note_text)}</p>' if master_note_text
                   else '<p>연결할 마스터 대기 경고가 수집되지 않았습니다.</p>')
    # 그 분에 기록된 느린 쿼리가 어느 대기와도 겹치지 않을 때만 "대응하는 경고 없음"이라고 쓴다.
    overlapped_minutes = {_minute_start(q) for o in links.overlaps for q in (*o.both, *o.one_assumption)}
    for minute in links.minutes:
        if minute.slow_queries and not minute.master_waits and minute.minute not in overlapped_minutes:
            out.append(f'<p>{esc(_time(minute.minute, "%H:%M"))} 지연에는 대응하는 마스터 대기 경고가 수집되지 않았습니다.</p>')
    if links.nodes:
        out.append('<h3>경고 대상 노드</h3><table><thead><tr><th>노드</th><th>IP · 티어</th><th>마스터 경고</th>'
                   '<th>지표(구간 최대)</th><th>노드 로그</th><th>이 노드로 간 쿼리</th></tr></thead><tbody>')
        for card in links.nodes:
            metric = f'CPU {card.metrics.cpu_max}% · heap {card.metrics.jvm_heap_max}%' if card.metrics else '지표 없음'
            out.append(f'<tr><td>{esc(card.node)}</td><td>{esc(card.node_ip)} · {esc(card.tier or "티어 미확인")}</td>'
                       f'<td>{len(card.waits)}건</td><td>{esc(metric)}</td><td>{_node_log(card)}</td>'
                       f'<td>{card.queries_targeting}건</td></tr>')
        out.append('</tbody></table>')
    if links.profile:
        out.append(f'<h3>쿼리 형태별 실행시간 (느린 기준 {esc(str(links.slow_threshold))}s)</h3><table><thead><tr>'
                   '<th>cmd</th><th>조회 기간(일)</th><th>인덱스 수</th><th>건수</th><th>느린 건수</th>'
                   '<th>중앙값(s)</th><th>최대(s)</th></tr></thead><tbody>')
        for row in links.profile:
            cells = (row.cmd, row.date_range, row.index_count, row.total, row.slow,
                     row.median_seconds if row.median_seconds is not None else '—',
                     row.max_seconds if row.max_seconds is not None else '—')
            out.append('<tr>' + ''.join(f'<td>{esc(str(v))}</td>' for v in cells) + '</tr>')
        out.append('</tbody></table>')
    return ''.join(out) + '</section>'
