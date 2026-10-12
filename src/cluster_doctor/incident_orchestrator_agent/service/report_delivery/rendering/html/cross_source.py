"""코드가 계산한 소스 간 연결을 그대로 보여준다. 해석은 결론과 원인 섹션의 몫이다."""
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.service.observation.correlation import CrossLinks, NodeCard, QueryInterval
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc


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
        if status.status == 'failed':
            texts.append(f'수집 실패 · {status.error}')
        elif not status.row_count:
            texts.append('조회 성공 · 0줄')
        else:
            texts.append(f'{status.row_count}줄 수집')
    return ' / '.join(esc(t) for t in texts)


def render_cross_source(links: CrossLinks) -> str:
    out = ['<section id="cross-source"><h2>소스 간 연결</h2>',
           '<p class="hint">쿼리 로그 시각이 요청 시작인지 종료인지 확정되지 않아 두 경우를 모두 계산했습니다.</p>']
    if links.overlaps:
        out.append('<h3>마스터 대기 경고와 느린 쿼리의 겹침</h3><table><thead><tr><th>마스터 경고</th>'
                   '<th>응답 대기 구간</th><th>두 가정 모두 겹침</th><th>한 가정에서만 겹침</th></tr></thead><tbody>')
        for overlap in links.overlaps:
            wait = overlap.wait
            timeout = f' · 제한 {wait.timeout_ms / 1000:.1f}s' if wait.timeout_ms is not None else ''
            out.append(
                f'<tr><td>{esc(wait.target_node or wait.target_node_id)} · {esc(wait.action)}{esc(timeout)}</td>'
                f'<td>{esc(_time(wait.sent_at))} ~ {esc(_time(wait.received_at))}</td>'
                f'<td>{_queries(overlap.both)}</td><td>{_queries(overlap.one_assumption)}</td></tr>')
        out.append('</tbody></table>')
    else:
        out.append('<p>연결할 마스터 대기 경고가 수집되지 않았습니다.</p>')
    for minute in links.minutes:
        if minute.slow_queries and not minute.master_waits:
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
