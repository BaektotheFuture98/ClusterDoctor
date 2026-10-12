"""리포트 첫 화면: 코드가 판정한 지연 범위, 노드 응답 지연, 모델 요약, 지표 카드.

심각도 배지는 두지 않는다. 첫 줄은 코드 판정이고 그 아래 문단이 모델의 해석이다.
"""
from cluster_doctor.incident_analysis_agent.service.observation.correlation import CrossLinks, NodeCard
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc


def _shape(shape: tuple[str, int, int]) -> str:
    cmd, days, indexes = shape
    return f'{cmd} · {days}일 · 인덱스 {indexes}개'


def _late_nodes(links: CrossLinks) -> tuple[NodeCard, ...]:
    # 노드 카드는 node-left 같은 줄에서도 만들어진다. 그런 노드는 늦게 응답한 것이 아니므로
    # 요청을 보낸 시각(sent_at)이 있는 대기가 하나라도 있는 노드만 센다.
    return tuple(card for card in links.nodes if any(wait.sent_at for wait in card.waits))


def verdict_line(links: CrossLinks) -> str:
    verdict = links.verdict
    shapes = ', '.join(_shape(s) for s in verdict.slow_shapes[:3])
    if verdict.scope == 'no_slow':
        return f'{links.slow_threshold}초 이상 실행된 요청이 없습니다.'
    if verdict.scope == 'specific':
        return (f'느린 요청은 {shapes} 형태에 한정됐습니다. 같은 시각 다른 요청의 실행시간 중앙값은 '
                f'{verdict.slow_minute_median}s로, 느린 요청이 없던 분({verdict.reference_median}s)과 비슷합니다.')
    if verdict.scope == 'cluster_wide':
        return (f'같은 시각 다른 요청도 함께 느려졌습니다. 다른 요청의 실행시간 중앙값이 '
                f'{verdict.reference_median}s에서 {verdict.slow_minute_median}s로 늘었습니다.')
    return f'느린 요청은 {shapes} 형태에서 나왔지만, 비교할 요청이 부족해 다른 요청도 느려졌는지는 판단할 수 없습니다.'


def node_line(links: CrossLinks) -> str:
    late = _late_nodes(links)
    if not late:
        return ''
    names = ', '.join(card.node for card in late[:4])
    return f'분석 구간에 데이터 노드 {len(late)}대({names})가 마스터 요청에 늦게 응답했습니다.'


def render_conclusion(links: CrossLinks, *, headline_html: str, notices_html: str = '',
                      analysis_failed: bool = False) -> str:
    out = ['<section id="conclusion"><h2>결론</h2>', f'<p class="verdict">{esc(verdict_line(links))}</p>']
    if line := node_line(links):
        out.append(f'<p class="verdict">{esc(line)}</p>')
    if headline_html:
        out.append(headline_html)
    if analysis_failed:
        out.append('<p class="hint">AI 분석에 실패해 관측값만 표시합니다.</p>')
    out.append(notices_html)
    return ''.join(out) + '</section>'


def render_metric_cards(rows, links: CrossLinks) -> str:
    maximum = f'{rows[0].execution_seconds}s' if rows and rows[0].execution_seconds is not None else '미확인'
    cards = (
        ('수집 쿼리 실행 로그', f'{len(rows)}건'),
        ('최대 실행시간', maximum),
        (f'{links.slow_threshold}초 이상 실행', f'{sum(m.slow_queries for m in links.minutes)}건'),
        ('응답 지연 노드', f'{len(_late_nodes(links))}대'),
    )
    return ('<section id="metrics" class="cards">'
            + ''.join(f'<div class="card">{esc(label)}<strong>{esc(value)}</strong></div>' for label, value in cards)
            + '</section>')
