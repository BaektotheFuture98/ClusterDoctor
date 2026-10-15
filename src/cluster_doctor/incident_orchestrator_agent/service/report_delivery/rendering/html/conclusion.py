"""리포트 첫 화면: 코드가 판정한 지연 범위, 노드 응답 지연, 원인·조치, 지표 카드.

심각도 배지는 두지 않는다. 첫 줄은 코드 판정이고 그 아래가 모델의 해석이다. 모델 요약 문단은 보이지 않는다.
"""
from cluster_doctor.incident_analysis_agent.service.observation.correlation import CrossLinks
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.cross_links_text import cluster_event_line, late_nodes, node_line, verdict_line
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc


def render_conclusion(links: CrossLinks, *, causes_html: str = '', actions_html: str = '', notices_html: str = '',
                      analysis_failed: bool = False) -> str:
    verdict = verdict_line(links)
    out = ['<section id="conclusion"><h2>결론</h2>', f'<p class="verdict">{esc(verdict.headline)}</p>']
    if verdict.detail:
        out.append(f'<p class="hint">{esc(verdict.detail)}</p>')
    if line := node_line(links):
        out.append(f'<p class="verdict">{esc(line)}</p>')
    if line := cluster_event_line(links):
        out.append(f'<p class="hint">{esc(line)}</p>')
    out.append(causes_html)
    out.append(actions_html)
    if analysis_failed:
        out.append('<p class="hint">AI 분석에 실패해 관측값만 표시합니다.</p>')
    out.append(notices_html)
    return ''.join(out) + '</section>'


def render_metric_cards(rows, links: CrossLinks) -> str:
    maximum = f'{rows[0].execution_seconds}s' if rows and rows[0].execution_seconds is not None else '미확인'
    # 로그가 없거나 못 읽었으면 0건이 아니라 미확인이다.
    slow_count = ('미확인' if links.verdict.scope == 'no_query_log'
                  else f'{sum(m.slow_queries for m in links.minutes)}건')
    cards = (
        ('수집 쿼리 실행 로그', f'{len(rows)}건'),
        ('최대 실행시간', maximum),
        (f'{links.slow_threshold}초 이상 실행', slow_count),
        ('마스터 응답 지연 노드', f'{len(late_nodes(links))}대'),
    )
    return ('<section id="metrics" class="cards">'
            + ''.join(f'<div class="card">{esc(label)}<strong>{esc(value)}</strong></div>' for label, value in cards)
            + '</section>')
