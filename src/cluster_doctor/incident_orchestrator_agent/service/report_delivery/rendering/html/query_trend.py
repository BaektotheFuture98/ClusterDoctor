"""하나의 시간 축을 공유하는 횟수/최대 실행 시간 SVG 차트를 분리해 그린다."""
from math import ceil, log10

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc

_NAMES = {'ok': '성공', 'failed': '실패', 'limited': '부분', 'unknown': '미확인'}
_LEFT, _RIGHT, _TOP, _BASE = 55, 875, 30, 190


def _axis_max(value: float, *, counts: bool) -> float:
    if value <= 0:
        return 2 if counts else 1
    scale = 10 ** int(log10(value) // 1)
    maximum = ceil(value / scale) * scale
    return max(2, ceil(maximum / 2) * 2) if counts else maximum


def _number(value) -> str:
    return f'{value:.6g}' if value else '0'


def _label(point: QueryTrendPoint) -> str:
    count = f'{point.count}건' if point.count is not None else '미확인'
    runtime = f'{_number(point.maximum_seconds)}초' if point.maximum_seconds is not None else '미확인'
    return (f'{point.start.astimezone(KST):%Y-%m-%d %H:%M:%S} ~ '
            f'{point.end.astimezone(KST):%H:%M:%S} KST · {_NAMES[point.status]} · '
            f'요청 기록 수 {count} · 최대 실행시간 {runtime}')


def _render_chart(points: tuple[QueryTrendPoint, ...], *, counts: bool) -> str:
    title = '요청 기록 수 (건)' if counts else '분별 최대 실행시간 (초)'
    values = [p.count if counts else p.maximum_seconds for p in points]
    peak = max((float(v) for v in values if v is not None), default=0)
    maximum = _axis_max(peak, counts=counts)
    step = (_RIGHT - _LEFT) / len(points)
    out = [f'<figure class="trend-chart" style="margin:20px 0 28px;break-inside:avoid">'
           f'<figcaption style="font-weight:600;margin-bottom:10px">{title}</figcaption>',
           f'<svg viewBox="0 0 900 235" role="img" aria-label="{title}">',
           f'<title>{title}</title>']
    for value in (0, maximum / 2, maximum):
        y = _BASE - (_BASE - _TOP) * value / maximum
        out.append(f'<line x1="{_LEFT}" y1="{y:.2f}" x2="{_RIGHT}" y2="{y:.2f}" stroke="#d5dde3"/>')
        out.append(f'<text x="{_LEFT-12}" y="{y+4:.2f}" text-anchor="end">{_number(value)}</text>')
    segments, current, marks = [], [], []
    for n, (point, value) in enumerate(zip(points, values)):
        x = _LEFT + step * (n + .5)
        partial = point.status == 'limited' or (point.end - point.start).total_seconds() < 60
        if value is None:
            if current:
                segments.append(current)
                current = []
            # 성공했지만 비어 있는 분은 최댓값이 없다. 실패한 분과는 표시를 구분한다.
            missing = '실패' if point.status == 'failed' else '—'
            marks.append(f'<text x="{x:.2f}" y="{_BASE-10}" text-anchor="middle">{missing}<title>{esc(_label(point))}</title></text>')
        else:
            y = _BASE - (_BASE - _TOP) * float(value) / maximum
            if counts:
                color = '#b6cbd4' if partial else '#2f6c87'
                out.append(f'<rect x="{x-step*.3:.2f}" y="{y:.2f}" width="{step*.6:.2f}" height="{_BASE-y:.2f}" fill="{color}"><title>{esc(_label(point))}</title></rect>')
            else:
                current.append(f'{x:.2f},{y:.2f}')
                marks.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4" fill="#2f6c87"><title>{esc(_label(point))}</title></circle>')
            if len(points) <= 16 or float(value) == peak:
                marks.append(f'<text class="trend-value" x="{x:.2f}" y="{y-9:.2f}" text-anchor="middle">{_number(value)}</text>')
        if n % max(1, ceil(len(points) / 12)) == 0 or n == len(points)-1:
            marks.append(f'<text class="trend-time" x="{x:.2f}" y="218" text-anchor="middle">{point.minute.astimezone(KST):%H:%M}</text>')
    if current:
        segments.append(current)
    if not counts:
        for segment in segments:
            if len(segment) > 1:
                out.append(f'<polyline points="{" ".join(segment)}" stroke="#2f6c87" stroke-width="2.5" fill="none"/>')
    return ''.join(out + marks) + '</svg></figure>'


def render_query_trend(points: tuple[QueryTrendPoint, ...]) -> str:
    if not points:
        return '<section id="query-trend"><h2>쿼리 실행 추이</h2><p>추이 자료 없음</p></section>'
    out = ['<section id="query-trend"><h2>쿼리 실행 추이</h2>',
           _render_chart(points, counts=True), _render_chart(points, counts=False)]
    if any(p.status == 'limited' or (p.end-p.start).total_seconds() < 60 for p in points):
        out.append('<p class="hint">연한 막대: 부분 수집 구간</p>')
    out.append('<table class="trend-values"><thead><tr><th>관측 구간</th><th>실행 로그</th><th>최대(s)</th><th>수집</th></tr></thead><tbody>')
    for point in points:
        count = point.count if point.count is not None else '—'
        runtime = _number(point.maximum_seconds) if point.maximum_seconds is not None else '—'
        out.append(f'<tr><td>{point.start.astimezone(KST):%H:%M:%S} ~ {point.end.astimezone(KST):%H:%M:%S}</td><td>{count}</td><td>{runtime}</td><td>{_NAMES[point.status]}</td></tr>')
    return ''.join(out) + '</tbody></table></section>'
