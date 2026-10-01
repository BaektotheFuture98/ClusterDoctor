"""Standalone SVG: counts and maximum runtime on one time axis."""
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc


def render_query_trend(points: tuple[QueryTrendPoint, ...]) -> str:
    if not points:
        return '<section id="query-trend"><h2>쿼리 실행 추이</h2><p>추이 자료 없음</p></section>'
    width,height=900,270
    step=800/max(len(points),1)
    maxcount=max([p.count or 0 for p in points]+[1])
    maxruntime=max([float(p.maximum_seconds or 0) for p in points]+[1.0])
    out=['<section id="query-trend"><h2>쿼리 실행 추이</h2><p class="hint">막대: 수집 실행 로그 건수 · 선: 최대 실행시간(s)</p>',
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="분별 실행 로그 건수와 최대 실행시간">',
        '<line x1="50" y1="210" x2="850" y2="210" stroke="#abb4c2"/>',
        f'<text x="5" y="30">{maxcount}건</text><text x="850" y="30">{maxruntime:g}s</text>']
    segments=[];current=[]
    for n,p in enumerate(points):
        x=50+step*(n+.5)
        stamp=p.start.astimezone(KST).strftime('%H:%M:%S')
        label=f'{stamp} ~ {p.end.astimezone(KST):%H:%M:%S} · {p.status} · {p.count if p.count is not None else "미확인"}건 · {p.maximum_seconds if p.maximum_seconds is not None else "미확인"}s'
        if p.count is not None:
            h=160*p.count/maxcount
            out.append(f'<rect x="{x-step*.3:.2f}" y="{210-h:.2f}" width="{step*.6:.2f}" height="{h:.2f}" fill="#bdd4ff"><title>{esc(label)}</title></rect>')
        else:
            out.append(f'<text x="{x:.2f}" y="180" text-anchor="middle">{esc("실패" if p.status=="failed" else "미확인")}</text>')
        if p.maximum_seconds is not None:
            y=210-160*float(p.maximum_seconds)/maxruntime
            current.append(f'{x:.2f},{y:.2f}')
            out.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4" fill="#1555aa"><title>{esc(label)}</title></circle>')
        elif current:
            segments.append(current);current=[]
        if len(points)<=12 or n%max(1,len(points)//10)==0 or n==len(points)-1:
            out.append(f'<text x="{x:.2f}" y="240" text-anchor="middle">{esc(stamp)}</text>')
    if current:segments.append(current)
    for segment in segments:
        out.append(f'<polyline points="{" ".join(segment)}" stroke="#1555aa" stroke-width="3" fill="none"/>')
    out.append('</svg><table class="trend-values"><thead><tr><th>관측 구간</th><th>실행 로그</th><th>최대(s)</th><th>수집</th></tr></thead><tbody>')
    names={'ok':'성공','failed':'실패','limited':'부분','unknown':'미확인'}
    for p in points:
        out.append(f'<tr><td>{esc(p.start.astimezone(KST).strftime("%H:%M:%S"))} ~ {esc(p.end.astimezone(KST).strftime("%H:%M:%S"))}</td><td>{p.count if p.count is not None else "—"}</td><td>{esc(str(p.maximum_seconds)) if p.maximum_seconds is not None else "—"}</td><td>{names[p.status]}</td></tr>')
    return ''.join(out)+ '</tbody></table></section>'
