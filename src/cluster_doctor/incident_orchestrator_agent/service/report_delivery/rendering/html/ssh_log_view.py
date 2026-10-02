from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc


def ssh_evidence(evidence, limit=10):
    items=[e for e in evidence if e.source=='node_log' and e.provenance and e.provenance.method=='ssh']
    severity={'Critical':3,'ERROR':3,'Warning':2,'WARN':2,'Info':1}
    return tuple(sorted(items,key=lambda e:(e.time_origin!='parsed',-severity.get(e.severity or '',0),e.event_time))[:limit])


def render_ssh_logs(evidence: tuple[Evidence, ...], *, limit: int = 10) -> str:
    items=ssh_evidence(evidence,limit)
    if not items:return '<section id="ssh-logs"><h2>SSH 노드 로그</h2></section>'
    rows=[]
    for e in items:
        time=stamp(e.event_time) if e.time_origin=='parsed' else ('앞선 로그 문맥 · 시각 미확인' if e.time_origin=='inherited' else '시각 미확인')
        rows.append('<tr>'+''.join(f'<td>{esc(v)}</td>' for v in (time,e.node_name or e.node_id or e.provenance.host or '미확인',e.severity or '미확인'))+
            f'<td><pre>{esc(e.raw or e.message)}</pre>{"<p>원문 잘림</p>" if e.raw_truncated else ""}</td><td>{esc(e.provenance.file_path or "경로 미확인")}</td></tr>')
    return '<section id="ssh-logs"><h2>SSH 노드 로그</h2><table><thead><tr><th>로그 시각</th><th>ES 노드</th><th>레벨</th><th>원문</th><th>파일 경로</th></tr></thead><tbody>'+''.join(rows)+'</tbody></table></section>'
