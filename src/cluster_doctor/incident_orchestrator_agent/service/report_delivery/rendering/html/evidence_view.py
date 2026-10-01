"""The #evidence section: every collected Evidence rendered once, each with its anchor."""

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    kst_stamp,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
    anchor,
    esc,
)

SUMMARY_MESSAGE_LIMIT = 200
METHOD_LABEL = {
    "ssh": "SSH",
    "clickhouse": "ClickHouse",
    "elasticsearch_api": "Elasticsearch API",
}
TIME_ORIGIN_LABEL = {
    "inherited": "직전 로그에서 상속",
    "fallback": "조회 구간 시작으로 대체",
}


def _first_line(message: str) -> str:
    line = message.strip().splitlines()[0] if message.strip() else ""
    if len(line) > SUMMARY_MESSAGE_LIMIT:
        return line[:SUMMARY_MESSAGE_LIMIT] + "…"
    return line


def _fields(e: Evidence) -> list[tuple[str, str]]:
    p = e.provenance
    pairs: list[tuple[str, str | None]] = [
        ("Evidence ID", e.evidence_id),
        ("Event Time", kst_stamp(e.event_time)),
        ("Source", str(e.source)),
        ("Node", e.node_name or e.node_id),
        ("Event Type", e.event_type),
        ("Severity", e.severity),
        # A message identical to raw is shown once, in the raw block.
        ("Message", e.message if e.message != e.raw else None),
    ]
    if p:
        pairs += [
            ("Method", METHOD_LABEL[p.method]),
            ("Host", p.host),
            ("Table", p.table),
            ("File Path", p.file_path),
            ("Endpoint", p.endpoint),
            ("Query From", kst_stamp(p.query_from) if p.query_from else None),
            ("Query To", kst_stamp(p.query_to) if p.query_to else None),
            ("Collected At", kst_stamp(p.collected_at) if p.collected_at else None),
            ("Role", p.role),
            ("Excerpt", "필터링된 발췌 (전체 로그가 아님)" if p.excerpt else None),
        ]
    pairs += [
        ("Time Origin", TIME_ORIGIN_LABEL.get(e.time_origin)),
        ("Selection Reason", e.selection_reason),
    ]
    return [(k, v) for k, v in pairs if v]


def _raw_block(e: Evidence) -> str:
    # Only a collected raw is shown; message is a summary, never a stand-in for it.
    if not e.raw:
        return ""
    kind = " raw-query" if e.raw_kind == "query" else ""
    truncated = '<span class="truncated">잘림</span>' if e.raw_truncated else ""
    return (
        '<details class="raw-block"><summary>View raw log'
        f"{truncated}</summary>"
        f'<pre class="raw{kind}"><code>{esc(e.raw)}</code></pre></details>'
    )


def render_evidence_item(e: Evidence) -> str:
    node = e.node_name or e.node_id
    stamp = e.event_time.astimezone(KST).strftime("%H:%M")
    return (
        f'<details class="evidence-item" id="{anchor(e.evidence_id)}"><summary>'
        '<span class="evidence-head">'
        f'<span class="evidence-id">{esc(e.evidence_id)}</span>'
        f'<time datetime="{e.event_time.isoformat()}">{esc(stamp)}</time>'
        f'<span class="evidence-source">{esc(e.source)}</span>'
        + (f'<span class="evidence-node">{esc(node)}</span>' if node else "")
        + "</span>"
        f'<span class="evidence-message">{esc(_first_line(e.message))}</span>'
        "</summary>"
        '<dl class="evidence-fields">'
        + "".join(
            f"<div><dt>{esc(k)}</dt><dd>{esc(v)}</dd></div>" for k, v in _fields(e)
        )
        + "</dl>"
        + _raw_block(e)
        + "</details>"
    )


def render_evidence(evidence: tuple[Evidence, ...], observation_detail: str) -> str:
    items = sorted(evidence, key=lambda e: (e.event_time, e.evidence_id))
    return (
        '<section id="evidence"><h2>근거</h2>'
        '<p class="hint">수집된 근거 원문입니다. 항목을 펼치면 수집 정보와 원문을 볼 수 있습니다.</p>'
        + (
            '<div class="evidence-list">'
            + "".join(render_evidence_item(e) for e in items)
            + "</div>"
            if items
            else '<p class="hint">수집된 근거 없음</p>'
        )
        + observation_detail
        + "</section>"
    )
