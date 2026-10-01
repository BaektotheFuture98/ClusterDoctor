"""Observation detail group and the #metadata section; both are code-observed values."""

from datetime import datetime

from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    kst_stamp,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_ranking import (
    ranking_lines,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
    esc,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.summary_view import (
    format_window,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    candidate_details,
    candidate_line,
    health_lines,
    master_log_lines,
    node_lines,
    offender_lines,
    overview_lines,
    timeline_line,
)

# Rendered as structured metadata items, so the overview lines for them are skipped.
STRUCTURED_OVERVIEW = ("분석 구간", "사용한 시각 기준", "총 대기 시간")


def pre(text: str) -> str:
    return f'<pre class="raw"><code>{esc(text)}</code></pre>'


def detail_group(label: str, lines: list[str] | str) -> str:
    body = lines if isinstance(lines, str) else "\n".join(lines)
    return f'<details class="detail-group"><summary>{esc(label)}</summary>{pre(body)}</details>'


def render_observation_detail(report: IncidentAnalysisReport) -> str:
    obs = report.observations
    picks = (
        {p.candidate_id: p.reason for p in report.narrative.suspect_picks}
        if report.narrative
        else {}
    )
    candidates = []
    for candidate in obs.candidates:
        candidates += [
            candidate_line(candidate),
            *candidate_details(candidate, picks.get(candidate.candidate_id, "")),
        ]
    blocks = [
        ("전체 노드 지표 · 노드별 구간 최대값 (관측값)", node_lines(obs.nodes)),
        ("검색 요청 집계 (관측값)", ranking_lines(obs.query_requests)),
        ("느린 요청 후보 (관측값 + 모델 선정)", candidates),
        (
            "마스터 노드 로그 (관측값)",
            master_log_lines(obs.master_events, obs.master_log_total),
        ),
        ("가해자 집계 (관측값)", offender_lines(obs.candidates)),
        ("전체 분 단위 관측값", [timeline_line(row) for row in obs.timeline]),
    ]
    groups = "".join(detail_group(label, lines) for label, lines in blocks if lines)
    if not groups:
        return ""
    return f'<details class="detail-group observation-detail"><summary>관측 상세</summary>{groups}</details>'


def render_metadata(report: IncidentAnalysisReport, now: datetime) -> str:
    obs = report.observations
    items: list[tuple[str, str]] = [
        ("Evidence verification", report.verification_status)
    ]
    if obs.time_basis:
        items.append(("Time basis", obs.time_basis))
    if report.analyzed_from and report.analyzed_to:
        items.append(
            ("Analyzed", format_window(report.analyzed_from, report.analyzed_to))
        )
    items.append(("Generated", kst_stamp(now)))
    if obs.requested:
        items.append(
            (
                "수집 구간",
                " · ".join(format_window(start, end) for start, end in obs.requested),
            )
        )
    if obs.total_wait_seconds or obs.wait_cap_reached:
        cap = " (대기 상한 도달)" if obs.wait_cap_reached else ""
        items.append(("대기 시간", f"{obs.total_wait_seconds:.0f}초{cap}"))
    for line in overview_lines(obs):
        if not line.startswith(STRUCTURED_OVERVIEW):
            key, _, value = line.partition(": ")
            items.append((key, value) if value else ("관측 개요", line))

    out = [
        '<section id="metadata"><h2>Analysis Metadata</h2><dl class="meta-list">'
        + "".join(f"<div><dt>{esc(k)}</dt><dd>{esc(v)}</dd></div>" for k, v in items)
        + "</dl>"
    ]
    # Issues on a non-passing report are already listed in the alert above.
    if report.verification_status == "PASSED" and report.verification_issues:
        out.append(detail_group("검증 이슈", list(report.verification_issues)))
    health = health_lines(obs.health, obs.requested)
    if health:
        out.append(
            detail_group("참고: 클러스터 현재 상태 (사고 시각 상태 아님)", health)
        )
    if report.narrative_text:
        out.append(detail_group("모델 리포트 (평문)", report.narrative_text))
    out.append("</section>")
    return "".join(out)
