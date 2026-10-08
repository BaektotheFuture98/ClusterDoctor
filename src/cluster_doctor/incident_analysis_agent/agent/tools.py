"""Analysis tools read runtime state and return replacement updates."""

from __future__ import annotations

import json
from datetime import timedelta

from langchain.tools import ToolRuntime
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.types import Command

from cluster_doctor.incident_analysis_agent.agent.dependencies import AnalysisSeams
from cluster_doctor.incident_analysis_agent.model.kst import parse_kst
from cluster_doctor.incident_analysis_agent.model.time_range import (
    TimeRange,
    split_span,
)
from cluster_doctor.incident_analysis_agent.service.evidence_collection.collector import (
    EvidenceCollector,
)
from cluster_doctor.incident_analysis_agent.service.observation.builder import (
    ObservationBuilder,
)

_MAX_REPORT_ATTEMPTS = 2


def collect_update(seams: AnalysisSeams, state: dict) -> dict:
    """Run collection locally; only immutable values escape this call."""
    if state.get("collected"):
        return {}
    request = state["request"]
    builder = ObservationBuilder.from_state(request.analysis_window, state)
    sequence = int(state.get("evidence_sequence", 0))

    def next_id() -> str:
        nonlocal sequence
        sequence += 1
        return f"E-{request.incident_id}-{sequence}"

    try:
        collector = EvidenceCollector(
            new_evidence_id=next_id,
            fetch_logs=seams.fetch_logs,
            fetch_node_logs=seams.fetch_node_logs,
            cluster=seams.cluster,
            node_resolver=seams.node_resolver,
            node_log_fetcher=seams.node_log_fetcher,
            call_llm=seams.call_llm,
            metric_thresholds=seams.metric_thresholds,
            analysis_concurrency=seams.analysis_concurrency,
        )
        collected = collector.collect(request.analysis_window, builder)
        evidence = tuple(collected.evidence)
        failed_minutes = tuple(sorted(collected.failed_minutes))
        investigated = tuple(collected.investigated_nodes)
    except Exception as exc:
        builder.mark_gap(f"근거 수집이 실패했다: {exc}")
        builder.degraded = True
        evidence, failed_minutes, investigated = (), (), ()
    unresolved = (
        (request.analysis_window,)
        if builder.degraded
        else tuple(
            TimeRange(start=m, end=m + timedelta(minutes=1)) for m in failed_minutes
        )
    )
    return {
        "collected": True,
        "evidence": evidence,
        "evidence_sequence": sequence,
        "failed_minutes": failed_minutes,
        "investigated_nodes": investigated,
        "unresolved_gaps": unresolved,
        **builder.state_update(),
    }


def report_update(seams: AnalysisSeams, state: dict, focus: str) -> dict:
    """Writing result: state replacements plus local draft suggestions.

    draft_suggested_windows is response metadata, not an Agent State channel.
    """
    attempts = int(state.get("report_attempts", 0))
    if (
        not state.get("collected")
        or not state.get("evidence")
        or attempts >= _MAX_REPORT_ATTEMPTS
    ):
        return {}
    request = state["request"].model_copy(
        update={"analysis_goal": focus or state["request"].analysis_goal}
    )
    builder = ObservationBuilder.from_state(request.analysis_window, state)
    update = {"report_attempts": attempts + 1}
    try:
        draft = seams.report_writer.draft_report(
            request, list(state["evidence"]), builder
        )
        report = draft.to_domain(
            incident_id=request.incident_id,
            window=request.analysis_window,
            evidence_refs=tuple(item.evidence_id for item in state["evidence"]),
        )
        # report_insufficient로 명시한 결정을 유지한다. 초안의 제안 구간은
        # 모델에게 보여 줄 뿐 자동으로 채택하지 않는다.
        update.update(
            report=report, draft_suggested_windows=tuple(draft.parsed_windows())
        )
    except Exception as exc:
        builder.mark_gap(f"리포트 작성이 실패했다: {exc}")
    return {**update, **builder.state_update()}


def _response(runtime: ToolRuntime, update: dict, payload: dict) -> Command:
    return Command(
        update={
            **update,
            "messages": [
                ToolMessage(
                    json.dumps(payload, ensure_ascii=False, default=str),
                    tool_call_id=runtime.tool_call_id,
                )
            ],
        }
    )


def _build_tools(seams: AnalysisSeams) -> list:
    @tool
    def collect_evidence(runtime: ToolRuntime) -> Command:
        """Collect approved-window evidence once; preserve successes and source/window failure gaps."""
        state = runtime.state
        update = collect_update(seams, state)
        projected = {**state, **update}
        request = projected.get("request")
        builder = (
            ObservationBuilder.from_state(request.analysis_window, projected)
            if request
            else None
        )
        return _response(
            runtime,
            update,
            {
                "evidence_count": len(projected.get("evidence", ())),
                "cached": bool(state.get("collected")),
                "investigated_nodes": projected.get("investigated_nodes", ()),
                "failed_minute_count": len(projected.get("failed_minutes", ())),
                "observation_summary": builder.summary_for_prompt() if builder else "",
                "suspect_candidates": builder.candidates_for_prompt()
                if builder
                else "",
                "gaps": projected.get("gaps", ()),
            },
        )

    @tool
    def write_report(focus: str, runtime: ToolRuntime) -> Command:
        """Write an unverified report; requires evidence, at most two attempts."""
        state = runtime.state
        if not state.get("collected") or not state.get("evidence"):
            return _response(
                runtime,
                {},
                {
                    "error": "근거를 먼저 수집해야 한다. 근거가 없으면 report_insufficient를 불러라."
                },
            )
        if state.get("report_attempts", 0) >= _MAX_REPORT_ATTEMPTS:
            return _response(
                runtime, {}, {"error": "리포트 작성 상한 2회를 이미 썼다."}
            )
        update = report_update(seams, state, focus)
        report = update.get("report")
        wanted = update.pop("draft_suggested_windows", ())
        return _response(
            runtime,
            update,
            {
                "verification_status": report.verification_status
                if report
                else "NOT_VERIFIED",
                "error": None if report else "리포트 작성에 실패했다.",
                "finding_count": len(report.findings) if report else 0,
                "attempts_left": _MAX_REPORT_ATTEMPTS - update["report_attempts"],
                "draft_suggested_windows": tuple(
                    f"{w.start.isoformat()}/{w.end.isoformat()}" for w in wanted
                ),
                "next_step": "구간 밖의 근거가 필요하면 report_insufficient로 제안해라.",
            },
        )

    @tool
    def report_insufficient(
        reason: str, suggested_windows: list[str], runtime: ToolRuntime
    ) -> Command:
        """Report missing context and propose windows without expanding scope."""
        accepted, rejected = [], []
        for item in suggested_windows or []:
            try:
                start, end = item.split("/", 1)
                pieces = split_span(parse_kst(start), parse_kst(end))
                if not pieces:
                    raise ValueError("시작은 끝보다 빨라야 한다")
                accepted.extend(pieces)
            except Exception as exc:
                rejected.append(f"{item!r}: {exc}")
        update = {
            "insufficient_reason": reason.strip(),
            "suggested_windows": tuple(accepted),
            "gaps": tuple(runtime.state.get("gaps", ()))
            + (
                (f"이 구간만으로는 부족하다: {reason.strip()}",)
                if reason.strip()
                else ()
            ),
        }
        return _response(
            runtime,
            update,
            {
                "accepted_windows": tuple(
                    f"{w.start.isoformat()}/{w.end.isoformat()}" for w in accepted
                ),
                "rejected": rejected,
                "note": "상위 Agent가 예산과 중복을 보고 실제 구간을 정한다.",
            },
        )

    return [collect_evidence, write_report, report_insufficient]
