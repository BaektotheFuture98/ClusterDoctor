"""분석 tool은 runtime state를 읽고 교체용 update를 반환한다."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Annotated

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
    """수집을 로컬에서 실행한다. 이 호출 밖으로는 불변 값만 나간다."""
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
    """작성 결과. state 교체 값과 로컬 draft 제안으로 구성된다.

    draft_suggested_windows는 응답 메타데이터이며 Agent State 채널이 아니다.
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
    wiki_context = ""
    if seams.wiki_client is not None:
        try:
            parts = [
                p for p in [
                    builder.candidates_for_prompt(),
                    builder.summary_for_prompt(),
                    builder.master_logs_for_prompt(),
                ] if p
            ]
            symptoms = "\n\n".join(parts)
            if symptoms:
                wiki_context = seams.wiki_client.ask(symptoms)
        except Exception as exc:
            builder.mark_gap(f"위키 조회 실패: {exc}")
    try:
        draft = seams.report_writer.draft_report(
            request, list(state["evidence"]), builder, wiki_context=wiki_context
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
        """승인된 고정 구간의 근거를 수집한다. 실제 수집은 한 번만 수행한다.

        재호출은 cached=true인 기존 결과를 반환하며 근거를 추가하지 않는다.
        응답에는 근거 수, 관측 요약, 의심 후보, 실패한 분 수와 gaps가 포함된다.
        일부 소스가 실패해도 다른 소스의 성공 결과와 실패 공백을 보존한다.
        """
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

    @tool(
        description=f"""수집된 근거로 검증 전 리포트를 작성해 저장한다. 먼저 수집한 근거가 있어야 한다.
작성 시도는 최대 {_MAX_REPORT_ATTEMPTS}회다. 실패했고 attempts_left가 남았을 때만 재시도한다.
성공한 리포트를 같은 입력으로 반복 작성하지 않는다.
응답의 verification_status는 작성 직후 상태이며 최종 검증 결과가 아니다.
finding_count, error, attempts_left와 draft_suggested_windows도 반환한다.
초안의 제안 구간은 자동 채택되지 않는다."""
    )
    def write_report(
        focus: Annotated[
            str, "이번 조사 목표에 맞춰 리포트에서 답할 질문. 빈 문자열이면 승인된 조사 목표를 사용한다."
        ],
        runtime: ToolRuntime,
    ) -> Command:
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
        reason: Annotated[
            str, "확인하지 못한 내용과 부족한 근거. 다른 구간을 제안하면 그 시간대가 필요한 이유도 적는다."
        ],
        suggested_windows: Annotated[
            list[str],
            '추가 확인이 필요한 구간 목록. 각 항목은 시간대 오프셋을 포함한 ISO 8601 시작/종료 문자열이다. '
            '예: "2026-10-18T13:50:00+09:00/2026-10-18T13:55:00+09:00". '
            "타당한 추가 구간을 특정할 수 없으면 빈 목록을 사용한다.",
        ],
        runtime: ToolRuntime,
    ) -> Command:
        """현재 구간에서 부족한 근거를 기록하고 추가 분석 구간을 Main Agent에 제안한다.

        현재 분석 범위를 넓히거나 기존 리포트를 삭제하지 않는다.
        제안 구간 목록은 이번 입력으로 교체되며, 긴 구간은 조회 상한에 맞춰 분할한다.
        응답은 accepted_windows와 잘못된 항목의 rejected 사유를 반환한다.
        제안 수락은 실행 승인이 아니다. 실제 분석 여부는 Main Agent가 결정한다.
        """
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
