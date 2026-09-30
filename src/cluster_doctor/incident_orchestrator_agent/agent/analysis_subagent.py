"""Main state ↔ Analysis contract adapter. Analysis never sees Main state."""

from deepagents import CompiledSubAgent
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda

from cluster_doctor.incident_analysis_agent.agent.subagent import run_analysis_agent
from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    AnalysisStatus,
    LogAnalysisRequest,
    LogAnalysisResponse,
    WindowAnalysisResult,
)
from cluster_doctor.incident_analysis_agent.model.kst import parse_kst
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    merge_observations,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisStatus
from cluster_doctor.incident_orchestrator_agent.agent.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    ANALYSIS_SUBAGENT,
)
from cluster_doctor.incident_orchestrator_agent.model.window_result import WindowResult
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    plan_new_windows,
)


def apply_analysis_result(parent: dict, result: WindowAnalysisResult) -> dict:
    reports = tuple(parent.get("window_results", ()))
    if result.report is not None:
        reports += (WindowResult(window=result.window, report=result.report),)
    verification_gaps = tuple(
        f"리포트 검증 불일치: {issue}"
        for item in reports
        for issue in item.report.verification_issues
    )
    response = LogAnalysisResponse(
        status=AnalysisStatus.FAILED
        if result.status is LogAnalysisStatus.FAILED or result.report is None
        else AnalysisStatus.COMPLETED,
        has_report=result.report is not None,
        verification_status=result.verification_status,
        failure_reason=(
            "; ".join(result.gaps)
            if result.status is LogAnalysisStatus.FAILED or result.report is None
            else "; ".join(result.report.verification_issues)
            if result.report and result.report.verification_issues
            else None
        ),
        analysis_summary=result.analysis_summary,
    )
    return {
        "latest_analysis_status": result.status,
        "latest_verification_status": result.verification_status,
        "window_results": reports,
        "evidence": tuple(parent.get("evidence", ())) + result.evidence,
        "observations": merge_observations(
            parent.get("observations", Observations()), result.observations
        ),
        "evidence_counter": result.evidence_sequence,
        "accumulated_gaps": tuple(
            dict.fromkeys(
                (*parent.get("accumulated_gaps", ()), *result.gaps, *verification_gaps)
            )
        ),
        "pending_windows": tuple(parent.get("pending_windows", ()))
        + tuple(plan_new_windows(result.suggested_windows, parent)),
        "unresolved_gaps": tuple(parent.get("unresolved_gaps", ()))
        + tuple(plan_new_windows(result.unresolved_gaps, parent)),
        ADMITTED_WINDOW: None,
        ADMITTED_GOAL: "",
        "admitted_task_call_id": None,
        "messages": [AIMessage(content=result.analysis_summary)],
        "structured_response": response,
    }


def build_analysis_subagent(*, seams, incident, model) -> CompiledSubAgent:
    def run(parent: dict) -> dict:
        admitted = parent.get(ADMITTED_WINDOW)
        if not admitted:
            return {"messages": [AIMessage(content="승인된 분석 구간이 없다.")]}
        window = TimeRange(
            start=parse_kst(admitted["start"]), end=parse_kst(admitted["end"])
        )
        request = LogAnalysisRequest(
            incident_id=incident.incident_id,
            cluster=incident.cluster,
            analysis_window=window,
            prior_report=parent["window_results"][-1].report
            if parent.get("window_results")
            else None,
            analysis_goal=parent.get(ADMITTED_GOAL, ""),
            evidence_sequence_start=parent.get("evidence_counter", 0),
            prior_candidate_ids=tuple(
                c.candidate_id
                for c in parent.get("observations", Observations()).candidates
            ),
        )
        result = run_analysis_agent(seams=seams, request=request, model=model)
        return apply_analysis_result(parent, result)

    return CompiledSubAgent(
        name=ANALYSIS_SUBAGENT,
        description="승인된 analysis window 하나를 근거 수집·리포트 작성·원문 검증까지 조사한다.",
        runnable=RunnableLambda(run),
    )
