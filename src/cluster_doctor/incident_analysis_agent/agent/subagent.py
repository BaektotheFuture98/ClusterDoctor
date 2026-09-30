"""Analysis execution boundary, independent of Main Agent state."""

from __future__ import annotations

import logging

from deepagents import create_deep_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import HumanMessage

from cluster_doctor.incident_analysis_agent.agent.dependencies import AnalysisSeams
from cluster_doctor.incident_analysis_agent.agent.prompts.subagent_prompt import (
    build_subagent_prompt,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.harness import (
    DENY_ALL_FILESYSTEM,
    HideHarnessToolsMiddleware,
    RefuseDelegationMiddleware,
    restrict_harness,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.tool_batch import (
    AnalysisToolAdmissionMiddleware,
)
from cluster_doctor.incident_analysis_agent.agent.state import (
    AnalysisAgentState,
    initial_analysis_agent_state,
)
from cluster_doctor.incident_analysis_agent.agent.tools import (
    _build_tools,
    collect_update,
    report_update,
)
from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    LogAnalysisRequest,
    WindowAnalysisResult,
)
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.validation import (
    VerificationIssueType,
)
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import (
    validate_report,
)
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import (
    GroundingValidator,
)

_logger = logging.getLogger(__name__)


def _observations(state: dict) -> Observations:
    window = state["request"].analysis_window
    return Observations(
        time_basis=state.get("time_basis", ""),
        requested=((window.start, window.end),),
        timeline=tuple(state.get("timeline", ())),
        nodes=tuple(state.get("nodes", ())),
        master_events=tuple(state.get("master_events", ())),
        master_log_total=state.get("master_log_total", 0),
        health=tuple(state.get("health", ())),
        candidates=tuple(state.get("candidates", ())),
    )


def _execution_projection(state: dict) -> dict:
    return {
        key: value
        for key, value in state.items()
        if key in AnalysisAgentState.__annotations__ and key != "messages"
    }


def _validation_failure(state: dict, report, retries: int, exc: Exception) -> dict:
    """Commit even freshly consumed IDs when a validation service aborts."""
    _logger.exception("Analysis 검증 실패")
    reason = f"리포트 검증이 실패했다: {type(exc).__name__}"
    return _execution_projection(
        {
            **state,
            "execution_failed": True,
            "reanalysis_count": retries,
            "report": report.model_copy(
                update={
                    "verification_status": VerificationStatus.NOT_VERIFIED,
                    "verification_issues": (reason,),
                }
            ),
            "gaps": tuple(state.get("gaps", ())) + (reason,),
        }
    )


def finalize_update(seams: AnalysisSeams, state: dict) -> dict:
    """Bounded deterministic validation; return replacements to the same graph."""
    report = state.get("report")
    if report is None:
        return {}
    # Each assignment is a replacement projection, never an in-place state mutation.
    current = state
    grounding = GroundingValidator(call_llm=seams.call_llm)
    revisions = 0
    reanalysis_count = int(state.get("reanalysis_count", 0))
    issues, unverifiable = [], []
    for round_no in range(3):
        evidence = list(current.get("evidence", ()))
        candidate_ids = set(current["request"].prior_candidate_ids)
        candidate_ids.update(c.candidate_id for c in current.get("candidates", ()))
        try:
            deterministic = validate_report(
                report, evidence, candidate_ids=candidate_ids
            )
            found = grounding.validate(report, evidence)
        except Exception as exc:
            return _validation_failure(current, report, reanalysis_count, exc)
        analysis = [
            i.reason
            for i in found
            if i.issue_type is VerificationIssueType.ANALYSIS_MISMATCH
        ]
        expression = [
            i.reason
            for i in found
            if i.issue_type is VerificationIssueType.REPORT_MISMATCH
        ]
        unverifiable = [
            i.reason
            for i in found
            if i.issue_type is VerificationIssueType.UNVERIFIABLE
        ]
        issues = [*deterministic.issues, *analysis, *expression]
        if not issues or round_no == 2:
            break
        if analysis:
            if reanalysis_count >= 1:
                break
            reanalysis_count += 1
            request = current["request"].model_copy(
                update={
                    "analysis_goal": f"{current['request'].analysis_goal}\n[재분석] {'; '.join(analysis)}".strip(),
                }
            )
            fresh = initial_analysis_agent_state(
                request, evidence_sequence=current.get("evidence_sequence", 0)
            )
            fresh = {**fresh, **collect_update(seams, fresh)}
            fresh = {
                **fresh,
                **_execution_projection(report_update(seams, fresh, request.analysis_goal)),
            }
            if fresh.get("report") is None:
                current = {
                    **current,
                    "evidence_sequence": fresh["evidence_sequence"],
                    "gaps": tuple(current.get("gaps", ()))
                    + tuple(fresh.get("gaps", ()))
                    + ("원문 불일치 재분석에 실패했다.",),
                }
                break
            # Preserve the original explicit expansion decision and new collection gaps.
            current = {
                **fresh,
                "suggested_windows": tuple(current.get("suggested_windows", ())),
                "insufficient_reason": current.get("insufficient_reason", ""),
                "report_attempts": current.get("report_attempts", 0)
                + fresh.get("report_attempts", 0),
                "gaps": tuple(
                    dict.fromkeys((*current.get("gaps", ()), *fresh.get("gaps", ())))
                ),
            }
            report = current["report"]
            continue
        try:
            revised = seams.report_writer.revise_report(report, tuple(issues), evidence)
        except Exception as exc:
            return _validation_failure(current, report, reanalysis_count, exc)
        if revised is None:
            break
        report = revised
        revisions += 1
    status = (
        VerificationStatus.MISMATCH
        if issues
        else (
            VerificationStatus.NOT_VERIFIED
            if unverifiable
            else VerificationStatus.PASSED
        )
    )
    final_report = report.model_copy(
        update={
            "verification_status": status,
            "verification_issues": tuple(issues or unverifiable),
            "revision_count": revisions,
        }
    )
    return _execution_projection(
        {
            **current,
            "report": final_report,
            "reanalysis_count": reanalysis_count,
        }
    )


class AnalysisValidationMiddleware(AgentMiddleware):
    def __init__(self, seams: AnalysisSeams):
        self._seams = seams

    def after_agent(self, state, runtime):
        return finalize_update(self._seams, state)


def project_result(state: dict) -> WindowAnalysisResult:
    report = state.get("report")
    gaps = tuple(state.get("gaps", ()))
    suggested = tuple(state.get("suggested_windows", ()))
    if (
        state.get("execution_failed")
        or not state.get("collected")
        or (state.get("degraded") and not state.get("evidence"))
    ):
        status = LogAnalysisStatus.FAILED
    elif suggested:
        status = LogAnalysisStatus.NEED_MORE_CONTEXT
    else:
        status = LogAnalysisStatus.COMPLETED if report else LogAnalysisStatus.FAILED
    if report:
        # ReportWriter's formatting only reads gaps from its observation helper.
        from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
            ReportWriter,
        )
        from cluster_doctor.incident_analysis_agent.service.observation.builder import (
            ObservationBuilder,
        )

        summary = ReportWriter.summary_for_supervisor(
            report,
            ObservationBuilder.from_state(state["request"].analysis_window, state),
        )
    else:
        summary = f"근거 {len(state.get('evidence', ()))}건, 리포트 미작성."
        if state.get("insufficient_reason"):
            summary += f" 부족 사유: {state['insufficient_reason']}"
        if gaps:
            summary += " " + "; ".join(gaps)
    return WindowAnalysisResult(
        window=state["request"].analysis_window,
        status=status,
        verification_status=report.verification_status
        if report
        else VerificationStatus.NOT_VERIFIED,
        report=report,
        evidence=tuple(state.get("evidence", ())),
        observations=_observations(state),
        suggested_windows=suggested,
        unresolved_gaps=tuple(state.get("unresolved_gaps", ()))
        if state.get("collected")
        else (state["request"].analysis_window,),
        gaps=gaps,
        evidence_sequence=int(state.get("evidence_sequence", 0)),
        analysis_summary=summary,
    )


def run_analysis_agent(
    *, seams: AnalysisSeams, request: LogAnalysisRequest, model
) -> WindowAnalysisResult:
    restrict_harness(model)
    graph = create_deep_agent(
        model=model,
        tools=_build_tools(seams),
        state_schema=AnalysisAgentState,
        middleware=[
            AnalysisToolAdmissionMiddleware(),
            AnalysisValidationMiddleware(seams),
            HideHarnessToolsMiddleware(),
            RefuseDelegationMiddleware(),
        ],
        system_prompt=build_subagent_prompt(
            cluster=request.cluster,
            window=request.analysis_window,
            goal=request.analysis_goal,
        ),
        name="analysis_deepagent",
        permissions=DENY_ALL_FILESYSTEM,
    )
    final = {
        **initial_analysis_agent_state(request),
        "messages": [
            HumanMessage(content=request.analysis_goal or "이 구간을 분석해라.")
        ],
    }
    try:
        for snapshot in graph.stream(
            final, {"recursion_limit": 24}, stream_mode="values"
        ):
            final = snapshot
    except Exception as exc:
        _logger.exception("Analysis Agent 실행 실패")
        final = {
            **final,
            "execution_failed": True,
            "gaps": tuple(final.get("gaps", ()))
            + (f"진단 루프가 실패했다: {type(exc).__name__}",),
        }
    return project_result(final)
