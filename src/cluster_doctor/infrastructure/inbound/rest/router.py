from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

from cluster_doctor.infrastructure.inbound.rest.schemas import DiagnosisRequest, DiagnosisResponse, LogEntryResponse, LogsResponse, MinutePromptResponse, PromptsResponse
from cluster_doctor.infrastructure.config.dependencies import get_diagnosis_use_case, get_log_repository
from cluster_doctor.infrastructure.outbound.llm.langgraph.prompts import build_minute_prompt
from cluster_doctor.application.port.outbound.llm_analyzer import LlmApiError, LlmResponseError
from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.domain.model.time_range import TimeRange

router = APIRouter(prefix="/api/v1")


def _run_diagnosis(request: DiagnosisRequest) -> DiagnosisReport:
    use_case = get_diagnosis_use_case(request.provider)
    time_range = TimeRange(start=request.from_, end=request.to)
    try:
        return use_case.diagnose(time_range, request.model)
    except LlmApiError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    except LlmResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.post("/diagnosis", response_model=DiagnosisResponse)
def diagnose(request: DiagnosisRequest) -> DiagnosisResponse:
    report = _run_diagnosis(request)
    return DiagnosisResponse(
        from_=report.time_range.start.isoformat(),
        to=report.time_range.end.isoformat(),
        analyzed_at=report.analyzed_at.isoformat(),
        total_logs=report.total_logs,
        log_level_counts=report.log_level_counts,
        report=report.report,
    )


@router.post("/diagnosis/text", response_class=PlainTextResponse)
def diagnose_as_text(request: DiagnosisRequest) -> str:
    return _run_diagnosis(request).report


@router.post("/prompts", response_model=PromptsResponse)
def fetch_prompts(request: DiagnosisRequest) -> PromptsResponse:
    time_range = TimeRange(start=request.from_, end=request.to)
    logs = get_log_repository().fetch_logs(time_range)

    grouped: dict = {}
    for log in logs:
        minute = log.timestamp.replace(second=0, microsecond=0)
        grouped.setdefault(minute, []).append(log)

    minute_prompts = [
        MinutePromptResponse(
            minute=minute.strftime("%Y-%m-%d %H:%M"),
            log_count=len(bucket),
            prompt=build_minute_prompt(bucket, minute.strftime("%Y-%m-%d %H:%M")),
        )
        for minute, bucket in sorted(grouped.items())
    ]
    return PromptsResponse(
        total_logs=len(logs),
        buckets=len(minute_prompts),
        minute_prompts=minute_prompts,
    )


@router.post("/logs", response_model=LogsResponse)
def fetch_logs(request: DiagnosisRequest) -> LogsResponse:
    time_range = TimeRange(start=request.from_, end=request.to)
    logs = get_log_repository().fetch_logs(time_range)
    return LogsResponse(
        total=len(logs),
        logs=[
            LogEntryResponse(
                timestamp=entry.timestamp,
                level=entry.level,
                source=entry.source,
                message=entry.message,
                node=entry.node,
                component=entry.component,
                company=entry.company,
                user=entry.user,
            )
            for entry in logs
        ],
    )
