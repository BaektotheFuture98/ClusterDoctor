from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse

from cluster_doctor.adapter.inbound.rest.schemas import DiagnosisRequest, DiagnosisResponse
from cluster_doctor.config.dependencies import get_diagnosis_use_case
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.port.inbound.diagnosis_use_case import DiagnosisUseCase

router = APIRouter(prefix="/api/v1")


@router.post("/diagnosis")
def diagnose(
    request: DiagnosisRequest,
    use_case: DiagnosisUseCase = Depends(get_diagnosis_use_case),
) -> dict:
    time_range = TimeRange(start=request.from_, end=request.to)
    report     = use_case.diagnose(time_range, request.model)
    return DiagnosisResponse(
        from_=str(report.time_range.start),
        to=str(report.time_range.end),
        analyzed_at=str(report.analyzed_at),
        total_logs=report.total_logs,
        log_level_counts=report.log_level_counts,
        report=report.report,
    ).model_dump(by_alias=True)


@router.post("/diagnosis/text", response_class=PlainTextResponse)
def diagnose_as_text(
    request: DiagnosisRequest,
    use_case: DiagnosisUseCase = Depends(get_diagnosis_use_case),
) -> str:
    time_range = TimeRange(start=request.from_, end=request.to)
    report     = use_case.diagnose(time_range, request.model)
    return report.report
