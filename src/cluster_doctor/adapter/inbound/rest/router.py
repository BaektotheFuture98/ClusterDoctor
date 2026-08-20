from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse

from cluster_doctor.adapter.inbound.rest.schemas import DiagnosisRequest, DiagnosisResponse
from cluster_doctor.config.dependencies import get_diagnosis_use_case
from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.port.inbound.diagnosis_use_case import DiagnosisUseCase

router = APIRouter(prefix="/api/v1")


def _run_diagnosis(request: DiagnosisRequest, use_case: DiagnosisUseCase) -> DiagnosisReport:
    time_range = TimeRange(start=request.from_, end=request.to)
    return use_case.diagnose(time_range, request.model)


@router.post("/diagnosis", response_model=DiagnosisResponse)
def diagnose(
    request: DiagnosisRequest,
    use_case: DiagnosisUseCase = Depends(get_diagnosis_use_case),
) -> DiagnosisResponse:
    report = _run_diagnosis(request, use_case)
    return DiagnosisResponse(
        from_=report.time_range.start.isoformat(),
        to=report.time_range.end.isoformat(),
        analyzed_at=report.analyzed_at.isoformat(),
        total_logs=report.total_logs,
        log_level_counts=report.log_level_counts,
        report=report.report,
    )


@router.post("/diagnosis/text", response_class=PlainTextResponse)
def diagnose_as_text(
    request: DiagnosisRequest,
    use_case: DiagnosisUseCase = Depends(get_diagnosis_use_case),
) -> str:
    report = _run_diagnosis(request, use_case)
    return report.report
