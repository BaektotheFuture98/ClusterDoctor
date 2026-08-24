from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.application.port.inbound.diagnosis_use_case import DiagnosisUseCase
from cluster_doctor.application.port.outbound.llm_analyzer import LlmAnalyzer
from cluster_doctor.application.port.outbound.log_repository import LogRepository


class DiagnosisService(DiagnosisUseCase):
    def __init__(self, log_repository: LogRepository, llm_analyzer: LlmAnalyzer):
        self._log_repository = log_repository
        self._llm_analyzer   = llm_analyzer

    def diagnose(self, time_range: TimeRange, model: str | None) -> DiagnosisReport:
        logs        = self._log_repository.fetch_logs(time_range)
        report_text = self._llm_analyzer.analyze(time_range, logs, model)
        return DiagnosisReport.create(time_range, logs, report_text)
