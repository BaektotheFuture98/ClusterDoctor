from abc import ABC, abstractmethod

from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.domain.model.time_range import TimeRange


class DiagnosisUseCase(ABC):
    @abstractmethod
    def diagnose(self, time_range: TimeRange, model: str | None) -> DiagnosisReport:
        ...
