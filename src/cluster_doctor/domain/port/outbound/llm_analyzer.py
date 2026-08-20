from abc import ABC, abstractmethod

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange


class LlmAnalyzer(ABC):
    @abstractmethod
    def analyze(self, time_range: TimeRange, logs: list[LogEntry], model: str | None) -> str:
        ...
