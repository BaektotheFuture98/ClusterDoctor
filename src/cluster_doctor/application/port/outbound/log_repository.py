from abc import ABC, abstractmethod

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange


class LogRepository(ABC):
    @abstractmethod
    def fetch_logs(self, time_range: TimeRange) -> list[LogEntry]:
        ...
