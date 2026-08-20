from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange


@dataclass
class DiagnosisReport:
    time_range: TimeRange
    analyzed_at: datetime
    total_logs: int
    log_level_counts: dict[str, int]
    report: str

    @classmethod
    def create(
        cls, time_range: TimeRange, logs: list[LogEntry], report_text: str
    ) -> "DiagnosisReport":
        counts = dict(Counter(log.level or "UNKNOWN" for log in logs))
        return cls(
            time_range=time_range,
            analyzed_at=datetime.now(),
            total_logs=len(logs),
            log_level_counts=counts,
            report=report_text,
        )
