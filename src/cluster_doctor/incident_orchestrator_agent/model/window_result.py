"""Immutable result of one validated analysis window."""

from pydantic import BaseModel, ConfigDict

from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange


class WindowResult(BaseModel):
    """A report belongs to a window; it is not mutable execution state."""

    model_config = ConfigDict(frozen=True)
    window: TimeRange
    report: LogAnalysisReport
