"""검증된 분석 window 하나의 불변 결과."""

from pydantic import BaseModel, ConfigDict

from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange


class WindowResult(BaseModel):
    """리포트는 window에 속하며 가변 실행 state가 아니다."""

    model_config = ConfigDict(frozen=True)
    window: TimeRange
    report: LogAnalysisReport
