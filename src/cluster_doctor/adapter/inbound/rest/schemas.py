from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class DiagnosisRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: datetime = Field(alias="from")
    to: datetime
    model: str | None = None


class DiagnosisResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str           = Field(alias="from")
    to: str
    analyzed_at: str     = Field(alias="analyzedAt")
    total_logs: int      = Field(alias="totalLogs")
    log_level_counts: dict[str, int] = Field(alias="logLevelCounts")
    report: str
