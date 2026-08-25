from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class LogEntryResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    timestamp: datetime
    level: str
    source: str
    message: str
    node: str | None = None
    component: str | None = None
    company: str | None = None
    user: str | None = None


class LogsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    total: int
    logs: list[LogEntryResponse]


class MinutePromptResponse(BaseModel):
    minute: str
    log_count: int
    prompt: str


class PromptsResponse(BaseModel):
    total_logs: int
    buckets: int
    minute_prompts: list[MinutePromptResponse]


class DiagnosisRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: datetime = Field(alias="from")
    to: datetime
    provider: str | None = None
    model: str | None = None


class DiagnosisResponse(BaseModel):
    # 필드에 alias가 있어도 기존 필드 이름으로도 값을 넣을 수 있게 해줌
    model_config = ConfigDict(populate_by_name=True)

    from_: str           = Field(alias="from")
    to: str
    analyzed_at: str     = Field(alias="analyzedAt")
    total_logs: int      = Field(alias="totalLogs")
    log_level_counts: dict[str, int] = Field(alias="logLevelCounts")
    report: str
