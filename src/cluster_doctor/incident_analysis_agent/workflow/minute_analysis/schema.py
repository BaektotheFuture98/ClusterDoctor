"""minute analysis 안에서만 쓰는 LLM structured-output schema."""

from pydantic import BaseModel, Field


class MapSelection(BaseModel):
    record_id: int
    event_type: str = ""
    reason: str = ""


class MapOutput(BaseModel):
    selected: list[MapSelection] = Field(default_factory=list)


class ReduceSelection(BaseModel):
    record_id: int
    selection_reason: str = ""


class ReduceOutput(BaseModel):
    keep: list[ReduceSelection] = Field(default_factory=list)
