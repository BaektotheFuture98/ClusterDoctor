"""LLM structured-output schemas local to minute analysis."""

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
