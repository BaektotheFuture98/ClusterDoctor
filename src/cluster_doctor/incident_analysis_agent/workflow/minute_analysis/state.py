"""minute analysis의 유일한 mutable LangGraph state schema."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    MinuteBucket,
    MinuteResult,
)


class MinuteAnalysisState(TypedDict):
    buckets: list[MinuteBucket]
    minute_results: Annotated[list[MinuteResult], operator.add]
    evidence: list[Evidence]
    reduce_degraded: bool
