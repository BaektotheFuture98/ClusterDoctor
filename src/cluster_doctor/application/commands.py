"""Application commands passed between inbound use cases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from cluster_doctor.domain.incident.models import Incident


@dataclass(frozen=True)
class StartIncident:
    """A settled incident ready for diagnosis."""

    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float
