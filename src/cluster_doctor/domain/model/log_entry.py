from dataclasses import dataclass
from datetime import datetime


@dataclass
class LogEntry:
    timestamp: datetime
    level: str
    source: str
    message: str
    component: str | None = None
    node: str | None = None
