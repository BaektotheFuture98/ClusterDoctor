"""Report publication result shared by lifecycle and delivery components."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ReportPublication:
    text_length: int = 0
