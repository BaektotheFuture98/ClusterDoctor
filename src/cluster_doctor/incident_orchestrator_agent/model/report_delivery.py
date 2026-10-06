"""Report publication result shared by lifecycle and delivery components."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ReportPublication:
    text_length: int = 0
    # 로컬에 저장한 HTML 파일. 저장에 실패하면 None.
    path: Path | None = None
    # 다른 서버에 올렸다면 그 서버의 경로. 올리지 않았거나 실패하면 None.
    remote_path: str | None = None
