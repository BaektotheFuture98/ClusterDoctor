"""로컬에 저장한 리포트를 SFTP로도 올리는 게시자.

로컬 저장은 안쪽 게시자가 맡는다. 업로드가 실패해도 로컬 파일과 게시 결과는
그대로이고, 분석을 막지 않도록 예외를 밖으로 내지 않는다.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from pathlib import Path
from typing import Protocol

from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.file.report_publisher import (
    ReportPublisher,
)

_logger = logging.getLogger(__name__)


class _Uploader(Protocol):
    def upload(self, local_path: Path) -> str: ...


class SftpReportPublisher(ReportPublisher):
    def __init__(self, inner: ReportPublisher, uploader: _Uploader) -> None:
        self._inner = inner
        self._uploader = uploader

    async def publish(
        self,
        report: IncidentAnalysisReport,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> ReportPublication:
        publication = await self._inner.publish(
            report, gaps=gaps, analysis_failed=analysis_failed
        )
        if publication.path is None:
            return publication
        try:
            # 접속과 전송은 느릴 수 있어 같은 루프의 Kafka 소비를 막지 않게 스레드로 돌린다.
            remote_path = await asyncio.to_thread(self._uploader.upload, publication.path)
        except Exception as exc:  # noqa: BLE001
            _logger.error(
                "리포트 SFTP 업로드 실패 — 로컬 파일은 유지된다: %s (%s)",
                publication.path,
                exc,
            )
            return publication
        _logger.info("리포트 업로드: %s", remote_path)
        return replace(publication, remote_path=remote_path)
