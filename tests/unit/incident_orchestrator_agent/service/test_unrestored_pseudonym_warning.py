import logging
from datetime import UTC, datetime

import pytest

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import unrestored_aliases
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    HtmlFileReportPublisher,
)

_T0 = datetime(2026, 10, 6, 9, 6, tzinfo=UTC)
_LOGGER = "cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier"


@pytest.mark.parametrize(
    "text, expected",
    [
        ("요청 호스트 ip-0006에서 발생", ("ip-0006",)),
        ("user-0010과 company-0003, ip-0006", ("company-0003", "ip-0006", "user-0010")),
        ("변형 ip-6 와 ip_0006 와 ip-0006abc", ("ip-0006abc", "ip-6", "ip_0006")),
        ("대문자 IP-0006", ("IP-0006",)),
        ("실제 값 192.168.1.40 logstash", ()),
        ("skip-0006 은 가명이 아니다", ()),
        ("", ()),
    ],
)
def test_unrestored_aliases_lists_pseudonym_like_strings(text, expected):
    assert unrestored_aliases(text) == expected


def _rendered(summary: str):
    report = LogAnalysisReport(
        incident_id="INC-1",
        analyzed_from=_T0,
        analyzed_to=_T0,
        summary=summary,
        verification_status=VerificationStatus.PASSED,
    )
    return to_incident_analysis_report(report, Observations(), [])


@pytest.mark.asyncio
async def test_publish_warns_when_a_pseudonym_is_left_in_the_report(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger=_LOGGER)

    await HtmlFileReportPublisher(output_dir=tmp_path).publish(
        _rendered("요청 호스트 ip-0006에서 bulk가 집중됐다")
    )

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "ip-0006" in warnings[0].getMessage()
    assert "bulk가 집중" not in warnings[0].getMessage()
    assert list(tmp_path.glob("*.html"))


@pytest.mark.asyncio
async def test_publish_does_not_warn_for_a_clean_report(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger=_LOGGER)

    await HtmlFileReportPublisher(output_dir=tmp_path).publish(
        _rendered("요청 호스트 192.168.1.40에서 bulk가 집중됐다")
    )

    assert not [r for r in caplog.records if r.levelno == logging.WARNING]
