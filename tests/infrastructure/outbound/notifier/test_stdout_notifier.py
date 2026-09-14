"""StdoutNotifier도 렌더링 실패에 리포트를 잃지 않는다.

예전에는 완성된 문자열을 로그에 넣을 뿐이라 실패 갈래가 없었다. 리포트가
객체가 되면서 ``notify`` 안에서 렌더링이 일어나는데, ``HtmlFileNotifier``가
모듈 docstring과 ``except Exception``으로 명시적으로 막아 둔 실패(짝 없는
서로게이트로 인한 인코딩 실패 등)를 이쪽은 하나도 막지 않고 있었다.

예외가 새면 ``_run_agent``의 ``succeeded``가 False로 남아 **리포트도 잃고
재트리거까지 막힌다** — HTML 쪽이 통째로 코드를 들여 막고 있는 그 사고다.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport, Observations
from cluster_doctor.infrastructure.outbound.notifier.stdout_notifier import (
    StdoutNotifier,
)

_REPORT = DiagnosisReport(observations=Observations(), narrative_text="평문 리포트")


def _notify(report: DiagnosisReport, **kwargs) -> None:
    asyncio.run(StdoutNotifier().notify(report, **kwargs))


def test_정상_리포트를_로그로_남긴다(caplog):
    with caplog.at_level("INFO"):
        _notify(_REPORT)

    assert "평문 리포트" in caplog.text


def test_렌더링이_실패해도_예외를_내지_않는다(caplog):
    with patch(
        "cluster_doctor.infrastructure.outbound.notifier.stdout_notifier.render_text",
        side_effect=ValueError("surrogates not allowed"),
    ):
        _notify(_REPORT)

    assert "렌더링 실패" in caplog.text


def test_렌더링이_실패하면_관측값이라도_남긴다(caplog):
    with (
        caplog.at_level("INFO"),
        patch(
            "cluster_doctor.infrastructure.outbound.notifier.stdout_notifier.render_text",
            side_effect=ValueError("boom"),
        ),
    ):
        _notify(_REPORT)

    assert "관측값" in caplog.text


def test_분석_실패와_누락은_따로_경고한다(caplog):
    with caplog.at_level("INFO"):
        _notify(_REPORT, gaps=("노드 로그 SSH 수집 실패",), analysis_failed=True)

    assert "분석 실패" in caplog.text
    assert "노드 로그 SSH 수집 실패" in caplog.text
