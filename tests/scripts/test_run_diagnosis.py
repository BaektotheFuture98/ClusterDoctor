import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from cluster_doctor.application.use_cases.diagnose_incident import (
    IncidentDiagnostics,
    IncidentOutcome,
)
from cluster_doctor.application.ports.report_publisher import ReportPublication
from cluster_doctor.domain.diagnosis.observations import Observations
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.domain.incident.models import IncidentStatus


def _load_script():
    path = Path(__file__).parents[2] / "scripts" / "run_diagnosis.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("run_diagnosis_for_test", path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def test_run_delegates_to_public_manual_use_case_and_prints_public_diagnostics(
    monkeypatch, tmp_path, capsys
):
    module = _load_script()
    moment = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
    report = LogAnalysisReport(
        incident_id="manual-1", analyzed_from=moment, analyzed_to=moment,
        verification_status=VerificationStatus.MISMATCH,
        verification_issues=("evidence missing",),
    )
    diagnostics = IncidentDiagnostics(
        report=report, observations=Observations(), evidence=(),
        publication=ReportPublication(text_length=321),
    )

    class FakeManualDiagnosis:
        async def handle(self, moments):
            assert moments == [moment]
            return IncidentOutcome(
                incident_id="manual-1", status=IncidentStatus.COMPLETED,
                diagnostics=diagnostics,
            )

    monkeypatch.setattr(module, "get_settings", lambda: SimpleNamespace(report_dir=tmp_path))
    monkeypatch.setattr(module, "build_manual_diagnosis", lambda _settings: FakeManualDiagnosis())

    assert module.run([moment]) == 0
    output = capsys.readouterr().out
    assert "검증            : MISMATCH" in output
    assert "Evidence        : 0건" in output
    assert "리포트 길이     : 321자" in output
    assert "관측값" in output
