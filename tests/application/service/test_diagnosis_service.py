from datetime import datetime
from unittest.mock import MagicMock

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.application.service.diagnosis_service import DiagnosisService

TR   = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 10))
LOGS = [LogEntry(timestamp=datetime(2026, 8, 20, 2, 9, 30), level="SUCCESS", source="es_query_log", message="msg")]


def test_diagnose_calls_repository_and_analyzer():
    repo     = MagicMock()
    repo.fetch_logs.return_value = LOGS
    analyzer = MagicMock()
    analyzer.analyze.return_value = "진단 결과"

    service = DiagnosisService(log_repository=repo, llm_analyzer=analyzer)
    report  = service.diagnose(TR, "gemini-2.5-flash")

    repo.fetch_logs.assert_called_once_with(TR)
    analyzer.analyze.assert_called_once_with(TR, LOGS, "gemini-2.5-flash")
    assert report.total_logs == 1
    assert report.report == "진단 결과"


def test_diagnose_passes_none_model():
    repo     = MagicMock()
    repo.fetch_logs.return_value = []
    analyzer = MagicMock()
    analyzer.analyze.return_value = "특이사항 없음"

    service = DiagnosisService(log_repository=repo, llm_analyzer=analyzer)
    service.diagnose(TR, None)

    analyzer.analyze.assert_called_once_with(TR, [], None)
