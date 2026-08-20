from datetime import datetime
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 10))

def _log(level: str) -> LogEntry:
    return LogEntry(timestamp=datetime(2026, 8, 20, 2, 9, 30), level=level, source="src", message="msg")

def test_create_counts_levels():
    logs = [_log("SUCCESS"), _log("SUCCESS"), _log("FAIL"), _log("METRIC")]
    report = DiagnosisReport.create(TR, logs, "분석 결과")
    assert report.total_logs == 4
    assert report.log_level_counts == {"SUCCESS": 2, "FAIL": 1, "METRIC": 1}

def test_create_empty_logs():
    report = DiagnosisReport.create(TR, [], "특이사항 없음")
    assert report.total_logs == 0
    assert report.log_level_counts == {}

def test_create_sets_fields():
    report = DiagnosisReport.create(TR, [], "진단 내용")
    assert report.report == "진단 내용"
    assert report.time_range == TR
    assert report.analyzed_at is not None
