from datetime import datetime
from unittest.mock import ANY, MagicMock

import pytest
from fastapi.testclient import TestClient

from cluster_doctor.config.dependencies import get_diagnosis_use_case
from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.main import app

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 10))
REPORT = DiagnosisReport(
    time_range=TR,
    analyzed_at=datetime(2026, 8, 20, 2, 10, 5),
    total_logs=416,
    log_level_counts={"SUCCESS": 308, "FAIL": 1, "METRIC": 107},
    report="진단 결과 텍스트",
)
BODY = {"from": "2026-08-20T02:09:00", "to": "2026-08-20T02:10:00"}


@pytest.fixture
def mock_uc():
    mock_uc = MagicMock()
    mock_uc.diagnose.return_value = REPORT
    return mock_uc


@pytest.fixture
def client(mock_uc):
    app.dependency_overrides[get_diagnosis_use_case] = lambda: mock_uc
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_post_diagnosis_returns_json(client):
    resp = client.post("/api/v1/diagnosis", json=BODY)
    assert resp.status_code == 200
    data = resp.json()
    assert data["totalLogs"]                 == 416
    assert data["logLevelCounts"]["SUCCESS"] == 308
    assert data["logLevelCounts"]["FAIL"]    == 1
    assert data["logLevelCounts"]["METRIC"]  == 107
    assert data["report"]                    == "진단 결과 텍스트"
    assert data["from"]       == "2026-08-20T02:09:00"
    assert data["to"]         == "2026-08-20T02:10:00"
    assert data["analyzedAt"] == "2026-08-20T02:10:05"


def test_post_diagnosis_with_optional_model(client, mock_uc):
    resp = client.post("/api/v1/diagnosis", json={**BODY, "model": "gemini-2.5-pro"})
    assert resp.status_code == 200
    mock_uc.diagnose.assert_called_once_with(ANY, "gemini-2.5-pro")


def test_post_diagnosis_without_model_passes_none(client, mock_uc):
    resp = client.post("/api/v1/diagnosis", json=BODY)
    assert resp.status_code == 200
    mock_uc.diagnose.assert_called_once_with(ANY, None)


def test_post_diagnosis_text_returns_plain(client):
    resp = client.post("/api/v1/diagnosis/text", json=BODY)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert resp.text == "진단 결과 텍스트"


def test_post_diagnosis_invalid_range_returns_400(client):
    body = {"from": "2026-08-20T02:10:00", "to": "2026-08-20T02:09:00"}
    resp = client.post("/api/v1/diagnosis", json=body)
    assert resp.status_code == 400
    assert "error" in resp.json()
