from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.main import app

# The request's range and the mock's report range are deliberately different
# timestamps. If the router ever ignored `to` (e.g. building the range from
# `from_` plus a fixed minute) or echoed the request instead of the report,
# these two would need to collide to hide the bug -- keeping them distinct
# means the response-shape assertions can only pass by reading the report's
# own time_range, and REQUEST_TR below is the exact TimeRange the use case
# must be called with.
REQUEST_FROM = datetime(2026, 8, 20, 2, 9, 0)
# Deliberately more than one minute after REQUEST_FROM, so a router that
# built the range as `from_ + 1 minute` (ignoring `to`) would construct a
# different TimeRange than REQUEST_TR below and get caught by the
# assert_called_once_with checks.
REQUEST_TO   = datetime(2026, 8, 20, 2, 14, 0)
REQUEST_TR   = TimeRange(start=REQUEST_FROM, end=REQUEST_TO)

REPORT_TR = TimeRange(start=datetime(2026, 8, 20, 5, 35, 0), end=datetime(2026, 8, 20, 5, 45, 0))
REPORT = DiagnosisReport(
    time_range=REPORT_TR,
    analyzed_at=datetime(2026, 8, 20, 5, 45, 5),
    total_logs=416,
    log_level_counts={"SUCCESS": 308, "FAIL": 1, "METRIC": 107},
    report="진단 결과 텍스트",
)
BODY = {"from": "2026-08-20T02:09:00", "to": "2026-08-20T02:14:00"}


@pytest.fixture
def mock_uc():
    mock_uc = MagicMock()
    mock_uc.diagnose.return_value = REPORT
    return mock_uc


@pytest.fixture
def client(mock_uc):
    with patch(
        "cluster_doctor.infrastructure.inbound.rest.router.get_diagnosis_use_case",
        return_value=mock_uc,
    ):
        yield TestClient(app)


def test_post_diagnosis_returns_json(client):
    resp = client.post("/api/v1/diagnosis", json=BODY)
    assert resp.status_code == 200
    data = resp.json()
    assert data["totalLogs"]                 == 416
    assert data["logLevelCounts"]["SUCCESS"] == 308
    assert data["logLevelCounts"]["FAIL"]    == 1
    assert data["logLevelCounts"]["METRIC"]  == 107
    assert data["report"]                    == "진단 결과 텍스트"
    # These must come from REPORT_TR (the mock's report), not from BODY (the
    # request) -- the two are deliberately different so this proves which
    # one the handler echoes.
    assert data["from"]       == "2026-08-20T05:35:00"
    assert data["to"]         == "2026-08-20T05:45:00"
    assert data["analyzedAt"] == "2026-08-20T05:45:05"


def test_post_diagnosis_with_optional_model(client, mock_uc):
    resp = client.post("/api/v1/diagnosis", json={**BODY, "model": "gemini-2.5-pro"})
    assert resp.status_code == 200
    mock_uc.diagnose.assert_called_once_with(REQUEST_TR, "gemini-2.5-pro")


def test_post_diagnosis_without_model_passes_none(client, mock_uc):
    resp = client.post("/api/v1/diagnosis", json=BODY)
    assert resp.status_code == 200
    mock_uc.diagnose.assert_called_once_with(REQUEST_TR, None)


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


def test_post_diagnosis_range_over_the_cap_returns_400(client):
    body = {"from": "2026-08-20T02:00:00", "to": "2026-08-20T02:10:01"}
    resp = client.post("/api/v1/diagnosis", json=body)
    assert resp.status_code == 400
    assert "error" in resp.json()


def test_post_diagnosis_mixed_timezone_awareness_returns_400(client):
    # `from` is aware, `to` is naive. Comparing them raises TypeError inside
    # TimeRange, which is not the domain rejection -- so this used to escape
    # as a 500, mislabelling caller-controlled input as a server fault and
    # logging it as one.
    body = {"from": "2026-08-20T02:00:00+00:00", "to": "2026-08-20T02:30:00"}
    resp = client.post("/api/v1/diagnosis", json=body)
    assert resp.status_code == 400, f"body was {resp.text!r}"
    assert "error" in resp.json()


def test_post_diagnosis_mixed_timezone_awareness_reversed_returns_400(client):
    body = {"from": "2026-08-20T02:00:00", "to": "2026-08-20T02:30:00+00:00"}
    resp = client.post("/api/v1/diagnosis", json=body)
    assert resp.status_code == 400, f"body was {resp.text!r}"


def test_openapi_documents_the_json_endpoint_with_the_response_model():
    """Guards the ``response_model`` registration on the JSON endpoint.

    Reverting that endpoint to ``-> dict`` plus a manual
    ``.model_dump(by_alias=True)`` produces byte-identical responses, so
    every behavioural test still passed and nothing guarded the *published
    schema* -- which is what clients and codegen consume. Asserting on the
    generated OpenAPI is the only thing that can catch that revert.
    """
    schema = app.openapi()
    content = schema["paths"]["/api/v1/diagnosis"]["post"]["responses"]["200"]["content"]

    assert content["application/json"]["schema"] == {
        "$ref": "#/components/schemas/DiagnosisResponse"
    }, "the 200 schema must reference DiagnosisResponse, not a bare object"
    assert "DiagnosisResponse" in schema["components"]["schemas"]
