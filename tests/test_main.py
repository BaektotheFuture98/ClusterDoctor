"""Tests for the ASGI app wiring in ``cluster_doctor.main``.

The security-relevant case: ``pydantic.ValidationError`` is a subclass of
``ValueError``, and settings used to be resolved lazily inside the request
path. A global ``ValueError`` handler therefore serialized the validation
message -- which embeds the parsed ``.env`` contents -- into the response
body of an unauthenticated endpoint.
"""

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from cluster_doctor.config import dependencies
from cluster_doctor.config import settings as settings_module
from cluster_doctor.main import app

SECRET = "hunter2"
BODY = {"from": "2026-08-20T02:09:00", "to": "2026-08-20T02:10:00"}


class _SettingsProbe(BaseModel):
    """Stand-in with the same failure mode as ``Settings``: a required field
    is missing while a secret-bearing field is present in the input."""

    gemini_api_key: str
    clickhouse_password: str = ""


def _raise_settings_validation_error(*_args, **_kwargs):
    _SettingsProbe(clickhouse_password=SECRET)
    raise AssertionError("probe was expected to raise ValidationError")


def _clear_settings_caches() -> None:
    settings_module.get_settings.cache_clear()
    dependencies._get_clickhouse_client.cache_clear()
    dependencies._get_log_repository.cache_clear()
    dependencies._get_llm_analyzer.cache_clear()


@pytest.fixture
def broken_settings(monkeypatch):
    _clear_settings_caches()
    monkeypatch.setattr(settings_module, "Settings", _raise_settings_validation_error)
    yield
    _clear_settings_caches()


def test_settings_validation_error_does_not_leak_secrets(broken_settings):
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.post("/api/v1/diagnosis", json=BODY)

    assert resp.status_code == 500, (
        "a config failure is a server fault, not a 400 client error; "
        f"body was {resp.text!r}"
    )
    assert SECRET not in resp.text
    assert "clickhouse_password" not in resp.text
    assert "validation error" not in resp.text.lower()


def test_settings_validation_error_does_not_leak_secrets_on_text_endpoint(broken_settings):
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.post("/api/v1/diagnosis/text", json=BODY)

    assert resp.status_code == 500
    assert SECRET not in resp.text
    assert "clickhouse_password" not in resp.text


def test_lifespan_resolves_settings_once_at_startup(monkeypatch):
    import cluster_doctor.main as main

    calls = []

    def fake_get_settings():
        calls.append(1)
        return object()

    monkeypatch.setattr(main, "get_settings", fake_get_settings)

    with TestClient(app):
        pass

    assert calls == [1], "startup must resolve settings exactly once"


def test_startup_fails_loudly_when_settings_are_invalid(broken_settings):
    with pytest.raises(Exception) as excinfo:
        with TestClient(app):
            pass

    assert "gemini_api_key" in str(excinfo.value)
