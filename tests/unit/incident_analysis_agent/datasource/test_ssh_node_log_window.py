import logging
import re
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from cluster_doctor.incident_analysis_agent.datasource.ssh import node_log
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import (
    DEFAULT_HOST_LOG_LINES,
    SshNodeLogFetcher,
    parse_log_time,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST

SEOUL = ZoneInfo("Asia/Seoul")
START = datetime(2026, 11, 6, 8, 30, tzinfo=KST)
END = datetime(2026, 11, 6, 8, 40, tzinfo=KST)


def fake_client(raw="", on_command=None):
    def execute(command, **kwargs):
        if on_command:
            on_command(command)
        return None, BytesIO(raw.encode()), BytesIO()

    return SimpleNamespace(
        set_missing_host_key_policy=lambda _: None,
        connect=lambda *a, **k: None,
        exec_command=execute,
        close=lambda: None,
    )


def run_fetch(raw, tz, commands=None):
    client = fake_client(raw, (commands.append if commands is not None else None))
    with patch("paramiko.SSHClient", return_value=client):
        return SshNodeLogFetcher("u", "p", log_timezone=tz).fetch(
            "host", "/es", "prod", START, END
        )


def test_parse_log_time_without_offset_uses_given_tz():
    assert parse_log_time("2026-11-05T23:30:00,123", UTC) == datetime(
        2026, 11, 6, 8, 30, 0, 123000, tzinfo=KST
    )
    assert parse_log_time("2026-11-06T08:30:00.000", SEOUL) == START
    assert parse_log_time("2026-11-06T08:30:00,000", UTC).utcoffset() == KST.utcoffset(None)


def test_parse_log_time_respects_offset():
    assert parse_log_time("2026-11-05T23:30:00.5Z", SEOUL) == datetime(
        2026, 11, 5, 23, 30, 0, 500000, tzinfo=UTC
    )
    assert parse_log_time("2026-11-06T08:30:00+09:00", UTC) == START


def test_minute_prefixes_for_utc_node():
    prefixes = node_log._minute_prefixes(START, END, UTC)
    assert prefixes[0] == r"\[2026-11-05T23:30"
    assert prefixes[-1] == r"\[2026-11-05T23:39"
    assert len(prefixes) == 10


def test_minute_prefixes_include_minute_of_unaligned_end():
    end = START + timedelta(minutes=2, seconds=30)
    prefixes = node_log._minute_prefixes(START, end, SEOUL)
    assert prefixes == [
        r"\[2026-11-06T08:30",
        r"\[2026-11-06T08:31",
        r"\[2026-11-06T08:32",
    ]


def test_command_uses_minute_prefixes_and_passes_allowlist():
    commands = []
    run_fetch("", UTC, commands)
    [cmd] = commands
    assert r"\[2026-11-05T23:30" in cmd and r"\[2026-11-05T23:39" in cmd
    assert "2026-11-06" not in cmd
    assert cmd.count("grep -aE -A 40") == 2
    assert cmd.endswith("tail -n 2000")
    node_log._assert_allowed(cmd)


def test_utc_lines_with_utc_tz_land_inside_window():
    raw = (
        "[2026-11-05T23:29:59,000][WARN ][o.e.x] before\n"
        "[2026-11-05T23:30:01,000][WARN ][o.e.x] inside\n"
        "    at foo.Bar(Bar.java:1)\n"
        "[2026-11-05T23:40:00,000][WARN ][o.e.x] after\n"
    )
    text = run_fetch(raw, UTC)
    assert "inside" in text and "at foo.Bar" in text
    assert "before" not in text and "after" not in text


def test_utc_lines_with_default_kst_tz_fall_outside_window():
    raw = "[2026-11-05T23:30:01,000][WARN ][o.e.x] inside\n"
    assert run_fetch(raw, SEOUL) == ""


def test_diagnostic_logs_counts_and_times_not_contents(caplog):
    raw = "[2026-11-05T23:30:01,000][WARN ][o.e.x] SECRET-BODY\n"
    with caplog.at_level(logging.INFO):
        run_fetch(raw, UTC)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "[node-ssh]" in text and "서버 1줄, 구간 안 1줄" in text
    assert "SECRET-BODY" not in text


def test_warning_when_server_returned_but_window_empty(caplog):
    raw = "[2026-11-05T23:30:01,000][WARN ][o.e.x] x\n"
    with caplog.at_level(logging.INFO):
        run_fetch(raw, SEOUL)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings and "NODE_LOG_TIMEZONE" in warnings[0].getMessage()


@pytest.mark.skipif(shutil.which("grep") is None, reason="원격과 같은 grep -E로 패턴을 확인한다")
def test_severity_pattern_passes_node_side_departure_and_gc_lines():
    for line in (
        "[2026-11-06T08:30:00,000][INFO ][o.e.c.s.ClusterApplierService] master node changed {previous [], current [{m}]}",
        "[2026-11-06T08:30:00,000][INFO ][o.e.m.j.JvmGcMonitorService] [gc][123] overhead, spent [600ms] collecting",
        "[2026-11-06T08:30:00,000][INFO ][o.e.c.c.Coordinator] removed {{n}{id}}",
        "[2026-11-06T08:30:00,000][INFO ][o.e.c.c.FollowerChecker] x",
    ):
        out = subprocess.run(
            ["grep", "-aE", node_log._SEVERITY_PATTERN],
            input=line, capture_output=True, text=True,
        )
        assert out.returncode == 0, line


def test_severity_pattern_matches_in_python_regex():
    assert re.search(node_log._SEVERITY_PATTERN, "master node changed")
    assert re.search(node_log._SEVERITY_PATTERN, "removed {{n}{id}}")
    assert re.search(node_log._SEVERITY_PATTERN, "[gc][123] overhead, spent")


def test_status_boundary_constant():
    assert DEFAULT_HOST_LOG_LINES == 300


def _status_for(line_count, exc=None):
    from cluster_doctor.incident_analysis_agent.model.resolved_node import ResolvedNode
    from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
    from cluster_doctor.incident_analysis_agent.service.node_investigation.node_investigation import (
        ProblemNodeCandidate,
        investigate_nodes,
    )

    class Fetcher(node_log.NodeLogFetcher):
        def fetch(self, *args, **kwargs):
            if exc:
                raise exc
            return "\n".join(
                f"[2026-11-06T08:30:00,000][WARN ][o.e.x] line {i}"
                for i in range(line_count)
            )

    resolved = ResolvedNode(
        node_id="n", node_name="n", host="10.0.0.1", log_path="/es", cluster_name="prod"
    )
    return investigate_nodes(
        [ProblemNodeCandidate(node_id="n", reason="r", evidence_refs=())],
        TimeRange(START, END),
        resolver=SimpleNamespace(resolve=lambda _: resolved),
        fetcher=Fetcher(),
        call_llm=lambda *a, **k: "[]",
        new_evidence_id=lambda: "E-1",
    )


def test_status_ok_below_limit_and_limited_at_limit():
    assert _status_for(DEFAULT_HOST_LOG_LINES - 1).source_statuses[-1].status == "ok"
    assert _status_for(DEFAULT_HOST_LOG_LINES).source_statuses[-1].status == "limited"


def test_failure_reason_uses_type_name_for_non_code_exceptions():
    status = _status_for(0, exc=OSError("secret host detail")).source_statuses[-1]
    assert status.status == "failed"
    assert "secret" not in str(status)
