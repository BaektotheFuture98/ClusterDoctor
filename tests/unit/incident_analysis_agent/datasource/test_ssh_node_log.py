from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
import pytest
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import SshNodeLogFetcher

T0=datetime(2026,10,1,tzinfo=UTC)


def ssh_result(raw=b'',err=b''):
    return SimpleNamespace(set_missing_host_key_policy=lambda _:None,connect=lambda *a,**k:None,
        exec_command=lambda *a,**k:(None,BytesIO(raw),BytesIO(err)),close=lambda:None)


def test_ssh_error_never_becomes_log():
    with patch('paramiko.SSHClient',return_value=ssh_result(err=b'grep: file not found')):
        with pytest.raises(RuntimeError,match='file not found'):
            SshNodeLogFetcher('user','secret').fetch('host','/actual/es','prod',T0,T0+timedelta(minutes=1))


def test_ssh_normal_empty_result_is_empty_log():
    with patch('paramiko.SSHClient',return_value=ssh_result()):
        assert SshNodeLogFetcher('user','secret').fetch('host','/actual/es','prod',T0,T0+timedelta(minutes=1))==''


def test_remote_filter_keeps_offset_date_and_stack_context():
    import re
    from cluster_doctor.incident_analysis_agent.model.kst import KST
    start=datetime(2026,10,2,tzinfo=KST)
    line='[2026-10-01T15:00:30.123456Z][WARN ][JvmGcMonitorService] warning'
    stack='    at example.QueryPhase.run(QueryPhase.java:42)'
    def execute(command,**kwargs):
        dates=set(re.findall(r'2026-\d{2}-\d{2}',command))
        kept=line if '2026-10-01' in dates else ''
        if kept and '-A' in command:kept+='\n'+stack
        return None,BytesIO(kept.encode()),BytesIO()
    client=ssh_result();client.exec_command=execute
    with patch('paramiko.SSHClient',return_value=client):
        text=SshNodeLogFetcher('user','secret').fetch('host','/actual/es','prod',start,start+timedelta(minutes=1))
    assert line in text and stack in text
