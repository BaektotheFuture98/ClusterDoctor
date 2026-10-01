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
