import logging
import posixpath
from pathlib import Path

import paramiko
import pytest

from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.sftp.sftp_uploader import (
    SftpTarget,
    SftpUploadError,
    SftpUploader,
)


class FakeSftp:
    def __init__(self, files=None, dirs=None):
        self.files = dict(files or {})
        self.dirs = {"/"} | set(dirs or ())
        self.calls = []
        self.closed = False
        self.channel_timeout = None

    def stat(self, path):
        if path in self.files or path in self.dirs:
            return object()
        raise FileNotFoundError(path)

    def mkdir(self, path):
        parent = posixpath.dirname(path) or "/"
        if parent not in self.dirs:
            raise FileNotFoundError(parent)
        self.dirs.add(path)
        self.calls.append(("mkdir", path))

    def put(self, local, remote):
        self.files[remote] = Path(local).read_bytes()
        self.calls.append(("put", remote))

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)
        self.calls.append(("rename", old, new))

    def remove(self, path):
        self.files.pop(path, None)
        self.calls.append(("remove", path))

    def get_channel(self):
        sftp = self

        class _Channel:
            def settimeout(self, seconds):
                sftp.channel_timeout = seconds

        return _Channel()

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, sftp, fail_connect=0):
        self.sftp = sftp
        self.fail_connect = fail_connect
        self.connect_kwargs = None
        self.loaded_host_keys = None
        self.policy = None
        self.closed = False

    def load_host_keys(self, path):
        self.loaded_host_keys = path

    def set_missing_host_key_policy(self, policy):
        self.policy = policy

    def connect(self, **kwargs):
        self.connect_kwargs = kwargs
        if self.fail_connect > 0:
            self.fail_connect -= 1
            raise OSError("connection refused")

    def open_sftp(self):
        return self.sftp

    def close(self):
        self.closed = True


def _target(**overrides):
    values = dict(host="reports.example", port=2222, user="svc", remote_dir="/srv/reports", password="pw")
    values.update(overrides)
    return SftpTarget(**values)


def _uploader(client, **kwargs):
    kwargs.setdefault("sleep", lambda seconds: None)
    return SftpUploader(_target(**kwargs.pop("target", {})), client_factory=lambda: client, **kwargs)


@pytest.fixture
def report(tmp_path):
    path = tmp_path / "report-20261006-090000.html"
    path.write_text("<html>리포트</html>", encoding="utf-8")
    return path


def test_upload_writes_the_file_through_a_partial_name_and_renames_it(report):
    sftp = FakeSftp(dirs={"/srv", "/srv/reports"})

    remote = _uploader(FakeClient(sftp)).upload(report)

    assert remote == "/srv/reports/report-20261006-090000.html"
    assert sftp.files[remote] == report.read_bytes()
    assert sftp.calls == [
        ("put", remote + ".part"),
        ("rename", remote + ".part", remote),
    ]
    assert not any(path.endswith(".part") for path in sftp.files)


def test_missing_remote_directories_are_created_parent_first(report):
    sftp = FakeSftp()

    _uploader(FakeClient(sftp)).upload(report)

    assert [call for call in sftp.calls if call[0] == "mkdir"] == [
        ("mkdir", "/srv"),
        ("mkdir", "/srv/reports"),
    ]


def test_existing_remote_name_gets_a_numeric_suffix(report):
    existing = "/srv/reports/report-20261006-090000.html"
    sftp = FakeSftp(files={existing: b"old"}, dirs={"/srv", "/srv/reports"})

    remote = _uploader(FakeClient(sftp)).upload(report)

    assert remote == "/srv/reports/report-20261006-090000-2.html"
    assert sftp.files[existing] == b"old"


def test_password_and_key_file_are_passed_without_agent_or_default_keys(report):
    client = FakeClient(FakeSftp(dirs={"/srv", "/srv/reports"}))

    _uploader(client, target=dict(key_file="/keys/id_ed25519", timeout_seconds=7.0)).upload(report)

    kwargs = client.connect_kwargs
    assert kwargs["hostname"] == "reports.example"
    assert kwargs["port"] == 2222
    assert kwargs["username"] == "svc"
    assert kwargs["password"] == "pw"
    assert kwargs["key_filename"] == "/keys/id_ed25519"
    assert kwargs["timeout"] == 7.0
    assert kwargs["allow_agent"] is False
    assert kwargs["look_for_keys"] is False


def test_empty_password_and_key_are_sent_as_none(report):
    client = FakeClient(FakeSftp(dirs={"/srv", "/srv/reports"}))

    _uploader(client, target=dict(password="", key_file="/keys/id")).upload(report)

    assert client.connect_kwargs["password"] is None


def test_known_hosts_file_enables_strict_host_key_checking(report):
    client = FakeClient(FakeSftp(dirs={"/srv", "/srv/reports"}))

    _uploader(client, target=dict(known_hosts="/etc/ssh/known_hosts")).upload(report)

    assert client.loaded_host_keys == "/etc/ssh/known_hosts"
    assert isinstance(client.policy, paramiko.RejectPolicy)


def test_without_known_hosts_unknown_host_keys_are_accepted(report):
    client = FakeClient(FakeSftp(dirs={"/srv", "/srv/reports"}))

    _uploader(client).upload(report)

    assert client.loaded_host_keys is None
    assert isinstance(client.policy, paramiko.AutoAddPolicy)


def test_transient_connect_failure_is_retried(report):
    client = FakeClient(FakeSftp(dirs={"/srv", "/srv/reports"}), fail_connect=2)
    delays = []

    remote = _uploader(client, attempts=3, retry_delay_seconds=1.5, sleep=delays.append).upload(report)

    assert remote.endswith("report-20261006-090000.html")
    assert delays == [1.5, 1.5]


def test_all_attempts_failing_raises_one_upload_error(report, caplog):
    client = FakeClient(FakeSftp(), fail_connect=99)
    caplog.set_level(logging.WARNING)

    with pytest.raises(SftpUploadError) as error:
        _uploader(client, attempts=3).upload(report)

    assert "connection refused" in str(error.value)
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 3


def test_client_and_sftp_are_closed_even_when_the_upload_fails(report):
    sftp = FakeSftp(dirs={"/srv", "/srv/reports"})
    sftp.put = lambda local, remote: (_ for _ in ()).throw(OSError("disk full"))
    client = FakeClient(sftp)

    with pytest.raises(SftpUploadError):
        _uploader(client, attempts=1).upload(report)

    assert sftp.closed
    assert client.closed


def test_password_never_reaches_the_logs_or_the_error(report, caplog):
    client = FakeClient(FakeSftp(), fail_connect=99)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(SftpUploadError) as error:
        _uploader(client, target=dict(password="s3cret-pw"), attempts=2).upload(report)

    assert "s3cret-pw" not in caplog.text
    assert "s3cret-pw" not in str(error.value)


def test_authentication_failure_is_not_retried(report):
    client = FakeClient(FakeSftp())
    calls = []

    def refuse(**kwargs):
        calls.append(kwargs)
        raise paramiko.AuthenticationException("Authentication failed.")

    client.connect = refuse
    delays = []

    with pytest.raises(SftpUploadError) as error:
        _uploader(client, attempts=3, sleep=delays.append).upload(report)

    assert len(calls) == 1
    assert delays == []
    assert "Authentication failed." in str(error.value)


def test_permission_error_on_the_remote_directory_is_not_retried(report):
    sftp = FakeSftp()
    attempts = []

    def denied(path):
        attempts.append(path)
        raise PermissionError(path)

    sftp.mkdir = denied

    with pytest.raises(SftpUploadError):
        _uploader(FakeClient(sftp), attempts=3).upload(report)

    assert attempts == ["/srv"]


def test_transfer_timeout_follows_the_configured_timeout(report):
    sftp = FakeSftp(dirs={"/srv", "/srv/reports"})

    _uploader(FakeClient(sftp), target=dict(timeout_seconds=7.0)).upload(report)

    assert sftp.channel_timeout == 7.0


def test_partial_file_is_removed_when_the_rename_fails(report):
    sftp = FakeSftp(dirs={"/srv", "/srv/reports"})

    def broken_rename(old, new):
        raise OSError("rename failed")

    sftp.rename = broken_rename

    with pytest.raises(SftpUploadError):
        _uploader(FakeClient(sftp), attempts=1).upload(report)

    assert ("remove", "/srv/reports/report-20261006-090000.html.part") in sftp.calls
    assert not any(path.endswith(".part") for path in sftp.files)


def test_stalled_transfer_skips_the_remote_cleanup(report):
    sftp = FakeSftp(dirs={"/srv", "/srv/reports"})

    def stalled(local, remote):
        raise TimeoutError("timed out")

    sftp.put = stalled

    with pytest.raises(SftpUploadError):
        _uploader(FakeClient(sftp), attempts=1).upload(report)

    assert not any(call[0] == "remove" for call in sftp.calls)


def test_trailing_slash_on_the_remote_directory_is_ignored(report):
    sftp = FakeSftp()

    remote = _uploader(FakeClient(sftp), target={"remote_dir": "/srv/reports/"}).upload(report)

    assert remote == "/srv/reports/report-20261006-090000.html"
    assert [call for call in sftp.calls if call[0] == "mkdir"] == [
        ("mkdir", "/srv"),
        ("mkdir", "/srv/reports"),
    ]


def test_password_is_not_part_of_the_target_repr():
    assert "s3cret-pw" not in repr(_target(password="s3cret-pw"))


def test_at_least_one_attempt_is_required():
    with pytest.raises(ValueError):
        SftpUploader(_target(), attempts=0)
