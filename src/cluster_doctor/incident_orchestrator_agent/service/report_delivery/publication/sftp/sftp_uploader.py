"""리포트 파일을 SFTP로 다른 서버에 올린다.

동기 코드다. 이벤트 루프를 막지 않도록 호출부가 스레드에서 돌린다.
비밀번호는 접속 인자로만 넘기고 로그·예외 문구에 싣지 않는다.
"""

from __future__ import annotations

import logging
import posixpath
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import paramiko

_logger = logging.getLogger(__name__)


class SftpUploadError(RuntimeError):
    """모든 시도가 실패했다. 원인 예외의 종류와 문구만 담는다."""


@dataclass(frozen=True)
class SftpTarget:
    host: str
    port: int
    user: str
    remote_dir: str
    password: str = field(default="", repr=False)
    key_file: str = ""
    known_hosts: str = ""
    timeout_seconds: float = 10.0


# 다시 시도해도 결과가 같거나(권한·호스트 키) 서버의 로그인 실패 잠금을 건드릴 수 있는(인증) 오류.
_PERMANENT_ERRORS = (
    paramiko.AuthenticationException,
    paramiko.BadHostKeyException,
    PermissionError,
)

# 채널이 멈췄거나 끊긴 오류. 같은 채널로 원격 정리를 해도 다시 시간 제한까지 기다릴 뿐이다.
_STALLED_ERRORS = (TimeoutError, EOFError)


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _remove_quietly(sftp, path: str) -> None:
    """실패한 업로드가 남긴 임시 파일을 지워 본다. 지우지 못해도 원래 오류를 가리지 않는다."""
    try:
        sftp.remove(path)
    except Exception:  # noqa: BLE001
        pass


class SftpUploader:
    def __init__(
        self,
        target: SftpTarget,
        *,
        client_factory: Callable[[], paramiko.SSHClient] = paramiko.SSHClient,
        attempts: int = 2,
        retry_delay_seconds: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        self._target = target
        self._client_factory = client_factory
        self._attempts = attempts
        self._retry_delay_seconds = retry_delay_seconds
        self._sleep = sleep

    def upload(self, local_path: Path) -> str:
        """``local_path``를 원격 디렉터리에 올리고 원격 경로를 돌려준다."""
        last: BaseException | None = None
        for attempt in range(1, self._attempts + 1):
            try:
                return self._upload_once(local_path)
            except Exception as exc:  # noqa: BLE001
                last = exc
                _logger.warning(
                    "SFTP 업로드 실패 (%d/%d): %s", attempt, self._attempts, _describe(exc)
                )
                if isinstance(exc, _PERMANENT_ERRORS):
                    break
                if attempt < self._attempts:
                    self._sleep(self._retry_delay_seconds)
        raise SftpUploadError(_describe(last)) from None

    def _upload_once(self, local_path: Path) -> str:
        target = self._target
        client = self._client_factory()
        try:
            if target.known_hosts:
                client.load_host_keys(target.known_hosts)
                client.set_missing_host_key_policy(paramiko.RejectPolicy())
            else:
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=target.host,
                port=target.port,
                username=target.user,
                password=target.password or None,
                key_filename=target.key_file or None,
                timeout=target.timeout_seconds,
                banner_timeout=target.timeout_seconds,
                auth_timeout=target.timeout_seconds,
                allow_agent=False,
                look_for_keys=False,
            )
            sftp = client.open_sftp()
            try:
                # 전송 중 서버가 멈춰도 무기한 기다리지 않는다.
                sftp.get_channel().settimeout(target.timeout_seconds)
                remote_dir = target.remote_dir.rstrip("/") or "/"
                _ensure_directory(sftp, remote_dir)
                name = _unique_name(sftp, remote_dir, local_path.name)
                final = posixpath.join(remote_dir, name)
                partial = final + ".part"
                try:
                    sftp.put(str(local_path), partial)
                    sftp.rename(partial, final)
                except Exception as exc:
                    # 응답이 끊긴 채널에서 정리를 시도하면 시간 제한만큼 더 기다린다.
                    if not isinstance(exc, _STALLED_ERRORS):
                        _remove_quietly(sftp, partial)
                    raise
                return final
            finally:
                sftp.close()
        finally:
            client.close()


def _exists(sftp, path: str) -> bool:
    try:
        sftp.stat(path)
    except FileNotFoundError:
        return False
    return True


def _ensure_directory(sftp, path: str) -> None:
    missing: list[str] = []
    current = path
    while current not in ("", "/") and not _exists(sftp, current):
        missing.append(current)
        current = posixpath.dirname(current)
    for directory in reversed(missing):
        sftp.mkdir(directory)


def _unique_name(sftp, remote_dir: str, name: str) -> str:
    """같은 이름이 이미 있으면 덮어쓰지 않고 ``-2``, ``-3``을 붙인다."""
    stem, extension = posixpath.splitext(name)
    candidate = name
    suffix = 2
    while _exists(sftp, posixpath.join(remote_dir, candidate)):
        candidate = f"{stem}-{suffix}{extension}"
        suffix += 1
    return candidate
