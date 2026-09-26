from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

import paramiko

from cluster_doctor.application.ports.node_log_fetcher import (
    DEFAULT_HOST_LOG_LINES,
    NodeLogFetcher,
)
from cluster_doctor.domain.incident.guardrails import (
    SSH_COMMAND_TIMEOUT_SECONDS,
    SSH_CONNECT_TIMEOUT_SECONDS,
)

_logger = logging.getLogger(__name__)
_KST = timezone(timedelta(hours=9))

# 서버에서 1차로 걸러낼 severity 키워드 패턴.
# 마스터/데이터 노드 모두에서 인시던트 원인과 직결되는 줄만 남긴다.
#
# ES 로그 형식: [2026-09-08T02:04:33,123][WARN ][클래스명] ...
# 레벨은 타임스탬프 뒤에 오며 WARN은 5자 패딩으로 "WARN "이다.
# "^\[(WARN|ERROR)\]"처럼 줄 시작에 앵커를 쓰면 타임스탬프로 시작하는
# ES 로그 형식과 맞지 않아 아무것도 잡히지 않는다.
_SEVERITY_PATTERN = (
    r"\[WARN |\[ERROR"   # ES 레벨 필드: [WARN ] / [ERROR]
    r"|GC overhead"
    r"|heap"
    r"|thread pool"
    r"|reject"
    r"|shard"
)

# 원격에서 돌릴 수 있는 프로그램. **allowlist다.** 이 경로에 LLM이 만든
# 문자열이 직접 닿지는 않지만(키워드는 정규식으로 씻고, 경로는 ES 조회에서
# 온다), 원격 셸 명령에서 "닿지 않는다"는 근거는 코드가 보장해야 한다 —
# ES 응답도, 노드 설정도 이 프로세스가 통제하지 않는 입력이다.
_ALLOWED_COMMANDS = ("grep", "tail")

# 파일 경로에 허용하는 글자. 셸 메타문자를 통째로 막는다.
_SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9_./\-]+$")


class UnsafeSshCommandError(RuntimeError):
    """조립된 명령이 allowlist를 벗어났다.

    예외를 올린다. 여기서 조용히 빈 문자열을 돌려주면 "그 시각에 로그가
    없었다"와 구별되지 않고, 막아야 할 일이 막혔다는 사실이 사라진다.
    """


def _assert_safe_path(path: str, label: str) -> str:
    if not _SAFE_PATH_RE.match(path or ""):
        raise UnsafeSshCommandError(f"{label}에 허용되지 않는 문자가 있다")
    return path


def _split_pipeline(command: str) -> list[str]:
    """따옴표 **밖의** ``|``로만 나눈다.

    단순히 ``command.split("|")``로 하면 안 된다. severity 정규식 자체가
    ``\\[WARN |\\[ERROR|GC overhead`` 처럼 교대(|)를 쓰므로, 따옴표를 무시하고
    나누면 정규식 조각이 "프로그램 이름"으로 검사되어 정상 명령이 전부 거절된다.
    """
    segments: list[str] = []
    current: list[str] = []
    quote: str | None = None
    for char in command:
        if quote:
            if char == quote:
                quote = None
            current.append(char)
        elif char in ("'", '"'):
            quote = char
            current.append(char)
        elif char == "|":
            segments.append("".join(current))
            current = []
        else:
            current.append(char)
    segments.append("".join(current))
    return segments


def _assert_allowed(command: str) -> str:
    """파이프라인의 각 단계가 allowlist의 프로그램으로 시작하는가.

    셸을 통째로 막지는 못한다(원격은 여전히 셸이다). 막는 것은 **이 코드가
    조립한 명령이 의도한 프로그램만 부르는가**이고, 경로 검사와 함께 쓰면
    바깥 입력이 명령을 바꿀 길이 없다.
    """
    for segment in _split_pipeline(command):
        program = segment.strip().split(" ", 1)[0]
        if program not in _ALLOWED_COMMANDS:
            raise UnsafeSshCommandError(f"허용되지 않는 명령: {program}")
    return command


class SshNodeLogFetcher(NodeLogFetcher):
    """SSH로 ES 노드에 접속해 메인 로그 파일을 읽는 어댑터.

    이름에 기술을 밝히는 것은 다른 어댑터와 같은 규칙이다
    (``ClickHouseLogAdapter``, ``ElasticsearchClusterAdapter``,
    ``HtmlFileNotifier``).
    """

    def __init__(self, ssh_user: str, ssh_password: str, ssh_port: int = 22) -> None:
        self._user = ssh_user
        self._password = ssh_password
        self._port = ssh_port

    def fetch(
        self,
        ip: str,
        log_path: str,
        cluster_name: str,
        start_dt: datetime,
        end_dt: datetime,
        keyword: str = "",
        max_lines: int = DEFAULT_HOST_LOG_LINES,
    ) -> str:
        """메인 ES 로그에서 start_dt ~ end_dt 구간의 라인을 반환한다.

        로그 파일 경로: {log_path}/{cluster_name}.log
        서버에서 severity 키워드 grep으로 1차 압축한 뒤
        Python에서 정확한 시간 범위로 2차 필터링한다.
        start_dt / end_dt는 timezone-aware여야 한다.
        """
        log_file = "{}/{}.log".format(
            _assert_safe_path(log_path, "log_path"),
            _assert_safe_path(cluster_name, "cluster_name"),
        )

        # 서버사이드 grep: severity 키워드 + 날짜 prefix로 볼륨을 크게 줄인다.
        # 구간이 자정을 넘으면 두 날짜를 모두 포함한다.
        start_date = start_dt.astimezone(_KST).strftime("%Y-%m-%d")
        end_date = end_dt.astimezone(_KST).strftime("%Y-%m-%d")
        if start_date == end_date:
            date_filter = f"grep '\\[{start_date}'"
        else:
            date_filter = f"grep -E '\\[{start_date}|\\[{end_date}'"
        cmd = _assert_allowed(
            f"grep -aE '{_SEVERITY_PATTERN}' '{log_file}'"
            f" | {date_filter}"
            f" | tail -n 2000"
        )

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                ip,
                port=self._port,
                username=self._user,
                password=self._password,
                timeout=SSH_CONNECT_TIMEOUT_SECONDS,
            )
            _, stdout, stderr = client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT_SECONDS)
            raw = stdout.read().decode(errors="replace")
            err = stderr.read().decode(errors="replace").strip()
        finally:
            client.close()

        if not raw and err:
            return f"로그 조회 실패: {err}"

        start_kst = start_dt.astimezone(_KST)
        end_kst = end_dt.astimezone(_KST)

        lines = raw.split("\n")
        filtered: list[str] = []
        in_window = False
        for line in lines:
            m = re.match(r'^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', line)
            if m:
                try:
                    ts = datetime.fromisoformat(m.group(1)).replace(tzinfo=_KST)
                    in_window = start_kst <= ts <= end_kst
                except ValueError:
                    pass
            if in_window:
                filtered.append(line)

        if keyword:
            safe = re.sub(r"[^\w\s.:-]", "", keyword)
            if safe:
                filtered = [ln for ln in filtered if safe.lower() in ln.lower()]

        return "\n".join(filtered[-max_lines:])
