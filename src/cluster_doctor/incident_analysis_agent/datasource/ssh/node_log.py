"""데이터 노드 로그 datasource: SSH 조회 + 선별 설정 + 레코드 변환.

노드 로그를 가져오는 경로는 둘이다. 마스터 노드 로그는 ClickHouse에 적재되어
``datasource/clickhouse/client.py``의 ``fetch_node_logs``로 오고, 데이터 노드
로그는 적재되는 곳이 없어 호스트에 직접 붙어야 한다. 문제 노드 후보가
나왔을 때만 접속한다(``service/node_investigation``).

원문이 파일 그대로 오므로 레코드 변환이 파싱을 포함한다. 조회 결과 중 시각을 뽑지 못한 줄
(스택 트레이스 연속 행)은 **버리지 않는다.** 직전 줄의 시각을 물려준다 —
예외 본문이 사라지면 그 예외가 무엇이었는지 알 수 없고, 값이 없다는 것과
줄이 없다는 것은 다르다.
"""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, tzinfo

import paramiko

from cluster_doctor.incident_analysis_agent.model.evidence import (
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    RawRecord,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.spec import (
    AnalysisSpec,
)

_logger = logging.getLogger(__name__)

# 이 경로가 한 번에 돌려주는 기본 줄 수.
#
# ClickHouse 조회의 ``DEFAULT_NODE_LOG_LIMIT``과 값이 같지만 뜻이 다르다.
# 저쪽은 ClickHouse 질의의 행 수 상한이라 ``ORDER BY timestamp``로 **가장
# 이른** 행이 남고, 이쪽은 서버에서 ``tail``로 자르므로 **가장 늦은** 줄이
# 남는다. 같은 숫자라고 한 상수로 묶으면 그 반대 편향이 가려진다.
DEFAULT_HOST_LOG_LINES = 300

SSH_CONNECT_TIMEOUT_SECONDS = 10
SSH_COMMAND_TIMEOUT_SECONDS = 30

# 서버에서 1차로 걸러낼 severity 키워드 패턴.
# 마스터/데이터 노드 모두에서 인시던트 원인과 직결되는 줄만 남긴다.
#
# ES 로그 형식: [2026-09-08T02:04:33,123][WARN ][클래스명] ...
# 레벨은 타임스탬프 뒤에 오며 WARN은 5자 패딩으로 "WARN "이다.
# "^\[(WARN|ERROR)\]"처럼 줄 시작에 앵커를 쓰면 타임스탬프로 시작하는
# ES 로그 형식과 맞지 않아 아무것도 잡히지 않는다.
_SEVERITY_PATTERN = (
    r"\[WARN |\[ERROR"  # ES 레벨 필드: [WARN ] / [ERROR]
    r"|GC overhead"
    r"|heap"
    r"|thread pool"
    r"|reject"
    r"|shard"
    r"|master node changed|master not discovered|node-left|removed \{"
    r"|disconnect|FollowerChecker|LeaderChecker"
    r"|\[gc\]|overhead, spent"
)

# 원격에서 돌릴 수 있는 프로그램. **allowlist다.** 이 경로에 LLM이 만든
# 문자열이 직접 닿지는 않지만(키워드는 정규식으로 씻고, 경로는 ES 조회에서
# 온다), 원격 셸 명령에서 "닿지 않는다"는 근거는 코드가 보장해야 한다 —
# ES 응답도, 노드 설정도 이 프로세스가 통제하지 않는 입력이다.
_ALLOWED_COMMANDS = ("grep", "tail")

# 파일 경로에 허용하는 글자. 셸 메타문자를 통째로 막는다.
_SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9_./\-]+$")

# ES 로그 한 줄의 머리: [시각][레벨][로거]. 로거 이름은 오른쪽이 공백으로
# 채워져 있다(``[o.e.c.c.C          ]``).
ES_LOG_LINE_RE = re.compile(
    r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[,.]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\]"
    r"\[([A-Z ]+)\]"
    r"\[([^\]]+)\]"
)

SPEC = AnalysisSpec(
    source=EvidenceSource.NODE_LOG,
    label="node log (문제 노드의 ES 로그)",
    what_matters=(
        "- OutOfMemory, GC overhead, long GC pause.\n"
        "- thread pool rejection, queue full.\n"
        "- circuit breaker 발동.\n"
        "- shard failure, recovery 실패, corrupt.\n"
        "- 마스터와의 연결 끊김, transport 예외.\n"
        "- 마스터 변경·마스터 미발견·클러스터에서 제거.\n"
        "- 스택 트레이스의 첫 줄과 예외 이름."
    ),
    what_is_noise=(
        "- 정상 시작/종료 기록, 설정 로딩.\n"
        "- 같은 예외가 연속으로 수십 줄 반복되는 경우 (대표 한 줄만 남긴다).\n"
        "- 스택 트레이스의 중간 프레임."
    ),
)


def parse_log_time(text: str, tz: tzinfo) -> datetime:
    """로그 줄의 시각 문자열을 KST-aware datetime으로.

    오프셋이 붙어 있으면 그 값을 따르고, 없으면 노드가 로그를 남기는 시간대
    ``tz``의 벽시계로 본다. 쉼표 밀리초(``,123``)와 점 밀리초를 모두 받는다.
    """
    parsed = datetime.fromisoformat(text.replace(",", "."))
    if parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(KST)


def _minute_prefixes(start_dt: datetime, end_dt: datetime, tz: tzinfo) -> list[str]:
    """구간을 노드 시간대의 분 단위 줄 머리 정규식 목록으로.

    시작이 속한 분부터 끝(배타)이 속한 분까지 한 분씩 센다.
    """
    cursor = start_dt.astimezone(tz).replace(second=0, microsecond=0)
    last = end_dt.astimezone(tz)
    prefixes: list[str] = []
    while cursor < last:
        prefixes.append("\\[" + cursor.strftime("%Y-%m-%dT%H:%M"))
        cursor += timedelta(minutes=1)
    return prefixes


def to_records(
    text: str,
    *,
    fallback_time: datetime,
    provenance: EvidenceProvenance | None = None,
    log_timezone: tzinfo = KST,
) -> list[RawRecord]:
    """SSH로 읽은 로그 원문을 선별 레코드로.

    ``fallback_time``은 첫 줄부터 시각을 뽑지 못했을 때 쓸 값이다. 보통 분석
    구간의 시작을 준다 — 시각이 없는 줄을 버리면 예외 본문이 통째로 사라지고,
    임의의 현재 시각을 붙이면 타임라인이 거짓이 된다.
    """
    records: list[RawRecord] = []
    current_time = fallback_time
    current_level: str | None = None
    has_timestamp = False

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        match = ES_LOG_LINE_RE.match(line)
        time_origin = "inherited" if has_timestamp else "fallback"
        if match:
            current_level = match.group(2).strip() or None
            try:
                current_time = parse_log_time(match.group(1), log_timezone)
                has_timestamp = True
                time_origin = "parsed"
            except ValueError:
                pass
        records.append(
            RawRecord(
                record_id=len(records) + 1,
                event_time=current_time,
                line=line,
                severity=current_level,
                provenance=provenance,
                time_origin=time_origin,
            )
        )
    return records


def ssh_provenance(
    resolved, start: datetime, end: datetime, *, role: str | None = None
) -> EvidenceProvenance:
    """이번 fetch에 사용한 확정 destination을 스냅샷으로 남긴다."""
    return EvidenceProvenance(
        method="ssh",
        collected_at=datetime.now(KST),
        query_from=start,
        query_to=end,
        host=resolved.host,
        file_path=f"{resolved.log_path}/{resolved.cluster_name}.log",
        role=role,
        excerpt=True,
    )


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


class NodeLogFetcher(ABC):
    """노드 호스트의 ES 로그 파일을 읽는다."""

    # 오프셋 없는 로그 시각을 해석할 시간대. 구현이 다르게 읽으면 덮어쓴다.
    log_timezone: tzinfo = KST

    @abstractmethod
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
        """``start_dt ~ end_dt`` 구간의 로그 줄을 원문 그대로 돌려준다.

        **예외를 삼키지 않는다.** 접속 실패와 "그 시각에 로그가 없다"는 다르고,
        둘을 같은 빈 문자열로 뭉개면 리포트에서 "못 봤다"와 "봤는데 없다"가
        구별되지 않는다. 호출하는 tool이 예외를 받아 gap으로 남긴다.
        """
        ...


class SshNodeLogFetcher(NodeLogFetcher):
    """SSH로 ES 노드에 접속해 메인 로그 파일을 읽는다."""

    def __init__(
        self,
        ssh_user: str,
        ssh_password: str,
        ssh_port: int = 22,
        log_timezone: tzinfo = KST,
    ) -> None:
        self._user = ssh_user
        self._password = ssh_password
        self._port = ssh_port
        self.log_timezone = log_timezone

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

        # 구간의 분 접두어로 먼저 거르고 심각도로 다시 거른다. 시간 필터가
        # 먼저여야 긴 기간의 결과가 tail에 밀려 구간이 잘리지 않는다.
        # 예외 스택 같은 이어지는 줄은 -A 40으로 두 필터를 모두 통과시킨다.
        # 이어지는 줄의 시각은 직전 레코드에서 가져오며 임의로 만들지 않는다.
        pattern = "|".join(_minute_prefixes(start_dt, end_dt, self.log_timezone))
        cmd = _assert_allowed(
            f"grep -aE -A 40 '{pattern}' '{log_file}'"
            f" | grep -aE -A 40 '{_SEVERITY_PATTERN}'"
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
            _, stdout, stderr = client.exec_command(
                cmd, timeout=SSH_COMMAND_TIMEOUT_SECONDS
            )
            raw = stdout.read().decode(errors="replace")
            err = stderr.read().decode(errors="replace").strip()
        finally:
            client.close()

        if err:
            raise RuntimeError(f"SSH 로그 조회 실패: {err}")

        start_kst = start_dt.astimezone(KST)
        end_kst = end_dt.astimezone(KST)

        lines = raw.split("\n")
        filtered: list[str] = []
        in_window = False
        server_lines = 0
        first_ts: datetime | None = None
        last_ts: datetime | None = None
        for line in lines:
            if line == "--":
                continue
            if line.strip():
                server_lines += 1
            m = ES_LOG_LINE_RE.match(line)
            if m:
                try:
                    ts = parse_log_time(m.group(1), self.log_timezone)
                    in_window = start_kst <= ts < end_kst
                    first_ts = ts if first_ts is None else min(first_ts, ts)
                    last_ts = ts if last_ts is None else max(last_ts, ts)
                except ValueError:
                    pass
            if in_window:
                filtered.append(line)

        in_window_count = sum(1 for ln in filtered if ln.strip())
        _logger.info(
            "[node-ssh] %s 결과: 서버 %d줄, 구간 안 %d줄, 서버 결과 시각 %s~%s (tz=%s)",
            ip,
            server_lines,
            in_window_count,
            first_ts.isoformat() if first_ts else "-",
            last_ts.isoformat() if last_ts else "-",
            getattr(self.log_timezone, "key", None) or str(self.log_timezone),
        )
        if server_lines and not in_window_count:
            _logger.warning(
                "[node-ssh] %s 서버는 줄을 돌려줬지만 구간 안은 0줄이다. "
                "시간대 설정(NODE_LOG_TIMEZONE)을 확인하라",
                ip,
            )

        if keyword:
            safe = re.sub(r"[^\w\s.:-]", "", keyword)
            if safe:
                filtered = [ln for ln in filtered if safe.lower() in ln.lower()]

        return "\n".join(filtered[-max_lines:])
