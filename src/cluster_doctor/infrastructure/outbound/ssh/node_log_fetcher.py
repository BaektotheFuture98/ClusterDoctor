from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

import paramiko

from cluster_doctor.application.port.outbound.node_log_fetcher import (
    DEFAULT_HOST_LOG_LINES,
    NodeLogFetcher,
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
        log_file = f"{log_path}/{cluster_name}.log"

        # 서버사이드 grep: severity 키워드 + 날짜 prefix로 볼륨을 크게 줄인다.
        # 구간이 자정을 넘으면 두 날짜를 모두 포함한다.
        start_date = start_dt.astimezone(_KST).strftime("%Y-%m-%d")
        end_date = end_dt.astimezone(_KST).strftime("%Y-%m-%d")
        if start_date == end_date:
            date_filter = f"grep '\\[{start_date}'"
        else:
            date_filter = f"grep -E '\\[{start_date}|\\[{end_date}'"
        cmd = (
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
                timeout=10,
            )
            _, stdout, stderr = client.exec_command(cmd, timeout=30)
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
