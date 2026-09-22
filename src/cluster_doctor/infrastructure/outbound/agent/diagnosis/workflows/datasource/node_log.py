"""데이터 노드 로그 datasource.

이 소스만 출처가 다르다. ClickHouse에는 마스터 노드 로그만 적재되므로 데이터
노드 로그는 호스트에 SSH로 붙어 읽는다 — 그래서 조건부로만 돈다. 문제 노드
후보가 나왔을 때에만 접속한다(``node_investigation``).

원문이 파일 그대로 오므로 레코드 변환이 파싱을 포함한다. 시각을 뽑지 못한 줄
(스택 트레이스 연속 행)은 **버리지 않는다.** 직전 줄의 시각을 물려준다 —
예외 본문이 사라지면 그 예외가 무엇이었는지 알 수 없고, 값이 없다는 것과
줄이 없다는 것은 다르다.
"""

from __future__ import annotations

import re
from datetime import datetime

from cluster_doctor.contracts.evidence import EvidenceSource
from cluster_doctor.infrastructure.outbound.agent.common.kst import KST
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.spec import TriageSpec
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.state import RawRecord

# ES 로그 한 줄의 머리: [시각][레벨][로거]. 로거 이름은 오른쪽이 공백으로
# 채워져 있다(``[o.e.c.c.C          ]``).
ES_LOG_LINE_RE = re.compile(
    r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[,.]\d+\]"
    r"\[([A-Z ]+)\]"
    r"\[([^\]]+)\]"
)

SPEC = TriageSpec(
    source=EvidenceSource.NODE_LOG,
    label="node log (문제 노드의 ES 로그)",
    what_matters=(
        "- OutOfMemory, GC overhead, long GC pause.\n"
        "- thread pool rejection, queue full.\n"
        "- circuit breaker 발동.\n"
        "- shard failure, recovery 실패, corrupt.\n"
        "- 마스터와의 연결 끊김, transport 예외.\n"
        "- 스택 트레이스의 첫 줄과 예외 이름."
    ),
    what_is_noise=(
        "- 정상 시작/종료 기록, 설정 로딩.\n"
        "- 같은 예외가 연속으로 수십 줄 반복되는 경우 (대표 한 줄만 남긴다).\n"
        "- 스택 트레이스의 중간 프레임."
    ),
)


def to_records(text: str, *, fallback_time: datetime) -> list[RawRecord]:
    """SSH로 읽은 로그 원문을 Triage 레코드로.

    ``fallback_time``은 첫 줄부터 시각을 뽑지 못했을 때 쓸 값이다. 보통 분석
    구간의 시작을 준다 — 시각이 없는 줄을 버리면 예외 본문이 통째로 사라지고,
    임의의 현재 시각을 붙이면 타임라인이 거짓이 된다.
    """
    records: list[RawRecord] = []
    current_time = fallback_time
    current_level: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        match = ES_LOG_LINE_RE.match(line)
        if match:
            current_level = match.group(2).strip() or None
            try:
                current_time = datetime.fromisoformat(match.group(1)).replace(tzinfo=KST)
            except ValueError:
                pass
        records.append(
            RawRecord(
                record_id=len(records) + 1,
                event_time=current_time,
                line=line,
                severity=current_level,
            )
        )
    return records
