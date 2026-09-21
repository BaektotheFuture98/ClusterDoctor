from dataclasses import dataclass
from typing import ClassVar

from cluster_doctor.domain.model.log_entry import LogEntry


@dataclass(frozen=True)
class NodeLogEntry(LogEntry):
    """ES 노드가 남긴 로그 한 줄.

    같은 내용을 SSH로도 가져올 수 있지만(데이터 노드는 아직 그 경로뿐이다),
    ClickHouse 조회가 세 가지에서 낫다.

      - ``node_role``이 있어 마스터 노드를 이름 없이 지목할 수 있다.
        SSH 경로는 ES에 ``_master``를 물어 IP를 얻는 왕복이 필요했다.
      - ``level``·``detected_level``이 컬럼이라 severity 정규식이 필요 없다.
      - 여러 노드를 한 쿼리로 본다. SSH는 노드마다 접속을 새로 열었다.

    ``level``은 ES가 기록한 값이고 ``detected_level``은 수집기가 추론한 값이다.
    둘이 어긋날 수 있으므로(``"WARN "`` 대 ``"warn"``) 둘 다 들고 있는다 —
    어느 쪽을 신뢰할지는 조회하는 쪽이 정한다.

    SSH 경로(``domain/model/ssh``)는 이 타입을 만들지 않는다 — 원문 문자열을
    그대로 돌려준다.
    """

    source: ClassVar[str] = "node_log"

    node: str
    node_role: str
    level: str
    detected_level: str
    logger: str
    filename: str
    host: str
    # 로그 원문 한 줄. 파싱하지 않는다 — 스택 트레이스·GC 통계처럼 구조가
    # 제각각인 내용이 들어오고, 진단에 쓰이는 것은 문장 그대로다.
    line: str
