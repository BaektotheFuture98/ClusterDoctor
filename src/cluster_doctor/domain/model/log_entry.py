"""세 소스가 공유하는 계약.

개별 사건(event) 로그(slowlog / es_query_log / node_log)와 주기 수집 샘플
(node_metric)이 전부 이 베이스를 쓴다. 실제 타입은 소스별 폴더에 있다 —
``kafka/slowlog_entry.py``, ``clickhouse/node_log_entry.py``,
``clickhouse/query_log_entry.py``, ``clickhouse/node_metric_entry.py``.

공통으로 두는 것은 발생 시각과 출처뿐이다. ``fetch_logs``가 여럿을 한 리스트에
담아 돌려주고 ``split_by_minute``이 ``timestamp``로 묶으므로 그 둘은 필요하다.
``source``는 인스턴스 필드가 아니라 ``ClassVar``다 — 타입이 정해지면 출처도
정해지므로 생성자에서 매번 넘길 이유가 없고, 넘기면 오타 한 번에 프롬프트의
소스별 묶기가 조용히 어긋난다.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar


@dataclass(frozen=True)
class LogEntry:
    """세 소스가 공유하는 계약."""

    timestamp: datetime

    # 값을 주지 않는다. 구현체가 반드시 정의해야 하는 것이지, 빠뜨렸을 때
    # 조용히 넘어갈 기본값이 있어서는 안 된다.
    source: ClassVar[str]
