"""로그·메트릭 소스가 공유하는 계약.

개별 사건(event) 로그(slowlog / es_query_log / node_log)와 주기 수집 샘플
(node_metric)이 전부 이 베이스를 쓴다. 실제 타입(``SlowlogEntry``,
``NodeLogEntry``, ``QueryLogEntry``, ``NodeMetricEntry``)은 소스별 폴더로
나뉘어 있지 않고 전부 이 파일 하나에 정의돼 있다.

공통 계약은 시각 접근자 ``timestamp``와 출처다. 쿼리 로그는 원본 필드
``reg_date``를 저장하고 내부 공통 처리에서만 읽기 전용 시각 접근자를 제공한다.
``fetch_logs``는 여러 소스의 레코드를 ``LogFetchResult.entries``에 함께 담는다.
조회 결과 정렬과 분 단위 관측값 집계는 공통 ``timestamp`` 접근자를 사용한다.
``source``는 인스턴스 필드가 아니라 ``ClassVar``다 — 타입이 정해지면 출처도
정해지므로 생성자에서 매번 넘길 이유가 없고, 넘기면 오타 한 번에 프롬프트의
소스별 묶기가 조용히 어긋난다.
"""

import hashlib
import json
from dataclasses import dataclass, field, fields
from datetime import datetime
from decimal import Decimal
from typing import ClassVar

from cluster_doctor.incident_analysis_agent.model.evidence import EvidenceProvenance


@dataclass(frozen=True)
class LogEntry:
    """저장소가 돌려주는 소스별 조회 결과의 공통 계약.

    아직 선별 전이며, 파이프라인이 RawRecord와 보고서 근거 Evidence로 변환한다.
    """

    provenance: EvidenceProvenance | None = field(default=None, kw_only=True)

    # 값을 주지 않는다. 구현체가 반드시 정의해야 하는 것이지, 빠뜨렸을 때
    # 조용히 넘어갈 기본값이 있어서는 안 된다.
    source: ClassVar[str]


def record_json(entry: LogEntry) -> str:
    """Serialize only the fetched record fields, without collection metadata."""
    values = {
        f.name: getattr(entry, f.name)
        for f in fields(entry)
        if f.name != "provenance"
    }
    return json.dumps(
        values,
        ensure_ascii=False,
        default=str,
        indent=2,
    )


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

    SSH 경로(``datasource/ssh/node_log.py``)는 이 타입을 만들지 않는다 — 원문 문자열을
    그대로 돌려준다.
    """

    source: ClassVar[str] = "node_log"

    timestamp: datetime

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


@dataclass(frozen=True)
class NodeMetricEntry(LogEntry):
    """한 노드의 한 시점 리소스 샘플."""

    source: ClassVar[str] = "node_metric"

    timestamp: datetime

    node_name: str
    node_ip: str
    os_cpu_percent: int
    os_mem_used_percent: int
    process_cpu_percent: int
    jvm_heap_used_percent: int
    search_active: int
    search_queue: int
    search_rejected: int
    write_active: int
    write_queue: int
    write_rejected: int


@dataclass(frozen=True)
class QueryLogEntry(LogEntry):
    """ClickHouse log record. Column names and stored values are preserved."""

    source: ClassVar[str] = "es_query_log"

    reg_date: datetime
    host: str
    run_time: Decimal
    success: str
    s_date: int
    e_date: int
    date_range: int
    keyword: tuple[str, ...]
    # url 컬럼은 저장하지 않는다. 수집 시 요청 대상과 DSL 조건만 뽑는다
    # (datasource/clickhouse/query_url.py).
    target_host: str | None
    index_name: str | None
    conditions: tuple[str, ...]
    cmd: str
    service: str
    env: str
    project: str
    company: str
    user: str
    search_count: int
    etc: str
    cluster: str
    # 테이블 컬럼이 아니다. 수집 시 keyword를 앞 5개로 자르면서 버린 개수를
    # 담는 계산 값이다. 0이면 잘리지 않았다.
    keyword_omitted: int = field(default=0, kw_only=True)

    @property
    def timestamp(self) -> datetime:
        """Internal event-time accessor; the stored DTO field remains reg_date."""
        return self.reg_date

    @property
    def keyword_text(self) -> str:
        """프롬프트·로그용 표기. 잘린 개수가 있으면 "외 N개"를 붙인다."""
        text = f"keyword={list(self.keyword)}"
        return f"{text} 외 {self.keyword_omitted}개" if self.keyword_omitted else text

    @property
    def is_success(self) -> bool | None:
        return {"Y": True, "N": False}.get(self.success)


@dataclass(frozen=True)
class SlowlogEntry(LogEntry):
    """ES가 임계치 초과로 남긴 느린 쿼리 한 건.

    발생 시각 외 필드는 조회 결과에서 채우며 비어 있을 수 있다. Kafka
    트리거는 intake 전용 이벤트 타입을 사용하고 이 조회 타입을 재사용하지 않는다.
    """

    source: ClassVar[str] = "slowlog"

    timestamp: datetime

    index_name: str = ""
    node: str = ""
    took: str = ""
    total_hits: str = ""
    total_shards: int = 0
    # x-opaque-id 원문. "service=web,project=...,company=50,user=579,..." 형태이며
    # company/user가 여기 숫자 ID로 들어 있다 — es_query_log의 표시명과는 다른
    # 식별자 체계라 아직 필드로 분리하지 않는다.
    opaque_id: str = ""
    query: str = ""


def query_record_key(record: QueryLogEntry) -> str:
    # 가져온 레코드의 지문. 근거를 한 건으로 특정하려는 용도이지 쿼리 식별자가 아니다.
    # DTO에 저장된 파싱 필드를 해시하므로, 저장 필드가 모두 같은 행은 같은 키를 갖는다.
    canonical = json.dumps(json.loads(record_json(record)), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()
