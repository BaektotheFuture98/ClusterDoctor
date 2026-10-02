"""datasource workflow가 골라낸 의미 있는 근거 하나.

분 단위 선별이 요약 문자열만 돌려주면 Cross-source 분석이 그 문장을 다시 파싱해야
하고, 그 순간 시각·노드·수치가 모델이 옮겨 적은 값이 된다. 이 저장소가 두 번
당한 실패가 그것이다(``observations.py`` 모듈 docstring). 그래서 근거는
**필드로** 나른다.

근거는 수집된 DTO에서 코드가 렌더링한 ``message``와 수집 위치만 나른다. DTO를
문자열로 다시 직렬화한 사본은 두지 않는다. 보고서와 검증은 DTO
(``Observations``)를 기준으로 하고, 쿼리 로그 근거는 ``record_key``로 그 실행을
가리킨다.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class EvidenceSource(StrEnum):
    """근거가 어느 datasource에서 왔는가.

    값은 ``LogEntry.source``의 문자열과 맞춘다. 두 체계가 갈리면 소스별 집계가
    조용히 어긋난다.
    """

    SLOWLOG = "slowlog"
    QUERY_LOG = "es_query_log"
    NODE_METRIC = "node_metric"
    MASTER_LOG = "master_log"
    NODE_LOG = "node_log"
    CLUSTER_STATE = "cluster_state"


class EvidenceProvenance(BaseModel):
    """Collection-time location, never authored by the model."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    method: Literal["ssh", "clickhouse", "elasticsearch_api"]
    collected_at: datetime | None = None
    query_from: datetime | None = None
    query_to: datetime | None = None
    host: str | None = None
    table: str | None = None
    file_path: str | None = None
    endpoint: str | None = None
    role: str | None = None
    excerpt: bool = False


class Evidence(BaseModel):
    """소스별 원자료에서 선별해 Incident 식별자를 부여한 근거 한 건.

    조회 계약 LogEntry, 실행 내 번호를 쓰는 RawRecord와 달리 보고서가 참조한다.
    수집기(``evidence_collection/collector.py``/``workflow/minute_analysis/nodes.py``/
    ``datasource/clickhouse/node_metric.py``) 셋만 만든다.

    ``message``는 Cross-source 프롬프트(``format_evidence_line``)와 운영자
    리포트 인용(``evidence_citation.cite``)이 함께 읽는, 코드가 렌더링한 한
    줄이다. 소스가 파일 원문이면(SSH ``node_log``) 원문 줄 그대로이고, 그 외에는
    구조화된 필드에서 조립한 서술이다.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    evidence_id: str

    event_time: datetime
    source: EvidenceSource

    node_id: str | None = None
    node_name: str | None = None

    event_type: str | None = None
    severity: str | None = None

    # 코드가 렌더링한 근거 한 줄. 소스에 따라 원문 그대로이거나 구조화된 값을
    # 조립한 서술이다. 프롬프트와 운영자 리포트가 함께 읽는다.
    message: str

    provenance: EvidenceProvenance | None = None
    time_origin: Literal["parsed", "inherited", "fallback"] = "parsed"
    # 왜 이 줄을 남겼는가. Reduce 단계가 채운다.
    selection_reason: str | None = None
    # 쿼리 로그 근거가 가리키는 실행 한 건의 지문(``query_record_key``).
    # ``Observations.query_requests``의 DTO와 이 값으로 연결한다. 다른 소스는 None.
    record_key: str | None = None


class ProblemNodeCandidate(BaseModel):
    """마스터 로그가 지목한, 더 들여다볼 노드.

    ``evidence_refs``가 비어 있으면 후보가 아니다. 근거 없이 노드를 지목하면
    SSH 접속 비용을 추측에 쓰게 된다 — Node Investigation이 조건부인 이유가
    그것이다.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    node_id: str
    reason: str = ""
    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)
