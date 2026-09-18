"""datasource workflow가 골라낸 의미 있는 근거 하나.

Triage가 요약 문자열만 돌려주면 Cross-source 분석이 그 문장을 다시 파싱해야
하고, 그 순간 시각·노드·수치가 모델이 옮겨 적은 값이 된다. 이 저장소가 두 번
당한 실패가 그것이다(``diagnosis_report`` 모듈 docstring). 그래서 근거는
**필드로** 나른다.

``raw_ref``가 요점이다. 원문 전량을 Evidence에 싣지 않으면서도 필요할 때 다시
꺼낼 수 있어야 한다 — 리포트 검증이 "이 주장이 실제 로그 줄과 맞는가"를 보려면
원문에 닿아야 하고, Context에는 원문을 쌓지 않아야 한다. 참조는
``ArtifactStore``가 푼다.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

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


class Evidence(BaseModel):
    """어느 datasource에서 왔든 같은 모양이 되는 근거 한 건."""

    model_config = ConfigDict(frozen=True)

    evidence_id: str

    event_time: datetime
    source: EvidenceSource

    node_id: str | None = None
    node_name: str | None = None

    event_type: str | None = None
    severity: str | None = None

    message: str

    # 원문을 다시 꺼낼 참조. 비어 있으면 그 근거는 인용으로 검증할 수 없다.
    raw_ref: str | None = None
    # 왜 이 줄을 남겼는가. Reduce 단계가 채운다.
    selection_reason: str | None = None

    def cite(self) -> str:
        """프롬프트에 실을 한 줄. 모델이 id로 골라 쓰게 한다."""
        parts = [f"[{self.evidence_id}]", self.event_time.strftime("%Y-%m-%d %H:%M:%S")]
        parts.append(str(self.source))
        if self.severity:
            parts.append(self.severity)
        if self.node_name or self.node_id:
            parts.append(f"node={self.node_name or self.node_id}")
        parts.append(self.message)
        return " | ".join(parts)


class ProblemNodeCandidate(BaseModel):
    """마스터 로그가 지목한, 더 들여다볼 노드.

    ``evidence_refs``가 비어 있으면 후보가 아니다. 근거 없이 노드를 지목하면
    SSH 접속 비용을 추측에 쓰게 된다 — Node Investigation이 조건부인 이유가
    그것이다.
    """

    model_config = ConfigDict(frozen=True)

    node_id: str
    reason: str = ""
    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)


class ResolvedNode(BaseModel):
    """노드 식별자를 접속 가능한 주소로 푼 결과.

    ``NodeResolver``가 만든다. LLM은 이 값을 만들지 않는다 — 노드 주소는 추론할
    것이 아니라 조회할 것이다.
    """

    model_config = ConfigDict(frozen=True)

    node_id: str
    node_name: str = ""
    host: str = ""
    log_path: str | None = None
    cluster_name: str = ""

    def is_reachable(self) -> bool:
        """SSH로 붙어 볼 만한 값이 다 있는가."""
        return bool(self.host and self.log_path)
