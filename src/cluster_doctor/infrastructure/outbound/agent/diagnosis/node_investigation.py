"""마스터 로그가 노드를 지목했을 때에만 그 노드를 직접 본다.

    Master Evidence
        ↓  (LLM — 어느 노드가 의심스러운가)
    ProblemNodeCandidate
        ↓  (조회 — LLM 아님)
    ResolvedNode
        ↓  (SSH — LLM 아님)
    Raw Node Logs
        ↓  (Triage 그래프)
    Meaningful Node Evidence

**조건부인 것이 요점이다.** 후보가 없으면 SSH에 붙지 않는다. 접속 하나가
ES 왕복 + 새 세션 + 파일 grep이고, 근거 없이 그것을 치르면 추측에 비용을 쓰는
것이다.

Resolver와 Fetcher는 LLM이 아니다. 노드 주소는 추론할 것이 아니라 조회할
것이고, SSH 명령은 모델이 정하지 않는다 — 어느 쪽도 모델이 틀렸을 때 되돌릴
방법이 없다.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from pydantic import BaseModel, Field

from cluster_doctor.application.exception import LlmApiError, LlmResponseError
from cluster_doctor.application.port.outbound.node_log_fetcher import NodeLogFetcher
from cluster_doctor.application.port.outbound.node_resolver import NodeResolver
from cluster_doctor.application.service.guardrails import truncate_raw
from cluster_doctor.domain.model.evidence import Evidence, ProblemNodeCandidate
from cluster_doctor.infrastructure.outbound.agent.common.log_format import (
    format_evidence_line,
)
from cluster_doctor.domain.model.elasticsearch.resolved_node import ResolvedNode
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.datasource import node_log
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.graph import run_triage

_logger = logging.getLogger(__name__)

# 한 번의 분석에서 직접 볼 노드 수. 후보가 많다는 것은 대개 클러스터 전체가
# 흔들렸다는 뜻이고, 그때는 노드 하나하나보다 마스터 로그 쪽이 더 말해 준다.
MAX_NODES_PER_ANALYSIS = 2

_CANDIDATE_MAX_TOKENS = 1024

_CANDIDATE_PROMPT = """너는 Elasticsearch 장애 분석 파이프라인의 노드 선별 단계다.

아래는 마스터 노드 로그에서 골라낸 근거들이다.
이 중 **특정 노드에 문제가 있다고 볼 근거가 있는 경우에만** 그 노드를 고른다.

규칙:
- 반드시 아래 근거에 이름이 실제로 등장하는 노드만 고른다. 지어내지 않는다.
- evidence_refs에는 그 판단의 근거가 된 [id]를 그대로 쓴다. 비우지 않는다.
- 클러스터 전반의 문제로 보이고 특정 노드를 지목할 수 없으면 **빈 배열**을
  돌려준다. 억지로 고르지 않는다.
- 최대 {limit}개까지만 고른다.
- 원인을 추론하지 않는다. "이 노드를 더 봐야 하는가"만 판단한다.

응답은 JSON 하나로만 한다.

--- 마스터 로그 근거 ---
{evidence}
"""


class _Candidate(BaseModel):
    node_id: str = ""
    reason: str = ""
    evidence_refs: list[str] = Field(default_factory=list)


class _CandidateOutput(BaseModel):
    candidates: list[_Candidate] = Field(default_factory=list)


@dataclass
class NodeInvestigationResult:
    evidence: list[Evidence] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)
    # 실제로 SSH까지 간 노드. 테스트와 로그가 "조건부로 돌았는가"를 확인한다.
    investigated: list[ResolvedNode] = field(default_factory=list)


def find_problem_nodes(
    master_evidence: list[Evidence],
    call_llm: Callable[..., str],
    *,
    limit: int = MAX_NODES_PER_ANALYSIS,
) -> list[ProblemNodeCandidate]:
    """마스터 로그 근거에서 더 볼 노드를 고른다. 없으면 빈 목록.

    근거가 없으면 LLM을 부르지도 않는다. 빈 목록을 보여 주고 "고르라"고 하는
    호출은 비용만 들고 결과가 정해져 있다.
    """
    if not master_evidence:
        return []

    known_refs = {item.evidence_id for item in master_evidence}
    prompt = _CANDIDATE_PROMPT.format(
        limit=limit,
        evidence="\n".join(format_evidence_line(item) for item in master_evidence),
    )
    try:
        text = call_llm(
            [{"role": "user", "content": prompt}],
            _CANDIDATE_MAX_TOKENS,
            response_format=_CandidateOutput,
        )
    except (LlmApiError, LlmResponseError) as exc:
        _logger.warning("[node] 문제 노드 후보 선별 실패: %s", exc)
        return []

    try:
        data = json.loads(text)
        raw = data.get("candidates") if isinstance(data, dict) else None
    except (json.JSONDecodeError, TypeError):
        _logger.warning("[node] 후보 응답이 JSON이 아니다")
        return []
    if not isinstance(raw, list):
        return []

    candidates: list[ProblemNodeCandidate] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        node_id = str(item.get("node_id") or "").strip()
        refs = tuple(
            str(ref).strip()
            for ref in (item.get("evidence_refs") or [])
            # 없는 참조를 근거로 든 후보는 버린다. 근거 없는 지목에 SSH 비용을
            # 쓰지 않는다는 규칙이 여기서 강제된다.
            if str(ref).strip() in known_refs
        )
        if not node_id or not refs:
            _logger.info("[node] 근거 없는 후보를 버린다: %r", item)
            continue
        candidates.append(
            ProblemNodeCandidate(
                node_id=node_id,
                reason=str(item.get("reason") or "").strip(),
                evidence_refs=refs,
            )
        )
        if len(candidates) >= limit:
            break

    _logger.info("[node] 문제 노드 후보 %d개", len(candidates))
    return candidates


def investigate_nodes(
    candidates: list[ProblemNodeCandidate],
    window: TimeRange,
    *,
    resolver: NodeResolver,
    fetcher: NodeLogFetcher,
    call_llm: Callable[..., str],
    new_evidence_id: Callable[[], str],
    put_raw: Callable[[str], str],
) -> NodeInvestigationResult:
    """후보 노드를 풀고, 붙고, Triage한다.

    **후보가 비어 있으면 아무것도 하지 않는다.** resolver도 fetcher도 부르지
    않는다 — 그것이 이 함수의 계약이다.

    각 단계의 실패는 gap으로 남기고 다음 후보로 넘어간다. 노드 하나에 붙지
    못한 것이 분석 전체의 실패는 아니다.
    """
    result = NodeInvestigationResult()
    if not candidates:
        _logger.info("[node] 문제 노드 후보가 없다 — 노드 조사를 건너뛴다")
        return result

    for candidate in candidates:
        try:
            resolved = resolver.resolve(candidate.node_id)
        except Exception as exc:
            _logger.warning("[node] %s 조회 실패: %s", candidate.node_id, exc)
            result.gaps.append(f"노드 정보 조회 실패 (id={candidate.node_id}): {exc}")
            continue

        if resolved is None:
            result.gaps.append(f"노드를 찾지 못했다 (id={candidate.node_id})")
            continue
        if not resolved.is_reachable():
            result.gaps.append(
                f"{candidate.node_id} 노드의 접속 정보가 불완전해 로그를 보지 못했다"
            )
            continue

        try:
            text = fetcher.fetch(
                resolved.host,
                resolved.log_path or "",
                resolved.cluster_name,
                start_dt=window.start,
                end_dt=window.end,
            )
        except Exception as exc:
            _logger.warning("[node] %s SSH 실패: %s", candidate.node_id, exc)
            result.gaps.append(f"{candidate.node_id} 노드 로그 SSH 수집 실패: {exc}")
            continue

        if not text.strip():
            result.gaps.append(
                f"{candidate.node_id} 노드의 해당 구간 로그가 비어 있었다"
            )
            continue

        result.investigated.append(resolved)
        records = node_log.to_records(
            truncate_raw(text), fallback_time=window.start
        )
        triage = run_triage(
            node_log.SPEC,
            records,
            call_llm,
            new_evidence_id=new_evidence_id,
            put_raw=put_raw,
        )
        # 노드 이름을 코드가 붙인다. SSH 원문에는 자기 노드 이름이 없으므로,
        # 모델에게 물으면 지어낸다.
        result.evidence.extend(
            item.model_copy(
                update={
                    "node_id": resolved.node_id,
                    "node_name": resolved.node_name or resolved.node_id,
                }
            )
            for item in triage.evidence
        )
        if triage.failed_minutes:
            result.gaps.append(
                f"{candidate.node_id} 노드 로그 중 {triage.failed_minutes}개 분의 "
                f"선별이 실패했다(전체 {triage.analyzed_minutes}개 분)."
            )

    return result
