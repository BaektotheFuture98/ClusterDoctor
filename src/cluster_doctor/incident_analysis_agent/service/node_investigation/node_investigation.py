"""마스터 로그에 노드 ID가 적혀 있을 때에만 그 노드를 직접 본다.

    Master Log (구간 전체)
        ↓  (코드 — 노드 ID 추출·순위)
    ProblemNodeCandidate
        ↓  (조회 — LLM 아님)
    ResolvedNode
        ↓  (SSH — LLM 아님)
    Raw Node Logs
        ↓  (분 단위 선별)
    Meaningful Node Evidence

**조건부인 것이 요점이다.** 후보가 없으면 SSH에 붙지 않는다. 접속 하나가
ES 왕복 + 새 세션 + 파일 grep이고, 근거 없이 그것을 치르면 추측에 비용을 쓰는
것이다.

후보를 고르는 것도, Resolver와 Fetcher도 LLM이 아니다. 노드 주소는 추론할 것이 아니라 조회할
것이고, SSH 명령은 모델이 정하지 않는다 — 어느 쪽도 모델이 틀렸을 때 되돌릴
방법이 없다.
"""

from __future__ import annotations

import ipaddress
import logging
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import (
    MasterEvent,
    SourceWindowStatus,
)

from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.node_resolver import (
    NodeResolver,
)
from cluster_doctor.incident_analysis_agent.datasource.ssh import node_log
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import (
    NodeLogFetcher,
)
from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    ProblemNodeCandidate,
)
from cluster_doctor.incident_analysis_agent.model.resolved_node import ResolvedNode
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.evidence_collection.limits import (
    MAX_RAW_LOG_CHARS,
)
from cluster_doctor.incident_analysis_agent.service.observation.node_mentions import (
    node_mentions,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.graph import (
    MAX_CONCURRENCY,
    run_analysis,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    group_into_buckets,
)

_logger = logging.getLogger(__name__)

# 한 번의 분석에서 직접 볼 노드 수. 후보가 많다는 것은 대개 클러스터 전체가
# 흔들렸다는 뜻이고, 그때는 노드 하나하나보다 마스터 로그 쪽이 더 말해 준다.
MAX_NODES_PER_ANALYSIS = 2

@dataclass
class NodeInvestigationResult:
    evidence: list[Evidence] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)
    source_statuses: list[SourceWindowStatus] = field(default_factory=list)
    investigated: list[ResolvedNode] = field(default_factory=list)


def select_problem_nodes(
    events: Iterable[MasterEvent],
    master_evidence: list[Evidence],
    *,
    limit: int = MAX_NODES_PER_ANALYSIS,
) -> list[ProblemNodeCandidate]:
    """마스터 로그에 ID가 적힌 노드 중 먼저 볼 노드를 고른다. 없으면 빈 목록.

    ERROR/WARN 줄에 나온 노드를 앞에 두고, 그다음 언급 횟수, 첫 등장 순이다.
    ``events``는 구간의 마스터 로그 전체이고, 선별 근거가 아니다.
    """
    mentions = node_mentions(events)
    ranked = sorted(
        enumerate(mentions), key=lambda pair: (-pair[1].severity, -pair[1].count, pair[0])
    )
    candidates: list[ProblemNodeCandidate] = []
    for _, mention in ranked[:limit]:
        reason = f"마스터 로그에 노드 ID가 {mention.count}회 나옴"
        if mention.worst_level:
            reason += f" ({mention.worst_level} 포함)"
        candidates.append(
            ProblemNodeCandidate(
                node_id=mention.node_id,
                reason=reason,
                evidence_refs=tuple(
                    item.evidence_id
                    for item in master_evidence
                    if mention.node_id in item.message
                ),
                node_name=mention.node_name,
                ip=mention.ip,
            )
        )
    _logger.info("[node] 문제 노드 후보 %d개", len(candidates))
    return candidates


_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9\-\.]{0,252}[A-Za-z0-9])?$")


def _valid_host(value: str) -> str:
    """SSH 대상으로 쓸 수 있는 IP 또는 hostname이면 그대로 반환, 아니면 빈 문자열."""
    v = (value or "").strip()
    if not v:
        return ""
    try:
        return str(ipaddress.ip_address(v))
    except ValueError:
        pass
    if _HOSTNAME_RE.match(v):
        return v
    return ""


def _unreached_status(window: TimeRange, reason: str, host: str = "") -> SourceWindowStatus:
    return SourceWindowStatus("node_log", window.start, window.end,
        "failed", None, datetime.now(KST), reason, host=host)


def _resolve_by_address(
    candidate: ProblemNodeCandidate,
    resolver: NodeResolver,
    defaults: list[tuple[str | None, str]],
) -> ResolvedNode | None:
    """ES 목록에 없는 노드를 로그에 적힌 주소로 푼다. 로그 경로는 다른 노드의 값을 빌린다.

    ``defaults``는 호출 한 번 동안의 캐시이며 ES는 최대 한 번만 묻는다.
    """
    host = _valid_host(candidate.ip)
    if not host:
        return None
    if not defaults:
        try:
            defaults.append(resolver.cluster_log_defaults())
        except Exception as exc:
            _logger.warning("[node] 클러스터 로그 경로 조회 실패: %s", exc)
            defaults.append((None, ""))
    log_path, cluster_name = defaults[0]
    if not log_path or not cluster_name:
        return None
    return ResolvedNode(
        node_id=candidate.node_id,
        node_name=candidate.node_name,
        host=host,
        log_path=log_path,
        cluster_name=cluster_name,
    )


def investigate_nodes(
    candidates: list[ProblemNodeCandidate],
    window: TimeRange,
    *,
    resolver: NodeResolver,
    fetcher: NodeLogFetcher,
    call_llm: Callable[..., str],
    new_evidence_id: Callable[[], str],
    max_concurrency: int = MAX_CONCURRENCY,
) -> NodeInvestigationResult:
    """후보 노드를 풀고, 붙고, 분 단위 선별한다.

    **후보가 비어 있으면 아무것도 하지 않는다.** resolver도 fetcher도 부르지
    않는다 — 그것이 이 함수의 계약이다.

    각 단계의 실패는 gap으로 남기고 다음 후보로 넘어간다. 노드 하나에 붙지
    못한 것이 분석 전체의 실패는 아니다.
    """
    result = NodeInvestigationResult()
    if not candidates:
        _logger.info("[node] 문제 노드 후보가 없다 — 노드 조사를 건너뛴다")
        result.source_statuses.append(SourceWindowStatus("node_log", window.start, window.end,
            "skipped", None, datetime.now(KST), "조사 대상 노드 후보 없음"))
        return result

    defaults: list[tuple[str | None, str]] = []
    for candidate in candidates:
        try:
            resolved = resolver.resolve(candidate.node_id)
        except Exception as exc:
            _logger.warning("[node] %s 조회 실패: %s", candidate.node_id, exc)
            reason = f"노드 정보 조회 실패 (id={candidate.node_id}): {exc}"
            result.gaps.append(reason)
            result.source_statuses.append(_unreached_status(window, reason, candidate.node_id))
            continue

        if resolved is None:
            resolved = _resolve_by_address(candidate, resolver, defaults)
            if resolved is not None:
                result.gaps.append(
                    f"{candidate.node_id} 노드가 ES 노드 목록에 없어 "
                    f"마스터 로그의 주소 {resolved.host}로 접속했다"
                )
        if resolved is not None and resolved.log_path is None:
            if not defaults:
                try:
                    defaults.append(resolver.cluster_log_defaults())
                except Exception as exc:
                    _logger.warning("[node] 클러스터 로그 경로 조회 실패: %s", exc)
                    defaults.append((None, ""))
            fallback_path, _ = defaults[0]
            if fallback_path:
                resolved = resolved.model_copy(update={"log_path": fallback_path})
                _logger.info("[node] %s: log_path 없음 → 클러스터 기본값 사용 (%s)", candidate.node_id, fallback_path)
        if resolved is None:
            reason = f"ES 노드 목록에 없는 노드라 로그를 수집하지 못했다 (id={candidate.node_id})"
            result.gaps.append(reason)
            result.source_statuses.append(_unreached_status(window, reason, candidate.node_id))
            continue
        if not resolved.is_reachable():
            reason = f"{candidate.node_id} 노드의 접속 정보가 불완전해 로그를 보지 못했다"
            result.gaps.append(reason)
            result.source_statuses.append(_unreached_status(window, reason, resolved.host))
            continue

        try:
            provenance = node_log.ssh_provenance(resolved, window.start, window.end)
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
            result.source_statuses.append(SourceWindowStatus("node_log", window.start, window.end,
                "failed", None, datetime.now(KST), str(exc), host=resolved.host))
            continue

        result.source_statuses.append(SourceWindowStatus("node_log", window.start, window.end,
            "limited" if text.strip() else "ok", len(text.splitlines()), datetime.now(KST), host=resolved.host))
        if not text.strip():
            result.gaps.append(
                f"{candidate.node_id} 노드의 해당 구간 로그가 비어 있었다"
            )
            continue

        result.investigated.append(resolved)
        records = node_log.to_records(
            text[:MAX_RAW_LOG_CHARS],
            fallback_time=window.start,
            provenance=provenance,
        )
        analysis = run_analysis(
            node_log.SPEC,
            group_into_buckets(records),
            call_llm,
            new_evidence_id=new_evidence_id,
            max_concurrency=max_concurrency,
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
            for item in analysis.evidence
        )
        if analysis.failed_minutes:
            result.gaps.append(
                f"{candidate.node_id} 노드 로그 중 {analysis.failed_minutes}개 분의 "
                f"선별이 실패했다(전체 {analysis.analyzed_minutes}개 분)."
            )

    return result
