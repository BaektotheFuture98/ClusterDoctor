"""Triage 그래프의 노드.

노드는 LLM 호출 방법을 모른다. provider/model/api_key가 이미 묶인 호출자를
받아 쓴다 — 덕분에 테스트가 litellm을 몽키패치하지 않고 선별 로직만 검증할 수
있고, provider 선택은 조립 시점에 한 번만 결정된다.

**모델은 번호만 돌려준다.** 시각·노드·원문은 코드가 ``RawRecord``에서 옮긴다.
이 규칙 하나가 Evidence의 전사 오류를 구조적으로 불가능하게 만든다.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime

from pydantic import BaseModel, Field

from cluster_doctor.application.exception import LlmApiError, LlmResponseError
from cluster_doctor.domain.model.evidence import Evidence
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.prompt import (
    build_map_prompt,
    build_reduce_prompt,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.spec import TriageSpec
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.state import (
    MinuteBucket,
    MinuteResult,
    RawRecord,
    SelectedRecord,
    TriageState,
)

_logger = logging.getLogger(__name__)

# 분별 선별은 번호와 짧은 이유만 돌려주므로 길 이유가 없다. 한도를 낮추면
# finish_reason="length"로 잘릴 여지도 함께 줄어든다.
_MAP_MAX_TOKENS = 1024
# Reduce는 구간 전체의 후보를 보고 고르므로 조금 더 준다.
_REDUCE_MAX_TOKENS = 2048

# ``complete``에서 provider/model/api_key를 미리 묶어 둔 형태. 호출부는
# 메시지·토큰 한도·응답 스키마만 정한다.
StructuredLlmCaller = Callable[..., str]
EvidenceIdFactory = Callable[[], str]
RawRefFactory = Callable[[str], str]


class MapSelection(BaseModel):
    record_id: int
    event_type: str = ""
    reason: str = ""


class MapOutput(BaseModel):
    selected: list[MapSelection] = Field(default_factory=list)


class ReduceSelection(BaseModel):
    record_id: int
    selection_reason: str = ""


class ReduceOutput(BaseModel):
    keep: list[ReduceSelection] = Field(default_factory=list)


def make_chunk_by_minute() -> Callable[[TriageState], dict]:
    """레코드를 1분 버킷으로 나눈다. 빈 분은 만들지 않는다.

    로그가 하나도 없는 분에 LLM을 부르는 것은 순수한 낭비다 — 새벽처럼 한산한
    시간대에는 대부분의 구간이 비어 있다.
    """

    def chunk_by_minute(state: TriageState) -> dict:
        grouped: dict[datetime, list[RawRecord]] = {}
        for record in state["records"]:
            minute = record.event_time.replace(second=0, microsecond=0)
            grouped.setdefault(minute, []).append(record)

        buckets = [
            MinuteBucket(minute=minute, records=grouped[minute])
            for minute in sorted(grouped)
        ]
        _logger.info(
            "[triage] %d개 레코드를 비어 있지 않은 분 %d개로 나눴다",
            len(state["records"]),
            len(buckets),
        )
        return {"buckets": buckets}

    return chunk_by_minute


def make_map_minute(spec: TriageSpec, call_llm: StructuredLlmCaller):
    """한 분에서 후보를 고르는 노드를 만든다.

    실패해도 예외를 올리지 않고 ``failed=True`` 결과를 돌려준다. 한 분이 rate
    limit에 걸렸다고 나머지 분의 선별까지 버리는 것은 과하다.
    """

    def map_minute(bucket: MinuteBucket) -> dict:
        label = bucket.minute.strftime("%Y-%m-%d %H:%M")
        valid_ids = {record.record_id for record in bucket.records}
        prompt = build_map_prompt(spec, bucket)
        try:
            text = call_llm(
                [{"role": "user", "content": prompt}],
                _MAP_MAX_TOKENS,
                response_format=MapOutput,
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("[triage] %s %s 선별 실패: %s", spec.label, label, exc)
            return {
                "minute_results": [
                    MinuteResult(
                        minute=bucket.minute,
                        failed=True,
                        record_count=len(bucket.records),
                    )
                ]
            }

        selected = [
            SelectedRecord(
                record_id=item["record_id"],
                event_type=str(item.get("event_type") or "").strip(),
                reason=str(item.get("reason") or "").strip(),
            )
            for item in _parse_items(text, "selected")
            # 없는 번호는 버린다. 모델이 지어낸 번호를 통과시키면 Evidence가
            # 가리키는 원문이 실제 로그와 어긋난다 — 번호로 고르게 한 이유가
            # 사라진다.
            if item.get("record_id") in valid_ids
        ]
        _logger.info(
            "[triage] %s %s → %d/%d줄 선별",
            spec.label,
            label,
            len(selected),
            len(bucket.records),
        )
        return {
            "minute_results": [
                MinuteResult(
                    minute=bucket.minute,
                    selected=selected,
                    record_count=len(bucket.records),
                )
            ]
        }

    return map_minute


def make_reduce(
    spec: TriageSpec,
    call_llm: StructuredLlmCaller,
    *,
    new_evidence_id: EvidenceIdFactory,
    put_raw: RawRefFactory,
):
    """구간 전체를 보고 남길 근거를 확정하는 노드를 만든다.

    **여기서 근본 원인을 정하지 않는다.** 이 단계의 출력은 결론이 아니라 줄어든
    근거 목록이다. 원인 판단은 모든 datasource의 Evidence가 모인 뒤에 한다.

    LLM이 실패하면 Map 선별 결과를 그대로 남긴다. 반복·정상 이벤트 제거가
    수행되지 않았다는 뜻이므로 ``reduce_degraded``로 표시하고, 호출자가 그것을
    gap으로 남긴다 — 조용히 넘기면 걸러지지 않은 목록이 걸러진 것처럼 쓰인다.
    """

    def reduce(state: TriageState) -> dict:
        records = {record.record_id: record for record in state["records"]}
        results = state["minute_results"]
        selections = {
            item.record_id: item
            for result in results
            if not result.failed
            for item in result.selected
        }

        if not selections:
            return {"evidence": [], "reduce_degraded": False}

        degraded = False
        chosen: list[tuple[int, str]] = []
        prompt = build_reduce_prompt(spec, results, records, limit=spec.max_evidence)
        try:
            text = call_llm(
                [{"role": "user", "content": prompt}],
                _REDUCE_MAX_TOKENS,
                response_format=ReduceOutput,
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("[triage] %s reduce 실패: %s", spec.label, exc)
            degraded = True
        else:
            chosen = [
                (item["record_id"], str(item.get("selection_reason") or "").strip())
                for item in _parse_items(text, "keep")
                if item.get("record_id") in selections
            ]

        # **빈 응답과 실패를 가른다.** Reduce가 정상적으로 "남길 것이 없다"고
        # 답하는 것은 정당한 결과다 — 구간 내내 같은 이벤트만 반복됐다면 그렇게
        # 나온다. 그때 Map 결과를 되살리면 걸러내라고 만든 단계가 아무것도 걸러
        # 내지 못한다. 반대로 호출 자체가 실패했을 때는 되살린다 — 걸러지지
        # 않은 근거가, 근거가 없는 것보다 낫다.
        if degraded:
            chosen = [
                (record_id, "(Reduce 단계가 실패해 분별 결과를 그대로 남겼다)")
                for record_id in sorted(
                    selections, key=lambda rid: records[rid].event_time
                )
            ]

        evidence: list[Evidence] = []
        seen_lines: set[str] = set()
        for record_id, selection_reason in chosen:
            record = records.get(record_id)
            if record is None or record.line in seen_lines:
                continue
            seen_lines.add(record.line)
            picked = selections.get(record_id)
            evidence.append(
                Evidence(
                    evidence_id=new_evidence_id(),
                    event_time=record.event_time,
                    source=spec.source,
                    node_id=record.node_id,
                    node_name=record.node_name,
                    event_type=(picked.event_type if picked else "") or None,
                    severity=record.severity,
                    message=record.line,
                    raw_ref=put_raw(record.line),
                    selection_reason=selection_reason or (picked.reason if picked else ""),
                )
            )
            if len(evidence) >= spec.max_evidence:
                break

        evidence.sort(key=lambda item: item.event_time)
        _logger.info(
            "[triage] %s → Evidence %d건 (후보 %d건, degraded=%s)",
            spec.label,
            len(evidence),
            len(selections),
            degraded,
        )
        return {"evidence": evidence, "reduce_degraded": degraded}

    return reduce


def _parse_items(text: str, key: str) -> list[dict]:
    """구조화 응답에서 항목 목록을 꺼낸다. 형식이 어긋나면 빈 목록.

    ``response_format``이 걸려 있어도 방어적으로 판다. provider가 바뀌거나
    구조화 출력이 조용히 무시될 수 있고, 그때 파싱 예외가 새면 노드 하나가
    아니라 그래프 전체가 죽는다.
    """
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        _logger.warning("[triage] 응답이 JSON이 아니다 — 선별을 비운다")
        return []
    if not isinstance(data, dict):
        return []
    items = data.get(key)
    if not isinstance(items, list):
        return []

    parsed: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            item = {**item, "record_id": int(item["record_id"])}
        except (KeyError, TypeError, ValueError):
            continue
        parsed.append(item)
    return parsed
