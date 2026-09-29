"""Report의 주장이 Evidence의 원문(Raw)과 맞는지 Claim 단위로 대조한다.

``validate_report``는 구조화된 필드만 본다. 원문이 그 주장을 실제로 뒷받침하는가는
문자열로 판단할 수 없어서, 여기서는 Claim과 원문을 함께 LLM에 주고 판정만
구조화된 JSON으로 받는다.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable

from cluster_doctor.application.ports.artifact_store import ArtifactStore
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport
from cluster_doctor.domain.diagnosis.validation_types import (
    MismatchKind,
    ValidationIssue,
)
from cluster_doctor.exceptions import LlmApiError, LlmResponseError

_logger = logging.getLogger(__name__)

# 원문 한 건이 프롬프트에서 차지할 수 있는 최대 길이. 원문 전량을 실으면
# Claim 수만큼 비용이 곱해진다.
_MAX_RAW_CHARS = 2000
_MAX_TOKENS = 4096

_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$")


def _unverifiable(description: str) -> ValidationIssue:
    # 검증을 돌리지 못한 것은 통과가 아니다. 빈 목록으로 돌려주면 대조하지
    # 못한 리포트가 PASSED로 나간다.
    return ValidationIssue(
        kind=MismatchKind.UNVERIFIABLE,
        description=description,
        affected_window=None,
    )


class GroundingValidator:
    def __init__(
        self, *, store: ArtifactStore, call_llm: Callable[..., str]
    ) -> None:
        self._store = store
        self._call_llm = call_llm

    def validate(
        self, report: LogAnalysisReport, incident_id: str
    ) -> list[ValidationIssue]:
        claims = self._extract_claims(report)
        if not claims:
            return []
        evidence = self._store.get_evidence(incident_id, tuple(report.evidence_refs))
        prompt = self._build_prompt(claims, {item.evidence_id: item for item in evidence})
        try:
            text = self._call_llm([{"role": "user", "content": prompt}], _MAX_TOKENS)
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("[grounding] 원문 대조 호출이 실패했다: %s", exc)
            return [_unverifiable(f"원문 대조 호출이 실패했다: {exc}")]
        return self._parse_response(text)

    @staticmethod
    def _extract_claims(report: LogAnalysisReport) -> list[dict]:
        claims: list[dict] = []
        for event in report.timeline:
            claims.append(
                {
                    "type": "timeline",
                    "description": event.description,
                    "evidence_refs": list(event.evidence_refs),
                }
            )
        for finding in report.findings:
            claims.append(
                {
                    "type": "finding",
                    "description": finding.detail or finding.title,
                    "evidence_refs": list(finding.evidence_refs),
                }
            )
        for cause in report.root_causes:
            claims.append(
                {
                    "type": "root_cause",
                    "description": cause.statement,
                    "evidence_refs": list(cause.supporting_evidence_refs),
                }
            )
        return claims

    def _build_prompt(self, claims: list[dict], evidence_map: dict) -> str:
        raw_texts: dict[str, str] = {}
        for evidence_id, item in evidence_map.items():
            raw = self._store.get_raw(item.raw_ref) if item.raw_ref else None
            raw_texts[evidence_id] = raw[:_MAX_RAW_CHARS] if raw else "(raw 없음)"

        return (
            "Report Claim과 Evidence Raw 원문의 사실적 일치를 검증하라.\n\n"
            f"Claims:\n{json.dumps(claims, ensure_ascii=False, indent=2)}\n\n"
            f"Evidence Raw:\n{json.dumps(raw_texts, ensure_ascii=False, indent=2)}\n\n"
            "각 Claim에 대해 PASSED 또는 MISMATCH 판정을 JSON 배열로 반환하라.\n"
            "분석 자체가 원문과 어긋나면 kind는 'analysis_mismatch', "
            "분석은 맞지만 표현이 과하거나 틀리면 'report_mismatch'다.\n"
            '형식: [{"claim": "...", "status": "PASSED|MISMATCH", '
            '"reason": "...", "kind": "...", "affected_evidence_refs": [...]}]'
        )

    @staticmethod
    def _parse_response(response: str) -> list[ValidationIssue]:
        try:
            items = json.loads(_FENCE.sub("", response.strip()))
        except json.JSONDecodeError:
            _logger.warning("[grounding] 검증 응답을 JSON으로 읽지 못했다")
            return [_unverifiable("원문 대조 응답을 읽지 못했다")]
        if not isinstance(items, list):
            return [_unverifiable("원문 대조 응답이 배열이 아니다")]

        issues: list[ValidationIssue] = []
        for item in items:
            if not isinstance(item, dict) or item.get("status") != "MISMATCH":
                continue
            try:
                kind = MismatchKind(item.get("kind", MismatchKind.UNVERIFIABLE))
            except ValueError:
                kind = MismatchKind.UNVERIFIABLE
            issues.append(
                ValidationIssue(
                    kind=kind,
                    description=str(item.get("reason", "")),
                    affected_window=None,
                    affected_evidence_refs=list(item.get("affected_evidence_refs", [])),
                )
            )
        return issues
