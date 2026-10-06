"""근거 수집이 강제하는 상한.

한 datasource workflow가 남길 수 있는 Evidence 수와, LLM에게 보여 줄 원문
한 덩어리의 크기를 제한한다. Analysis Agent 전용이다 — Incident 전체 예산은
``incident_orchestrator_agent/service/analysis_window/guardrails.py``에 있다.
"""

from __future__ import annotations

import logging

_logger = logging.getLogger(__name__)

# 한 datasource workflow가 남길 수 있는 Evidence 수. Reduce가 "중요하지 않은
# 것을 지운다"를 수행하지 않고 전부 통과시키면 Cross-source 단계의 프롬프트가
# 원문 크기로 돌아간다.
MAX_EVIDENCE_PER_SOURCE = 25
MAX_EVIDENCE_TOTAL = 80

# LLM에게 보여 줄 원문 한 덩어리의 상한.
MAX_RAW_LOG_CHARS = 60_000


def truncate_raw(text: str, limit: int = MAX_RAW_LOG_CHARS) -> str:
    """프롬프트에 실을 원문을 상한으로 자른다. 잘린 사실을 본문에 적는다."""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... (원문 {len(text) - limit}자를 상한으로 잘랐다)"
