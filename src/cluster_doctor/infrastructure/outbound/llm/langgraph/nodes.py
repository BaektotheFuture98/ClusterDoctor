"""그래프 노드. 각 노드는 상태 일부를 받아 상태 일부를 돌려준다.

노드는 LLM 호출 방법을 모른다. ``LlmCaller``(부분 적용된 ``complete``)를
받아 쓴다. 덕분에 테스트가 litellm을 몽키패치하지 않고 노드 로직만 검증할 수
있고, provider 선택은 조립 시점에 한 번만 결정된다.
"""

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta

from pydantic import BaseModel

from cluster_doctor.infrastructure.outbound.llm.langgraph.prompts import (
    build_minute_prompt,
    build_synthesis_prompt,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.state import (
    GraphState,
    MinuteBucket,
    MinuteFinding,
)
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.application.port.outbound.llm_analyzer import (
    LlmApiError,
    LlmResponseError,
)

_logger = logging.getLogger(__name__)

# 분별 요약은 짧아야 한다. 한도를 낮추면 finish_reason="length"로 잘릴
# 여지도 함께 줄어든다.
_MINUTE_MAX_TOKENS = 1024
# 최종 리포트는 7개 섹션을 다 써야 하므로 단발 모드와 같은 한도를 준다.
_SYNTHESIS_MAX_TOKENS = 8192

# ``complete``에서 provider/model/api_key를 미리 묶어 둔 형태.
# 노드는 누구에게 묻는지 모르고, 얼마나 길게 답할지만 정한다.
LlmCaller = Callable[[list[dict], int], str]


class MinuteOutput(BaseModel):
    summary: str
    evidence: list[str]

_MINUTE_FORMAT = "%Y-%m-%d %H:%M"


def split_by_minute(state: GraphState) -> dict:
    """로그를 1분 버킷으로 나눈다. 빈 구간은 만들지 않는다.

    로그가 하나도 없는 분에 LLM을 부르는 것은 순수한 낭비다 — 새벽처럼
    한산한 시간대에는 대부분의 구간이 비어 있다. 종합 프롬프트도 "로그가
    없던 구간은 목록에 없다"고 알려 주므로, 빠진 구간을 모델이 오해하지
    않는다.
    """
    grouped: dict[datetime, list[LogEntry]] = {}
    for log in state["logs"]:
        minute = log.timestamp.replace(second=0, microsecond=0)
        grouped.setdefault(minute, []).append(log)

    buckets = [
        MinuteBucket(minute=minute, logs=grouped[minute])
        for minute in sorted(grouped)
    ]
    _logger.info(
        "split %d logs into %d non-empty minute buckets",
        len(state["logs"]),
        len(buckets),
    )
    return {"buckets": buckets}


def make_analyze_minute(call_llm: LlmCaller):
    """한 구간을 분석하는 노드를 만든다.

    팬아웃된 각 인스턴스는 ``Send``로 받은 ``MinuteBucket`` 하나만 본다.
    실패해도 예외를 올리지 않고 ``failed=True`` finding을 돌려준다. 구간
    하나가 rate limit에 걸렸다고 나머지 9분의 분석까지 버리는 것은 과하다.
    전부 실패한 경우의 판단은 ``synthesize``가 한다.

    ``call_llm``은 ``response_format``이 걸려 ``MinuteOutput`` JSON을 돌려준다.
    그래도 파싱은 방어적으로 한다 — provider가 바뀌거나 structured output이
    조용히 무시될 수 있고, 그때 구간을 통째로 잃는 것보다 텍스트로라도 파는
    편이 낫다.
    """

    def analyze_minute(bucket: MinuteBucket) -> dict:
        label = bucket.minute.strftime(_MINUTE_FORMAT)
        prompt = build_minute_prompt(bucket.logs, label)
        try:
            text = call_llm(
                [{"role": "user", "content": prompt}], _MINUTE_MAX_TOKENS
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("minute %s analysis failed: %s", label, exc)
            return {
                "findings": [
                    MinuteFinding(
                        minute=bucket.minute,
                        summary=f"이 구간은 분석하지 못했습니다 ({exc}).",
                        failed=True,
                    )
                ]
            }

        try:
            data = json.loads(text)
            summary = (data.get("summary") or "").strip()
            evidence = data.get("evidence") or []
        except (json.JSONDecodeError, AttributeError):
            # JSONDecodeError: 아예 JSON이 아니다.
            # AttributeError: json.loads는 성공했지만 dict가 아니다
            #                 (숫자·문자열 리터럴도 유효한 JSON이다).
            summary, evidence = _parse_minute_response(text)

        # evidence는 모델이 준 JSON에서 온다. 스키마가 list[str]을 강제하지만
        # 그것이 우회되면 dict나 숫자가 섞여 들어올 수 있고, 아래 로깅의
        # "\n".join(...)은 try 밖이라 그대로 터져 구간이 통째로 날아간다.
        evidence = [str(item) for item in evidence]

        _logger.info("minute %s analysis done: %s", label, summary)
        if evidence:
            _logger.info("minute %s evidence:\n%s", label, "\n".join(evidence))
        return {
            "findings": [
                MinuteFinding(
                    minute=bucket.minute, summary=summary, evidence=evidence
                )
            ]
        }

    return analyze_minute


def _parse_minute_response(text: str) -> tuple[str, list[str]]:
    """분별 응답을 요약과 근거로 가른다.

    모델이 형식을 어길 수 있으므로 관대하게 판다. "근거:" 표식을 못 찾으면
    전체를 요약으로 보고 근거는 비운다 — 파싱 실패로 구간을 통째로 잃는
    것보다 요약만이라도 살리는 편이 낫다.
    """
    marker = "근거:"
    head, sep, tail = text.partition(marker)

    summary = head.replace("요약:", "", 1).strip()
    if not sep:
        return text.strip(), []

    evidence = [line.strip() for line in tail.splitlines() if line.strip()]
    return summary or text.strip(), evidence


def make_synthesize(
    call_llm: LlmCaller,
    prompt_builder: Callable = build_synthesis_prompt,
):
    """구간별 결과를 최종 리포트로 합성하는 노드를 만든다.

    prompt_builder를 교체하면 출력 형식을 바꿀 수 있다.
    기본값은 7섹션 진단 리포트.
    """

    def synthesize(state: GraphState) -> dict:
        findings = sorted(state["findings"], key=lambda f: f.minute)
        failed = [f for f in findings if f.failed]

        if findings and len(failed) == len(findings):
            # 전 구간 실패는 부분 손실이 아니라 진단 실패다. "특이사항 없음"
            # 처럼 읽히는 리포트를 돌려주면 운영자가 이상 없음으로 오해한다.
            raise LlmApiError(
                f"모든 구간({len(findings)}개)의 분석이 실패해 리포트를 "
                f"만들 수 없습니다."
            )

        prompt = prompt_builder(
            time_range=state["time_range"],
            minute_sections=_format_findings(findings),
            analyzed=len(findings) - len(failed),
            failed=len(failed),
        )
        return {
            "report": call_llm(
                [{"role": "user", "content": prompt}], _SYNTHESIS_MAX_TOKENS
            )
        }

    return synthesize


def _format_findings(findings: list[MinuteFinding]) -> str:
    if not findings:
        return "(해당 시간 범위에 로그가 없습니다.)"

    blocks = []
    for finding in findings:
        label = finding.minute.strftime(_MINUTE_FORMAT)
        marker = " [분석 실패]" if finding.failed else ""
        block = [f"--- {label}{marker} ---", finding.summary]
        if finding.evidence:
            block.append("근거 로그:")
            block.extend(finding.evidence)
        blocks.append("\n".join(block))
    return "\n\n".join(blocks)
