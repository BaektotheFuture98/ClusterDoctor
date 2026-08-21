"""litellm 기반 LLM 어댑터. Gemini와 NVIDIA Build를 함께 지원한다."""

import os

# LITELLM_LOCAL_MODEL_COST_MAP은 litellm이 import 시점에 읽는다. import 전에
# 설정해야 효과가 있으므로 아래 import 순서는 의도적이다 (E402).
# 이 값이 없으면 litellm이 import 때 GitHub에서 비용 데이터를 받아온다.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

import litellm  # noqa: E402
import openai  # noqa: E402

from cluster_doctor.adapter.outbound.llm.prompt_builder import build_prompt  # noqa: E402
from cluster_doctor.domain.model.log_entry import LogEntry  # noqa: E402
from cluster_doctor.domain.model.time_range import TimeRange  # noqa: E402
from cluster_doctor.domain.port.outbound.llm_analyzer import (  # noqa: E402
    LlmAnalyzer,
    LlmApiError,
    LlmResponseError,
)

litellm.suppress_debug_info = True

_TEMPERATURE = 0.2
_MAX_OUTPUT_TOKENS = 8192
_REQUEST_TIMEOUT_SECONDS = 120.0

# 실측 확인된 litellm provider 식별자. nvidia_nim의 기본 api_base는
# https://integrate.api.nvidia.com/v1 로 litellm 내부에 하드코딩돼 있다.
_PROVIDER_PREFIX: dict[str, str] = {
    "gemini": "gemini",
    "nvidia": "nvidia_nim",
}

# 응답에 텍스트가 없을 때의 안내. litellm은 provider의 원본 사유 문자열을
# 보존하지 않고 OpenAI의 일반 사유로 정규화하므로, Gemini의
# SAFETY/RECITATION/BLOCKLIST/PROHIBITED_CONTENT/SPII 는 모두
# content_filter 하나로 도착한다. 따라서 가능한 원인을 전부 열거한다.
_EMPTY_RESPONSE_GUIDANCE: dict[str, str] = {
    "length": (
        "LLM이 토큰 한도에 도달해 응답이 잘렸습니다. "
        "진단 시간 범위를 좁히거나 로그량을 줄여 다시 시도하세요."
    ),
    "content_filter": (
        "LLM 정책 필터가 응답을 차단했습니다. 로그에 개인정보(PII), "
        "금지 용어, 저작권 인용으로 오판될 내용이 포함됐을 수 있습니다. "
        "해당 시간대의 로그 내용을 확인하세요."
    ),
}
_UNKNOWN_EMPTY_RESPONSE = (
    "LLM이 응답에 텍스트를 담지 않았습니다 (finish_reason={reason})."
)


class LiteLlmAdapter(LlmAnalyzer):
    def __init__(self, provider: str, api_key: str, default_model: str):
        if provider not in _PROVIDER_PREFIX:
            supported = ", ".join(sorted(_PROVIDER_PREFIX))
            raise ValueError(
                f"지원하지 않는 provider={provider!r} (지원: {supported})"
            )
        self._provider = provider
        self._api_key = api_key
        self._default_model = default_model

    def analyze(
        self, time_range: TimeRange, logs: list[LogEntry], model: str | None
    ) -> str:
        prefix = _PROVIDER_PREFIX[self._provider]
        effective_model = model or self._default_model

        try:
            response = litellm.completion(
                model=f"{prefix}/{effective_model}",
                messages=[
                    {"role": "user", "content": build_prompt(time_range, logs)}
                ],
                api_key=self._api_key,
                temperature=_TEMPERATURE,
                max_tokens=_MAX_OUTPUT_TOKENS,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
        except openai.APIError as exc:
            # openai.APIError를 잡는 것이 맞다. litellm.exceptions.APIError는
            # RateLimitError/AuthenticationError/BadRequestError/
            # InternalServerError를 하나도 잡지 못한다 — 그 구체 예외들은
            # litellm의 동명 APIError가 아니라 openai의 APIError를 상속한다.
            #
            # exc를 체이닝하지 않는다(from None). litellm 예외 메시지에는
            # provider가 돌려준 본문이 실릴 수 있고, 그 안에 요청 URL이
            # 들어올 수 있다.
            status = getattr(exc, "status_code", None) or "unknown"
            raise LlmApiError(
                f"LLM provider({self._provider}) 호출이 실패했습니다 "
                f"(status={status})"
            ) from None

        choice = response.choices[0]
        text = choice.message.content
        if not text:
            reason = choice.finish_reason or "unknown"
            guidance = _EMPTY_RESPONSE_GUIDANCE.get(
                reason, _UNKNOWN_EMPTY_RESPONSE.format(reason=reason)
            )
            raise LlmResponseError(guidance)
        return text
