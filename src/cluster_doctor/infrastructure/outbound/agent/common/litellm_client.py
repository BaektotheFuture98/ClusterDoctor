"""litellm 왕복 1회. provider 선택·키 취급·응답 가드를 여기에만 둔다.

호출부를 복붙하면 아래 세 가지 보호 장치가 두 갈래로 갈라지고, 한쪽만
고쳐지는 사고가 난다:

1. 키는 파라미터로만 전달한다 (모델 문자열·URL에 실리지 않는다)
2. provider 예외는 상태 코드만 남기고 체이닝을 끊는다
3. 텍스트 없는 200 응답을 ``LlmResponseError``로 바꾼다
"""

import logging
import os

# LITELLM_LOCAL_MODEL_COST_MAP은 litellm이 import 시점에 읽는다. import 전에
# 설정해야 효과가 있으므로 아래 import 순서는 의도적이다 (E402).
# 이 값이 없으면 litellm이 import 때 GitHub에서 비용 데이터를 받아온다.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

import litellm  # noqa: E402
import openai  # noqa: E402

from cluster_doctor.application.exception import (  # noqa: E402
    LlmApiError,
    LlmResponseError,
)

litellm.suppress_debug_info = True

_logger = logging.getLogger(__name__)

_MAX_OUTPUT_TOKENS = 8192
_REQUEST_TIMEOUT_SECONDS = 120.0

# 429 진단용으로 남길 응답 헤더. 화이트리스트인 것이 핵심이다 —
# 응답 본문·요청 URL·str(exc)는 어떤 경우에도 로그에 넣지 않는다. provider에
# 따라 요청 URL에 API 키가 실리고, 본문에는 provider 내부 정보가 실린다.
# 그 원칙 때문에 LlmApiError는 상태 코드만 담는데, 그 결과 429가 났을 때
# "요청 수 한도인지 토큰 한도인지"를 알 길이 사라졌다. 아래 값들이 그 구분을
# 알려 주는 표준 헤더이고, 키나 URL을 담지 않는다.
_RATE_LIMIT_HEADERS = (
    "retry-after",
    "x-ratelimit-limit-requests",
    "x-ratelimit-remaining-requests",
    "x-ratelimit-reset-requests",
    "x-ratelimit-limit-tokens",
    "x-ratelimit-remaining-tokens",
    "x-ratelimit-reset-tokens",
)

_PROVIDER_PREFIX: dict[str, str] = {
    "gemini": "gemini",
    # NVIDIA NIM. settings._SUPPORTED_LLM_PROVIDERS와 같은 집합이어야 한다.
    #
    # 두 가지를 알아 둘 것:
    # 1. google/gemma-4-31b-it는 litellm 로컬 cost map에 항목이 없다. 라우팅은
    #    이 prefix로 결정되므로 호출은 정상이지만 비용 추적은 0으로 잡힌다.
    # 2. 이 provider는 reasoning_effort를 지원하지 않는다(litellm의
    #    get_supported_openai_params 지원 목록에 없다). thinking으로 추론
    #    깊이를 올리는 길이 없어, 추론은 프롬프트로 유도한다 —
    #    구획 CoT 프롬프트를 볼 것.
    "nvidia_nim": "nvidia_nim",
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


def _log_provider_error(provider: str, status, exc: Exception) -> None:
    """provider 거절의 원인을 알 수 있는 값만 골라 남긴다.

    429는 원인이 둘이다 — 분당 요청 수(RPM) 초과와 분당 입력 토큰 수(TPM)
    초과. 대응이 서로 다른데(앞은 호출 간격, 뒤는 프롬프트 크기) 상태 코드만
    보면 구별할 수 없다. ``x-ratelimit-*`` 헤더가 그 구분을 알려 주므로
    화이트리스트로 뽑아 남긴다.

    ``exc``에서 꺼내는 것은 화이트리스트 헤더와 짧은 기계 코드(``code``,
    ``type``)뿐이다. ``str(exc)``·``exc.body``·``exc.request.url``은 넣지
    않는다 — 그것들이 새는 것을 막는 것이 이 모듈 전체의 전제다.

    메시지가 ASCII인 이유는 ClickHouse 어댑터의 절단 경고와 같다. root
    ``StreamHandler``가 ``sys.stderr``에 쓰고 Python이 OS 로케일로 인코딩하므로
    (한국어 Windows에서 cp949) 한국어를 쓰면 UTF-8 수집기에서 깨진다.

    로깅이 실패해도 호출자의 예외 변환을 막지 않는다. 진단용 부가 정보가
    본래 오류를 가리는 것은 뒤바뀐 우선순위다.
    """
    try:
        details = []
        for attr in ("code", "type"):
            value = getattr(exc, attr, None)
            if value:
                details.append(f"{attr}={value}")

        headers = getattr(getattr(exc, "response", None), "headers", None) or {}
        for name in _RATE_LIMIT_HEADERS:
            value = headers.get(name)
            if value:
                details.append(f"{name}={value}")

        _logger.warning(
            "LLM provider=%s rejected the request with status=%s; %s",
            provider,
            status,
            " ".join(details) if details else "no rate-limit headers were returned",
        )
    except Exception:  # noqa: BLE001
        _logger.warning(
            "LLM provider=%s rejected the request with status=%s; "
            "could not read rate-limit details",
            provider,
            status,
        )


def require_supported_provider(provider: str) -> str:
    """지원 provider인지 확인하고 그대로 돌려준다.

    생성자에서 부르라고 만든 것이다. 잘못된 provider를 첫 호출 때까지
    끌고 가면, 진단 요청 한 건을 통째로 날린 뒤에야 오타를 알게 된다.
    """
    if provider not in _PROVIDER_PREFIX:
        supported = ", ".join(sorted(_PROVIDER_PREFIX))
        raise ValueError(f"지원하지 않는 provider={provider!r} (지원: {supported})")
    return provider


def complete(
    messages: list[dict],
    provider: str,
    model: str,
    api_key: str,
    max_tokens: int = _MAX_OUTPUT_TOKENS,
    response_format=None,
) -> str:
    """LLM에 한 번 물어보고 텍스트를 받는다.

    ``max_tokens``만 호출자가 조절할 수 있게 열어 뒀다. 분별 분석은 짧은
    요약이면 충분하고, 한도를 낮추면 ``finish_reason="length"``로 잘릴 일도
    줄기 때문이다. 나머지 파라미터는 provider가 바뀌어도 같아야 하므로
    고정한다.
    """
    prefix = _PROVIDER_PREFIX[provider]

    try:
        kwargs: dict = {
            "model": f"{prefix}/{model}",
            "messages": messages,
            "api_key": api_key,
            # temperature를 비롯한 샘플링 인자는 보내지 않는다. 명시하지 않으면
            # provider 기본값이 적용되고, 모델을 바꿀 때 그 모델에 맞는 기본값을
            # 그대로 따라간다.
            "max_tokens": max_tokens,
            "timeout": _REQUEST_TIMEOUT_SECONDS,
            # 재시도하지 않는다. 이 경로의 실패는 대부분 429이고, 그것은
            # 분당 입력 토큰 한도 초과가 원인이다. 같은 프롬프트를 다시
            # 보내면 실패가 보장된 채 소비만 배로 늘어난다(실측: 513,122
            # 토큰 → 재시도 포함 2,052,488 토큰, 한도 250,000의 821%).
            # litellm.completion()은 오류 종류별 재시도 정책을 받지 않으므로
            # 일시적 오류까지 함께 포기한다 — 분 하나가 실패해도 분석
            # 전체는 살아남게 되어 있어(Triage의 failed=True) 감당된다.
            "num_retries": 0,
        }
        if response_format is not None:
            kwargs["response_format"] = response_format
        response = litellm.completion(**kwargs)
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
        # 예외 메시지에는 상태 코드만 담기므로, 원인 구분에 필요한 값은
        # 여기서 로그로 남긴다. 던지기 전에 부르는 것이 중요하다 —
        # 호출자가 이 예외를 삼키더라도 진단 흔적은 남는다.
        _log_provider_error(provider, status, exc)
        raise LlmApiError(
            f"LLM provider({provider}) 호출이 실패했습니다 (status={status})"
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
