"""Analysis 호출과 Main 조립이 사용하는 litellm 왕복 1회.

호출부를 복붙하면 아래 세 가지 보호 장치가 두 갈래로 갈라지고, 한쪽만
고쳐지는 사고가 난다:

1. 키는 파라미터로만 전달한다 (모델 문자열·URL에 실리지 않는다)
2. provider 예외는 상태 코드만 남기고 체이닝을 끊는다
3. 텍스트 없는 200 응답을 ``LlmResponseError``로 바꾼다
4. IP·이메일·회사·사용자 같은 식별 값은 가명으로 보내고 응답에서 되돌린다
"""

import logging
import os
import re
import threading
import time

# LITELLM_LOCAL_MODEL_COST_MAP은 litellm이 import 시점에 읽는다. import 전에
# 설정해야 효과가 있으므로 아래 import 순서는 의도적이다 (E402).
# 이 값이 없으면 litellm이 import 때 GitHub에서 비용 데이터를 받아온다.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

import litellm  # noqa: E402
import openai  # noqa: E402

from cluster_doctor.exceptions import (  # noqa: E402
    LlmApiError,
    LlmResponseError,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.llm_call_log import (  # noqa: E402
    LlmCallTrace,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import (  # noqa: E402
    PSEUDONYMS,
    mask_messages,
    pseudonym_scope,
)

litellm.suppress_debug_info = True

_logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 1200.0

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

# 분당 요청 수(RPM) 상한. provider마다 다르고, 없는 provider는 거르지 않는다
# (nvidia_nim은 실측 실패가 항상 504였지 429가 아니었다). gemini는 무료 티어
# 한도가 15 RPM인데, minute_analysis의 map 단계가 MAX_CONCURRENCY=5로 팬아웃
# 하므로(graph.py) 동시에 여러 스레드가 이 함수를 두드린다. 스레드마다 따로
# sleep을 넣어도 동시에 깨어나면 순간적으로 한도를 넘기므로, 락으로 감싼
# 공유 최소 호출 간격으로 건다 — ``complete()``를 거치는 호출은 여기 한 곳만
# 지키면 호출부 동시성 설정과 무관하게 지켜진다. DeepAgent(ChatLiteLLM) 호출은
# 이 간격을 거치지 않는다.
_MIN_CALL_INTERVAL_SECONDS: dict[str, float] = {
    "gemini": 5.0,  # 12 RPM. 무료 티어 15 RPM에 안전마진을 둔 값.
}
_rate_limit_lock = threading.Lock()
_last_call_at: dict[str, float] = {}


def _throttle(provider: str) -> None:
    interval = _MIN_CALL_INTERVAL_SECONDS.get(provider)
    if interval is None:
        return
    with _rate_limit_lock:
        now = time.monotonic()
        wait = _last_call_at.get(provider, 0.0) + interval - now
        if wait > 0:
            time.sleep(wait)
        _last_call_at[provider] = time.monotonic()


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
        "LLM이 모델의 최대 출력 길이에 도달해 응답이 잘렸습니다. "
        "같은 내용을 반복해 출력하고 있을 수 있으니 응답 내용을 확인하세요."
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


# 재시도는 429와 503, 두 경우뿐이다. 분당 토큰 한도 429는 provider가 알려 준
# 대기 시간(retryDelay)을 기다린 뒤에만 재시도한다 — 바로 다시 보내면 실패가
# 보장된 채 소비만 늘기 때문이다. 대기 시간을 모르면 재시도하지 않는다.
_RETRYABLE_STATUS = frozenset({429, 503})
_MAX_RETRIES = 1
# provider가 대기 시간을 알려 주지 않을 때의 대기. 시도 횟수 순서대로 쓴다.
_FALLBACK_BACKOFF_SECONDS = (5.0, 15.0)
# provider가 알려 준 대기가 이보다 길면 기다리지 않고 실패시킨다. 일 단위
# 한도처럼 분석 한 건이 기다릴 수 없는 경우다.
_MAX_RETRY_WAIT_SECONDS = 60.0
_RETRY_DELAY_MARGIN_SECONDS = 1.0
_RETRY_DELAY_PATTERN = re.compile(r'"retryDelay"\s*:\s*"(\d+(?:\.\d+)?)s"')
_TOKEN_QUOTA_PATTERN = re.compile(r'"quotaId"\s*:\s*"[^"]*Token', re.IGNORECASE)


def _retry_wait(exc: Exception, status, attempt: int) -> float | None:
    """재시도할 가치가 있으면 대기 초를, 없으면 ``None``을 돌려준다.

    ``str(exc)``는 여기서 값을 파싱하는 데만 쓰고 로그나 예외 메시지에 싣지
    않는다 — 그 본문에 요청 URL이 들어올 수 있다.
    """
    if status not in _RETRYABLE_STATUS:
        return None

    text = str(exc)
    token_quota = status == 429 and bool(_TOKEN_QUOTA_PATTERN.search(text))

    headers = getattr(getattr(exc, "response", None), "headers", None) or {}
    delay: float | None = None
    header_value = headers.get("retry-after")
    if header_value:
        try:
            delay = float(header_value)
        except ValueError:
            delay = None
    if delay is None:
        match = _RETRY_DELAY_PATTERN.search(text)
        if match:
            delay = float(match.group(1))

    if delay is None:
        if token_quota:
            return None
        return _FALLBACK_BACKOFF_SECONDS[min(attempt, len(_FALLBACK_BACKOFF_SECONDS) - 1)]
    if delay > _MAX_RETRY_WAIT_SECONDS:
        return None
    return delay + _RETRY_DELAY_MARGIN_SECONDS


@pseudonym_scope()
def complete(
    messages: list[dict],
    provider: str,
    model: str,
    api_key: str,
    response_format=None,
) -> str:
    """LLM에 한 번 물어보고 텍스트를 받는다.

    ``max_tokens``는 보내지 않는다 — provider가 스스로 정한 상한(모델의 실제 최대
    출력 토큰)을 그대로 쓴다. 구조화된 JSON을 통째로 돌려받아야 하는 호출에서
    우리가 임의로 건 한도 때문에 응답이 문자열 중간에서 잘려 파싱 자체가 실패하는
    사고가 있었다. 나머지 파라미터도 provider가 바뀌어도 같아야 하므로 고정한다.
    """
    prefix = _PROVIDER_PREFIX[provider]

    kwargs: dict = {
        "model": f"{prefix}/{model}",
        # IP는 가명으로 보내고 응답에서 되돌린다(pseudonym.py).
        "messages": mask_messages(messages),
        "api_key": api_key,
        # temperature를 비롯한 샘플링 인자는 보내지 않는다. 명시하지 않으면
        # provider 기본값이 적용되고, 모델을 바꿀 때 그 모델에 맞는 기본값을
        # 그대로 따라간다.
        "timeout": _REQUEST_TIMEOUT_SECONDS,
        # litellm 자체 재시도는 끈다. 이 경로의 실패는 대부분 429이고, 그것은
        # 분당 입력 토큰 한도 초과가 원인이다. 같은 프롬프트를 다시
        # 보내면 실패가 보장된 채 소비만 배로 늘어난다(실측: 513,122
        # 토큰 → 재시도 포함 2,052,488 토큰, 한도 250,000의 821%).
        # litellm.completion()은 오류 종류별 재시도 정책을 받지 않으므로
        # 일시적 오류까지 함께 포기한다 — 분 하나가 실패해도 분석
        # 전체는 살아남게 되어 있어(MinuteResult.failed=True) 감당된다.
        # 재시도가 필요한 오류만 아래 루프가 ``_retry_wait``로 가려 직접 한다.
        "num_retries": 0,
    }
    if response_format is not None:
        kwargs["response_format"] = response_format

    for attempt in range(_MAX_RETRIES + 1):
        try:
            _throttle(provider)
            trace = LlmCallTrace("complete", model)
            response = litellm.completion(**kwargs)
            break
        except openai.APIError as exc:
            # openai.APIError를 잡는 것이 맞다. litellm.exceptions.APIError는
            # RateLimitError/AuthenticationError/BadRequestError/
            # InternalServerError를 하나도 잡지 못한다 — 그 구체 예외들은
            # litellm의 동명 APIError가 아니라 openai의 APIError를 상속한다.
            status = getattr(exc, "status_code", None) or "unknown"
            trace.failed(f"status={status}")
            wait = _retry_wait(exc, status, attempt) if attempt < _MAX_RETRIES else None
            if wait is not None:
                _logger.warning(
                    "LLM provider=%s rejected the request with status=%s; "
                    "retrying in %.0fs (%d/%d)",
                    provider,
                    status,
                    wait,
                    attempt + 1,
                    _MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            # exc를 체이닝하지 않는다(from None). litellm 예외 메시지에는
            # provider가 돌려준 본문이 실릴 수 있고, 그 안에 요청 URL이
            # 들어올 수 있다.
            #
            # 예외 메시지에는 상태 코드만 담기므로, 원인 구분에 필요한 값은
            # 여기서 로그로 남긴다. 던지기 전에 부르는 것이 중요하다 —
            # 호출자가 이 예외를 삼키더라도 진단 흔적은 남는다.
            _log_provider_error(provider, status, exc)
            raise LlmApiError(
                f"LLM provider({provider}) 호출이 실패했습니다 (status={status})"
            ) from None

    choice = response.choices[0]
    usage = getattr(response, "usage", None)
    trace.done(
        prompt_tokens=getattr(usage, "prompt_tokens", None),
        completion_tokens=getattr(usage, "completion_tokens", None),
        finish_reason=choice.finish_reason,
    )
    text = choice.message.content
    if not text:
        reason = choice.finish_reason or "unknown"
        guidance = _EMPTY_RESPONSE_GUIDANCE.get(
            reason, _UNKNOWN_EMPTY_RESPONSE.format(reason=reason)
        )
        raise LlmResponseError(guidance)
    return PSEUDONYMS.restore(text)
