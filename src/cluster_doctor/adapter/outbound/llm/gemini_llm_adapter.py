import httpx

from cluster_doctor.adapter.outbound.llm.prompt_builder import build_prompt
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.port.outbound.llm_analyzer import LlmAnalyzer


class GeminiApiError(RuntimeError):
    """Gemini returned a non-2xx status.

    Built from the status code alone. ``httpx.HTTPStatusError`` interpolates
    the full request URL into its message, so letting it propagate wrote the
    API key into uvicorn's logs on every 429/403/400.
    """


class GeminiResponseError(RuntimeError):
    """Gemini returned 200 with no usable text.

    Happens when ``maxOutputTokens`` is exhausted by thinking tokens
    (``finishReason: MAX_TOKENS``, ``content`` present but no ``parts``) or
    when the prompt is safety-blocked (``promptFeedback.blockReason``, no
    ``candidates`` at all). Both used to raise an opaque ``KeyError``.
    """


class GeminiLlmAdapter(LlmAnalyzer):
    def __init__(self, api_key: str, base_url: str, default_model: str):
        self._api_key       = api_key
        self._base_url      = base_url
        self._default_model = default_model

    def analyze(self, time_range: TimeRange, logs: list[LogEntry], model: str | None) -> str:
        effective_model = model or self._default_model
        url = f"{self._base_url}/models/{effective_model}:generateContent"
        headers = {"x-goog-api-key": self._api_key}
        payload = {
            "contents": [{"parts": [{"text": build_prompt(time_range, logs)}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 8192},
        }
        with httpx.Client() as client:
            resp = client.post(url, json=payload, headers=headers, timeout=120.0)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise GeminiApiError(
                    f"Gemini API 호출 실패 (HTTP {exc.response.status_code})"
                ) from None
            return _extract_text(resp.json())


def _extract_text(body: dict) -> str:
    candidates = body.get("candidates") or []
    if not candidates:
        block_reason = (body.get("promptFeedback") or {}).get("blockReason", "UNKNOWN")
        raise GeminiResponseError(
            f"Gemini 응답에 candidates가 없습니다 (blockReason={block_reason})"
        )

    candidate = candidates[0]
    parts = (candidate.get("content") or {}).get("parts") or []
    if not parts or "text" not in parts[0]:
        finish_reason = candidate.get("finishReason", "UNKNOWN")
        raise GeminiResponseError(
            f"Gemini 응답에 텍스트가 없습니다 (finishReason={finish_reason})"
        )

    return parts[0]["text"]
