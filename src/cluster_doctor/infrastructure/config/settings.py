from functools import lru_cache
from typing import Literal

from pydantic import ValidationError, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROVIDER_KEY_FIELDS: dict[str, str] = {
    "gemini": "gemini_api_key",
    "nvidia": "nvidia_api_key",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # gemini_api_key/nvidia_api_key default to "" -- without this, pydantic
        # skips validating a field that was never supplied, so an unset key
        # would never reach _require_selected_provider_key below.
        validate_default=True,
    )

    llm_provider: Literal["gemini", "nvidia"] = "gemini"


    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.5-flash"

    nvidia_api_key: str = ""
    nvidia_model: str = "meta/llama-3.3-70b-instruct"

    clickhouse_url: str
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_slowlog_table: str = "slowlog_v2"
    clickhouse_log_table: str = "log"
    clickhouse_node_metric_table: str = "es_node_metric"

    @field_validator("gemini_api_key", "nvidia_api_key")
    @classmethod
    def _require_selected_provider_key(cls, v: str, info: ValidationInfo) -> str:
        """선택된 provider의 키가 비어 있으면 거부한다.

        의도적으로 필드 단위(field_validator) 검증이다: 모델 단위
        (model_validator(mode="after"))로 짰을 때는 값이 아니라 필드
        이름만 언급해도 pydantic이 ValidationError.__str__ 안에
        input_value=<모델 전체 dict>를 그대로 붙여 다른 비밀값까지
        새어나갔다(직접 확인함). 필드 단위 검증기는 실패한 그 필드
        자신의 스칼라 값(빈 문자열)만 input_value로 싣기 때문에 다른
        필드의 비밀값이 메시지에 실리지 않는다.
        """
        provider = info.data.get("llm_provider")
        if (
            provider is not None
            and _PROVIDER_KEY_FIELDS.get(provider) == info.field_name
            and not v
        ):
            raise ValueError(
                f"{info.field_name} is required when llm_provider={provider!r}"
            )
        return v


class ConfigurationError(RuntimeError):
    """Startup configuration is missing or invalid.

    Carries the *names* of the offending settings and nothing else.
    ``pydantic.ValidationError`` cannot be used for this: it embeds
    ``input_value={...}`` -- the entire assembled settings dict, secrets
    included -- in its message, and a lifespan failure's traceback is written
    verbatim to uvicorn's stderr.
    """


@lru_cache
def get_settings() -> Settings:
    """Build settings once, converting a validation failure into a value-free error.

    The guard lives here rather than at the boot call site so that *every*
    path to the settings is covered. It used to sit in ``main.py``'s
    lifespan; anything that reached for ``Settings()`` another way -- a
    script, a test helper, a future request-path caller -- bypassed it and
    got the raw, secret-bearing ``ValidationError`` back. That happened for
    real during manual verification and printed part of an API key.

    ``exc.errors(include_input=False, include_url=False)`` drops the
    ``input_value`` payload entirely; the message is then assembled from the
    ``loc`` entries alone, so it can only ever contain field names that are
    already declared in ``Settings``. ``msg``/``type`` are deliberately left
    out too -- naming which setting is at fault is what an operator needs,
    and it keeps this immune to any pydantic error variant that renders part
    of the offending value into its own text.

    ``from None`` suppresses the cause chain: without it the original,
    value-bearing ``ValidationError`` is re-printed under "The above
    exception was the direct cause of..." and the leak survives the fix.

    The message is deliberately ASCII, unlike the Korean domain-error
    messages. Those reach callers as UTF-8 JSON bodies; this one is printed
    by uvicorn into a raw byte stream that Python encodes with the *OS locale*
    (cp949 on a Korean Windows host), so Korean text here arrives as invalid
    UTF-8 in journald/Docker/Kubernetes -- mojibake in the one message whose
    whole job is telling an operator what to fix.

    ``lru_cache`` does not memoize exceptions, so a failing configuration is
    re-validated (and re-raised) on every call rather than being cached.
    """
    try:
        return Settings()
    except ValidationError as exc:
        fields = ", ".join(
            ".".join(str(part) for part in error["loc"]) or "(root)"
            for error in exc.errors(include_input=False, include_url=False)
        )
        raise ConfigurationError(
            f"missing or invalid configuration: {fields}"
        ) from None
