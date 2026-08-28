from functools import lru_cache

from pydantic import ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # validate_default=True: gemini_api_key 기본값("")도 검증을 거치게 한다.
        # 이 값이 없으면 pydantic이 기본값 필드를 건너뛰어
        # _require_gemini_key가 빈 키를 허용하게 된다.
        validate_default=True,
    )

    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.5-flash"

    clickhouse_url: str
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_slowlog_table: str = "slowlog_v2"
    clickhouse_log_table: str = "log"
    clickhouse_node_metric_table: str = "es_node_metric"

    es_host: str = ""
    es_port: int = 9200
    es_user: str = ""
    es_password: str = ""

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "slowlog"
    kafka_group_id: str = "clusterdoctor"

    @field_validator("gemini_api_key")
    @classmethod
    def _require_gemini_key(cls, v: str) -> str:
        """gemini_api_key가 비어 있으면 거부한다.

        필드 단위 검증기를 쓰는 이유: model_validator(mode="after")는
        ValidationError.__str__ 안에 input_value=<모델 전체 dict>를 담아
        다른 비밀값(clickhouse_password 등)이 에러 메시지에 노출된다.
        필드 단위 검증기는 실패한 필드 자신의 값만 싣는다.
        """
        if not v:
            raise ValueError("gemini_api_key is required")
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
