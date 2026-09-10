from functools import lru_cache

from pydantic import ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# 지원하는 LLM provider. litellm_client._PROVIDER_PREFIX와 같은 집합이어야
# 한다 — 여기서 통과한 값이 그쪽 조회 키로 그대로 쓰인다.
_SUPPORTED_LLM_PROVIDERS = ("gemini", "nvidia_nim")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # validate_default=True: API 키 기본값("")도 검증을 거치게 한다.
        # 이 값이 없으면 pydantic이 기본값 필드를 건너뛰어
        # 키 검증기가 빈 키를 허용하게 된다.
        validate_default=True,
    )

    # provider 선택이 키 필드보다 먼저 선언돼야 한다. 아래 키 검증기들이
    # info.data["llm_provider"]를 읽는데, pydantic v2의 field_validator는
    # 자기보다 앞서 선언된 필드만 info.data에서 볼 수 있다.
    llm_provider: str = "gemini"

    gemini_api_key: str = ""
    gemini_model: str = "gemini-3.5-flash-lite"

    nvidia_api_key: str = ""
    nvidia_model: str = "google/gemma-4-31b-it"

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

    ssh_user: str = ""
    ssh_password: str = ""
    ssh_port: int = 22

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "slowlog"
    kafka_group_id: str = "clusterdoctor"

    # 첫 slowlog 수신 후 agent를 띄우기까지 모으는 시간. slowlog는 몰려서
    # 오므로 한 건마다 진단하면 같은 사고를 수십 번 분석하게 된다.
    micro_batch_seconds: float = 10.0

    @field_validator("llm_provider")
    @classmethod
    def _require_known_provider(cls, v: str) -> str:
        """알 수 없는 provider를 첫 호출까지 끌고 가지 않는다.

        아래 두 키 검증기와 llm_api_key/llm_model 프로퍼티가 이 값을 조회 키로
        쓰므로, 여기서 막지 않으면 오타가 KeyError로 늦게 터진다.
        """
        if v not in _SUPPORTED_LLM_PROVIDERS:
            supported = ", ".join(_SUPPORTED_LLM_PROVIDERS)
            raise ValueError(f"llm_provider must be one of: {supported}")
        return v

    @field_validator("gemini_api_key")
    @classmethod
    def _require_gemini_key_when_selected(cls, v: str, info) -> str:
        """선택된 provider의 키만 요구한다.

        예전에는 provider와 무관하게 Gemini 키를 요구했다. 쓰지 않는 키를
        강제하는 한편, 정작 쓰는 키가 비어 있으면 통과시켜 진단 요청 한 건을
        통째로 날린 뒤에야 알게 됐다.

        필드 단위 검증기를 쓰는 이유는 그대로다: model_validator(mode="after")는
        ValidationError.__str__ 안에 input_value=<모델 전체 dict>를 담아
        다른 비밀값(clickhouse_password 등)이 에러 메시지에 노출된다.
        필드 단위 검증기는 실패한 필드 자신의 값만 싣는다.
        """
        if info.data.get("llm_provider") == "gemini" and not v:
            raise ValueError("gemini_api_key is required when llm_provider=gemini")
        return v

    @field_validator("nvidia_api_key")
    @classmethod
    def _require_nvidia_key_when_selected(cls, v: str, info) -> str:
        """nvidia_nim을 골랐으면 그 키를 요구한다."""
        if info.data.get("llm_provider") == "nvidia_nim" and not v:
            raise ValueError("nvidia_api_key is required when llm_provider=nvidia_nim")
        return v

    @field_validator("es_host")
    @classmethod
    def _require_es_host(cls, v: str) -> str:
        """es_host가 비어 있으면 거부한다.

        agent의 첫 진단 단계가 cluster_health()이므로 ES는 필수다. 비워 두면
        _get_es_client가 hosts=[]로 Elasticsearch를 만들다 ValueError를 던지는데,
        그 메시지는 어떤 설정이 문제인지 알려주지 않는다. 필드 단위 검증기로
        올려 ConfigurationError가 이름을 짚어 주게 한다.
        """
        if not v.strip():
            raise ValueError("es_host is required")
        return v

    @property
    def llm_api_key(self) -> str:
        """선택된 provider의 API 키.

        호출부가 provider를 분기하지 않게 한다. 예전에는 dependencies가
        gemini_api_key를 직접 읽어, .env에 NVIDIA 설정을 넣어도 Gemini로 갔다.

        dict 조회가 KeyError를 낼 수 없다 — _require_known_provider가
        통과시킨 값만 여기 도달한다.
        """
        return {
            "gemini": self.gemini_api_key,
            "nvidia_nim": self.nvidia_api_key,
        }[self.llm_provider]

    @property
    def llm_model(self) -> str:
        """선택된 provider의 모델명."""
        return {
            "gemini": self.gemini_model,
            "nvidia_nim": self.nvidia_model,
        }[self.llm_provider]


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
