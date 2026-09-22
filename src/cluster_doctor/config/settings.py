from functools import lru_cache
from typing import Literal, get_args

from pydantic import ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LlmProvider = Literal["gemini", "nvidia_nim"]
_SUPPORTED_LLM_PROVIDERS: tuple[str, ...] = get_args(LlmProvider)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        validate_default=True,
    )

    llm_provider: LlmProvider = "nvidia_nim"

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
    clickhouse_node_log_table: str = "loki_logs"

    node_heap_warn_percent: int = 85
    node_queue_warn: int = 100

    cluster_name: str = "elasticsearch"

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

    micro_batch_seconds: float = 10.0

    report_dir: str = "reports"

    @field_validator("gemini_api_key")
    @classmethod
    def _require_gemini_key_when_selected(cls, v: str, info) -> str:
        if info.data.get("llm_provider") == "gemini" and not v:
            raise ValueError("gemini_api_key is required when llm_provider=gemini")
        return v

    @field_validator("nvidia_api_key")
    @classmethod
    def _require_nvidia_key_when_selected(cls, v: str, info) -> str:
        if info.data.get("llm_provider") == "nvidia_nim" and not v:
            raise ValueError("nvidia_api_key is required when llm_provider=nvidia_nim")
        return v

    @field_validator("cluster_name")
    @classmethod
    def _require_cluster_name(cls, v: str) -> str:
        # 빈 이름은 리포트와 로그에서 어느 클러스터를 본 것인지 지운다.
        # 런타임이 아니라 여기서 막는 이유: 값을 고칠 수 있는 것은 운영자이고,
        # Incident 한 건을 태운 뒤에 알게 되는 것보다 기동이 실패하는 편이 낫다.
        if not v.strip():
            raise ValueError("cluster_name is required")
        return v

    @field_validator("es_host")
    @classmethod
    def _require_es_host(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("es_host is required")
        return v

    @property
    def llm_api_key(self) -> str:
        return {
            "gemini": self.gemini_api_key,
            "nvidia_nim": self.nvidia_api_key,
        }[self.llm_provider]

    @property
    def llm_model(self) -> str:
        return {
            "gemini": self.gemini_model,
            "nvidia_nim": self.nvidia_model,
        }[self.llm_provider]


class ConfigurationError(RuntimeError):
    pass


@lru_cache
def get_settings() -> Settings:
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
