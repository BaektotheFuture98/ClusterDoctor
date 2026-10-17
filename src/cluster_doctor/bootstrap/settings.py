from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LlmProvider = Literal["gemini", "nvidia_nim"]

_DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024
_DEFAULT_LOG_BACKUP_COUNT = 5


class LoggingSettings(BaseSettings):
    """외부 서비스 설정을 검증하기 전에 로그 설정을 먼저 읽는다."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        validate_default=True,
    )

    log_dir: str = "logs"
    # app.log를 이 크기에서 넘기고 백업을 이 개수까지만 남긴다. 총량은 대략
    # (backup_count + 1) * max_bytes다.
    log_max_bytes: int = Field(default=_DEFAULT_LOG_MAX_BYTES, gt=0)
    log_backup_count: int = Field(default=_DEFAULT_LOG_BACKUP_COUNT, gt=0)

    @field_validator("log_dir")
    @classmethod
    def _default_empty_log_dir(cls, v: str) -> str:
        return v if v.strip() else "logs"

    @field_validator("log_max_bytes", "log_backup_count", mode="before")
    @classmethod
    def _default_empty_rotation_value(cls, v, info):
        if isinstance(v, str) and not v.strip():
            return _DEFAULT_LOG_MAX_BYTES if info.field_name == "log_max_bytes" else _DEFAULT_LOG_BACKUP_COUNT
        return v


class Settings(LoggingSettings):
    """환경 변수와 .env에서 읽는 프로세스 기동 설정.

    조립 코드가 어댑터별 설정으로 나누며, 도메인 상태나 분석 결과는 담지 않는다.
    """

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

    # 분 단위 선별 그래프 하나가 동시에 돌리는 분의 수. 데이터소스마다 그래프가
    # 따로 돌므로 전체 동시 작업 수는 이 값의 몇 배가 될 수 있다.
    analysis_concurrency: int = Field(default=15, gt=0)

    @field_validator("analysis_concurrency", mode="before")
    @classmethod
    def _default_empty_analysis_concurrency(cls, v):
        if isinstance(v, str) and not v.strip():
            return 15
        return v

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
    kafka_failure_timeout_seconds: float = Field(default=300.0, gt=0)

    report_dir: str = "reports"

    @field_validator("report_dir")
    @classmethod
    def _default_empty_report_dir(cls, v: str) -> str:
        return v if v.strip() else "reports"

    # 리포트를 SFTP로 다른 서버에 올린다. REPORT_SFTP_HOST가 비어 있으면 꺼진다.
    report_sftp_host: str = ""
    report_sftp_port: int = Field(default=22, gt=0, le=65535)
    report_sftp_user: str = ""
    report_sftp_password: SecretStr = SecretStr("")
    report_sftp_key_file: str = ""
    report_sftp_remote_dir: str = ""
    report_sftp_known_hosts: str = ""
    report_sftp_timeout_seconds: float = Field(default=10.0, gt=0)

    @field_validator("report_sftp_port", "report_sftp_timeout_seconds", mode="before")
    @classmethod
    def _default_empty_sftp_number(cls, v, info):
        if isinstance(v, str) and not v.strip():
            return 22 if info.field_name == "report_sftp_port" else 10.0
        return v

    @model_validator(mode="after")
    def _require_sftp_connection_details(self) -> "Settings":
        # 켜져 있는데 값이 모자라면 사건 한 건을 태운 뒤가 아니라 기동에서 멈춘다.
        # 오류 문구에는 필드 이름만 싣고 값은 싣지 않는다.
        if not self.report_sftp_host.strip():
            return self
        missing = [
            name
            for name, value in (
                ("report_sftp_user", self.report_sftp_user),
                ("report_sftp_remote_dir", self.report_sftp_remote_dir),
            )
            if not value.strip()
        ]
        if not (self.report_sftp_password.get_secret_value() or self.report_sftp_key_file.strip()):
            missing.append("report_sftp_password")
        if missing:
            raise ValueError(
                "report_sftp_host is set but these are missing: "
                + ", ".join(missing)
                + " (report_sftp_password or report_sftp_key_file is required)"
            )
        return self

    @property
    def report_sftp_enabled(self) -> bool:
        return bool(self.report_sftp_host.strip())

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
        # 모델 단위 검증 오류는 loc이 비어 있으므로 문구(값을 담지 않는다)를 대신 보여 준다.
        fields = ", ".join(
            ".".join(str(part) for part in error["loc"]) or str(error["msg"])
            for error in exc.errors(include_input=False, include_url=False)
        )
        raise ConfigurationError(
            f"missing or invalid configuration: {fields}"
        ) from None
