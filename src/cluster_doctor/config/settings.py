from functools import lru_cache
from typing import Literal

from pydantic import ValidationInfo, field_validator
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
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta"

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


@lru_cache
def get_settings() -> Settings:
    return Settings()
