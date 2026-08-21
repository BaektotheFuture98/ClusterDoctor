"""단발 호출 어댑터. 로그 전체를 한 프롬프트에 담아 LLM에 한 번 물어본다.

10분 창에서도 소스당 200건 샘플링 상한에 걸릴 수 있다. 상한을 넘긴 구간이
통째로 사라지는 것이 문제라면 ``ANALYSIS_MODE=graph``(분 단위 map-reduce)를
쓴다 — ``langgraph.analyzer`` 참조.
"""

from cluster_doctor.adapter.outbound.llm.litellm_client import (
    complete,
    require_supported_provider,
)
from cluster_doctor.adapter.outbound.llm.prompt_builder import build_prompt
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.port.outbound.llm_analyzer import LlmAnalyzer


class LiteLlmAdapter(LlmAnalyzer):
    def __init__(self, provider: str, api_key: str, default_model: str):
        self._provider = require_supported_provider(provider)
        self._api_key = api_key
        self._default_model = default_model

    def analyze(
        self, time_range: TimeRange, logs: list[LogEntry], model: str | None
    ) -> str:
        return complete(
            messages=[
                {"role": "user", "content": build_prompt(time_range, logs)}
            ],
            provider=self._provider,
            model=model or self._default_model,
            api_key=self._api_key,
        )
