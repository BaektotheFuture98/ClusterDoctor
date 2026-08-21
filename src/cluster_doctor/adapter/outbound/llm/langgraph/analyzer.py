"""분 단위 map-reduce 어댑터. ``LlmAnalyzer`` 포트의 두 번째 구현체다.

단발 어댑터와 같은 포트 뒤에 서므로 ``DiagnosisService``는 어느 쪽이
끼워졌는지 모른다. 고르는 곳은 ``config/dependencies.py`` 한 곳이다.
"""

from functools import partial

from cluster_doctor.adapter.outbound.llm.langgraph.graph import build_graph
from cluster_doctor.adapter.outbound.llm.litellm_client import (
    complete,
    require_supported_provider,
)
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.port.outbound.llm_analyzer import LlmAnalyzer


class LangGraphAnalyzer(LlmAnalyzer):
    """로그를 1분 구간으로 나눠 각각 분석한 뒤 종합한다.

    단발 어댑터는 소스당 200건에서 자르고, 로그가 최신순으로 정렬돼 있어
    오래된 구간이 통째로 빠진다. 바쁜 소스라면 10분 창을 요청해도 마지막
    1분만 모델에게 도달한다. 구간을 나눠 각각 전량을 보여주면 그 손실이
    사라진다.
    """

    def __init__(self, provider: str, api_key: str, default_model: str):
        self._provider = require_supported_provider(provider)
        self._api_key = api_key
        self._default_model = default_model

    def analyze(
        self, time_range: TimeRange, logs: list[LogEntry], model: str | None
    ) -> str:
        effective_model = model or self._default_model
        # provider/model/api_key를 여기서 묶어 그래프에 넘긴다. 노드는
        # 누구에게 묻는지 모르고, 얼마나 길게 답할지만 정한다.
        call_llm = partial(
            _call,
            provider=self._provider,
            model=effective_model,
            api_key=self._api_key,
        )
        final_state = build_graph(call_llm).invoke(
            {
                "time_range": time_range,
                "logs": logs,
                "model": effective_model,
                "buckets": [],
                "findings": [],
                "report": "",
            }
        )
        return final_state["report"]


def _call(
    messages: list[dict], max_tokens: int, *, provider: str, model: str, api_key: str
) -> str:
    """``LlmCaller`` 모양(messages, max_tokens)으로 ``complete``를 감싼다."""
    return complete(
        messages=messages,
        provider=provider,
        model=model,
        api_key=api_key,
        max_tokens=max_tokens,
    )
