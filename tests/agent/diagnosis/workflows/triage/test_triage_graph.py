"""Hierarchical Map-Reduce Log Triage.

요구사항 6이 여기 있다 — 이 그래프는 Raw Log가 아니라 Evidence를 돌려준다.

함께 확인하는 것: 모델은 **번호만** 돌려주고 시각·원문은 코드가 옮긴다는 규칙,
그리고 Reduce가 원인을 확정하지 않는다는 경계.
"""

import json
from datetime import datetime

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.contracts.evidence import Evidence, EvidenceSource
from cluster_doctor.agent.diagnosis.workflows.triage.graph import run_triage
from cluster_doctor.agent.diagnosis.workflows.triage.nodes import (
    MapOutput,
    ReduceOutput,
)
from cluster_doctor.agent.diagnosis.workflows.triage.spec import TriageSpec
from cluster_doctor.agent.diagnosis.workflows.triage.state import RawRecord
from tests.contracts.test_time_range_spans import KST

SPEC = TriageSpec(
    source=EvidenceSource.SLOWLOG,
    label="테스트 소스",
    what_matters="오래 걸린 것",
    what_is_noise="반복되는 것",
    max_evidence=3,
)


def at(minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 18, 14, minute, second, tzinfo=KST)


def records(*specs) -> list[RawRecord]:
    return [
        RawRecord(record_id=index, event_time=moment, line=line)
        for index, (moment, line) in enumerate(specs, start=1)
    ]


class ScriptedLlm:
    """Map과 Reduce를 ``response_format``으로 구분해 답한다."""

    def __init__(self, *, keep_in_map, keep_in_reduce, fail_map_at=(), fail_reduce=False):
        self._keep_in_map = keep_in_map
        self._keep_in_reduce = keep_in_reduce
        self._fail_map_at = set(fail_map_at)
        self._fail_reduce = fail_reduce
        self.map_calls = 0
        self.reduce_calls = 0
        self.prompts: list[str] = []

    def __call__(self, messages, max_tokens, response_format=None):
        prompt = messages[0]["content"]
        self.prompts.append(prompt)
        if response_format is MapOutput:
            self.map_calls += 1
            if self.map_calls in self._fail_map_at:
                raise LlmApiError("429")
            visible = {
                int(line.split()[0].lstrip("#"))
                for line in prompt.splitlines()
                if line.startswith("#")
            }
            return json.dumps(
                {
                    "selected": [
                        {"record_id": rid, "event_type": "slow_query", "reason": "느림"}
                        for rid in self._keep_in_map
                        if rid in visible
                    ]
                }
            )
        assert response_format is ReduceOutput
        self.reduce_calls += 1
        if self._fail_reduce:
            raise LlmApiError("429")
        return json.dumps(
            {
                "keep": [
                    {"record_id": rid, "selection_reason": "시작 시점"}
                    for rid in self._keep_in_reduce
                ]
            }
        )


def run(spec, items, llm, **kwargs):
    return run_triage(
        spec,
        items,
        llm,
        new_evidence_id=kwargs.get("new_evidence_id", _counter()),
        put_raw=kwargs.get("put_raw", lambda text: f"R:{text[:8]}"),
    )


def _counter():
    state = {"n": 0}

    def next_id() -> str:
        state["n"] += 1
        return f"E-{state['n']}"

    return next_id


class TestOutputIsEvidenceNotRawLogs:
    def test_결과가_Evidence다(self):
        """요구사항 6번. 이 그래프의 산출물은 줄어든 근거이지 원문이 아니다."""
        llm = ScriptedLlm(keep_in_map=[1, 2], keep_in_reduce=[2])

        result = run(SPEC, records((at(0), "took=1s"), (at(1), "took=37s")), llm)

        assert all(isinstance(item, Evidence) for item in result.evidence)
        assert [item.message for item in result.evidence] == ["took=37s"]

    def test_시각과_원문은_코드가_옮긴다(self):
        """모델은 번호만 돌려준다. 옮겨 적게 시키면 틀린다."""
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1])

        result = run(SPEC, records((at(3, 17), "took=37.1s index=logs-2026")), llm)

        evidence = result.evidence[0]
        assert evidence.event_time == at(3, 17)
        assert evidence.message == "took=37.1s index=logs-2026"
        assert evidence.source is EvidenceSource.SLOWLOG

    def test_원문_참조가_붙는다(self):
        """검증이 "이 주장이 실제 줄과 맞는가"를 보려면 원문에 닿아야 한다."""
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1])
        stored: list[str] = []

        result = run(
            SPEC,
            records((at(0), "took=37.1s")),
            llm,
            put_raw=lambda text: stored.append(text) or f"R-{len(stored)}",
        )

        assert result.evidence[0].raw_ref == "R-1"
        assert stored == ["took=37.1s"]

    def test_선별_이유가_남는다(self):
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1])

        result = run(SPEC, records((at(0), "took=37.1s")), llm)

        assert result.evidence[0].selection_reason == "시작 시점"


class TestMapReduceShape:
    def test_분마다_한_번씩_고른다(self):
        llm = ScriptedLlm(keep_in_map=[1, 2, 3], keep_in_reduce=[1])

        run(SPEC, records((at(0), "a"), (at(1), "b"), (at(2), "c")), llm)

        assert llm.map_calls == 3
        assert llm.reduce_calls == 1

    def test_로그가_없는_분에는_LLM을_부르지_않는다(self):
        llm = ScriptedLlm(keep_in_map=[1, 2], keep_in_reduce=[1])

        run(SPEC, records((at(0), "a"), (at(5), "b")), llm)

        assert llm.map_calls == 2

    def test_레코드가_없으면_그래프를_돌리지_않는다(self):
        llm = ScriptedLlm(keep_in_map=[], keep_in_reduce=[])

        result = run(SPEC, [], llm)

        assert result.evidence == []
        assert llm.map_calls == 0
        assert llm.reduce_calls == 0

    def test_Reduce는_Map이_고른_것_중에서만_고른다(self):
        """Map이 버린 줄을 Reduce가 되살리면 분 단위 선별이 무의미해진다."""
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1, 2])

        result = run(SPEC, records((at(0), "a"), (at(0), "b")), llm)

        assert [item.message for item in result.evidence] == ["a"]

    def test_상한을_넘겨_남기지_않는다(self):
        llm = ScriptedLlm(keep_in_map=[1, 2, 3, 4], keep_in_reduce=[1, 2, 3, 4])

        result = run(
            SPEC,
            records((at(0), "a"), (at(0), "b"), (at(0), "c"), (at(0), "d")),
            llm,
        )

        assert len(result.evidence) == SPEC.max_evidence


class TestFabricationIsBlocked:
    def test_없는_번호는_버린다(self):
        """모델이 지어낸 번호를 통과시키면 Evidence가 가리키는 원문이 실제
        로그와 어긋난다 — 번호로 고르게 한 이유가 사라진다."""
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1, 99])

        result = run(SPEC, records((at(0), "a")), llm)

        assert [item.message for item in result.evidence] == ["a"]

    def test_같은_줄은_한_번만_남는다(self):
        llm = ScriptedLlm(keep_in_map=[1, 2], keep_in_reduce=[1, 2])

        result = run(SPEC, records((at(0), "같은 줄"), (at(0), "같은 줄")), llm)

        assert len(result.evidence) == 1


class TestPartialFailure:
    def test_한_분이_실패해도_나머지는_남는다(self):
        llm = ScriptedLlm(keep_in_map=[1, 2], keep_in_reduce=[1, 2], fail_map_at=(1,))

        result = run(SPEC, records((at(0), "a"), (at(1), "b")), llm)

        assert result.failed_minutes == 1
        assert result.analyzed_minutes == 2
        assert len(result.evidence) == 1

    def test_실패한_분의_시각을_알려준다(self):
        """개수만으로는 "어느 시각을 못 봤는가"를 말할 수 없다."""
        llm = ScriptedLlm(keep_in_map=[1, 2], keep_in_reduce=[1, 2], fail_map_at=(1,))

        result = run(SPEC, records((at(0), "a"), (at(1), "b")), llm)

        assert result.failed_minutes_at == (at(0),)

    def test_전부_실패하면_그_사실이_드러난다(self):
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1], fail_map_at=(1, 2))

        result = run(SPEC, records((at(0), "a"), (at(1), "b")), llm)

        assert result.fully_failed is True
        assert result.evidence == []

    def test_Reduce가_실패하면_Map_결과를_남기고_표시한다(self):
        """걸러지지 않은 근거가, 근거가 없는 것보다 낫다. 다만 걸러지지
        않았다는 사실이 드러나야 한다."""
        llm = ScriptedLlm(keep_in_map=[1, 2], keep_in_reduce=[], fail_reduce=True)

        result = run(SPEC, records((at(0), "a"), (at(1), "b")), llm)

        assert result.reduce_degraded is True
        assert len(result.evidence) == 2

    def test_Reduce가_아무것도_고르지_않은_것은_실패가_아니다(self):
        """구간 내내 같은 이벤트만 반복됐다면 정당한 결과다. 여기서 Map 결과를
        되살리면 걸러내라고 만든 단계가 아무것도 걸러 내지 못한다."""
        llm = ScriptedLlm(keep_in_map=[1, 2], keep_in_reduce=[])

        result = run(SPEC, records((at(0), "a"), (at(1), "b")), llm)

        assert result.evidence == []
        assert result.reduce_degraded is False


class TestPromptBoundary:
    def test_Map_프롬프트는_원인을_묻지_않는다(self):
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1])

        run(SPEC, records((at(0), "a")), llm)

        assert "원인을 추론하지 않는다" in llm.prompts[0]

    def test_Reduce_프롬프트도_근본_원인을_확정하지_않는다(self):
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1])

        run(SPEC, records((at(0), "a")), llm)

        assert "근본 원인을 확정하지 않는다" in llm.prompts[-1]

    def test_datasource별_기준이_프롬프트에_실린다(self):
        llm = ScriptedLlm(keep_in_map=[1], keep_in_reduce=[1])

        run(SPEC, records((at(0), "a")), llm)

        assert SPEC.what_matters in llm.prompts[0]
        assert SPEC.what_is_noise in llm.prompts[0]
