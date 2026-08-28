"""그래프 전체 동작. LLM은 가짜 호출자로 대체한다.

노드가 ``LlmCaller``를 주입받는 덕분에 litellm을 몽키패치할 필요가 없다.

분별 분석은 structured output(JSON)으로만 동작한다. 조립부가 분별 호출자에
``response_format=MinuteOutput``을 걸어 두므로 모델은 스키마 밖으로 나올 수
없다. 그래서 여기 가짜 LLM도 JSON을 돌려준다 — 텍스트를 돌려주면 프로덕션이
결코 받지 않는 응답을 검증하게 된다.
"""

import json
import threading
import time
from datetime import datetime

import pytest

from cluster_doctor.infrastructure.outbound.llm.langgraph.graph import build_graph
from cluster_doctor.domain.model.log_entry import SlowlogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.application.port.outbound.llm_analyzer import LlmApiError

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 14))

MINUTE_MAX_TOKENS = 1024
MINUTE_REPLY = json.dumps(
    {"summary": "특이사항 없음", "evidence": ["근거A", "근거B"]}, ensure_ascii=False
)
FINAL_REPLY = "최종 리포트"


def _log(minute: int, second: int, marker: str | None = None) -> SlowlogEntry:
    """구간 격리를 확인하려면 항목이 어느 분에서 왔는지 프롬프트에서 보여야 한다.

    ``query``에 표식을 넣는다 — 프롬프트 줄에 그대로 실리는 필드다.
    """
    return SlowlogEntry(
        timestamp=datetime(2026, 8, 20, 2, minute, second),
        query=marker or f"log-{minute:02d}:{second:02d}",
    )


class _Recorder:
    """호출을 기록하는 가짜 LLM. 분별 호출과 종합 호출을 토큰 한도로 구분한다."""

    def __init__(self, minute_reply=MINUTE_REPLY, minute_error=None, delay=0.0):
        self.calls = []
        self.prompts = []
        self._minute_reply = minute_reply
        self._minute_error = minute_error
        self._delay = delay
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def __call__(self, messages, max_tokens):
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.calls.append(max_tokens)
            self.prompts.append(messages[0]["content"])
        try:
            if self._delay:
                time.sleep(self._delay)
            if max_tokens == MINUTE_MAX_TOKENS:
                if self._minute_error is not None:
                    raise self._minute_error
                return self._minute_reply
            return FINAL_REPLY
        finally:
            with self._lock:
                self.active -= 1

    @property
    def minute_calls(self):
        return [c for c in self.calls if c == MINUTE_MAX_TOKENS]

    @property
    def minute_prompts(self):
        return [
            p for p, c in zip(self.prompts, self.calls) if c == MINUTE_MAX_TOKENS
        ]

    @property
    def synthesis_prompt(self):
        return next(
            p for p, c in zip(self.prompts, self.calls) if c != MINUTE_MAX_TOKENS
        )


def _run(llm, logs, time_range=TR):
    # 같은 recorder를 두 자리에 넘긴다. 분별 호출과 종합 호출은 max_tokens로
    # 구분되므로 하나로 충분하다.
    return build_graph(llm, llm).invoke(
        {
            "time_range": time_range,
            "logs": logs,
            "model": "test-model",
            "buckets": [],
            "findings": [],
            "report": "",
        }
    )


def test_build_graph_requires_a_structured_minute_caller():
    """텍스트 전용 모드는 없다.

    예전에는 call_llm_minute을 생략하면 분별 호출에 response_format이 걸리지
    않은 호출자가 쓰였고, 응답을 텍스트로만 파싱했다. 프로덕션은 그 경로를
    쓴 적이 없는데 테스트만 그쪽을 검증하고 있었다. 생략을 막아 두 갈래가
    다시 갈라지지 않게 한다.
    """
    with pytest.raises(TypeError):
        build_graph(_Recorder())


def test_one_llm_call_per_non_empty_minute_plus_one_synthesis():
    logs = [_log(m, s) for m in (9, 10, 11) for s in (5, 45)]
    llm = _Recorder()

    state = _run(llm, logs)

    assert len(llm.minute_calls) == 3, "구간마다 한 번씩"
    assert len(llm.calls) == 4, "분별 3 + 종합 1"
    assert state["report"] == FINAL_REPLY


def test_minutes_without_logs_cost_nothing():
    """빈 구간에 LLM을 부르는 것은 순수한 낭비다.

    02:09과 02:13에만 로그가 있으면 그 사이 세 구간은 호출하지 않는다.
    """
    logs = [_log(9, 5), _log(13, 5)]
    llm = _Recorder()

    _run(llm, logs)

    assert len(llm.minute_calls) == 2


def test_each_minute_node_sees_only_its_own_logs():
    """이 그래프가 존재하는 이유. 구간이 섞이면 분할한 의미가 없다."""
    logs = [_log(9, 5), _log(10, 5), _log(11, 5)]
    llm = _Recorder()

    _run(llm, logs)

    for prompt in llm.minute_prompts:
        present = [m for m in ("log-09", "log-10", "log-11") if m in prompt]
        assert len(present) == 1, f"한 구간의 로그만 있어야 하는데 {present}"


def test_minute_logs_are_not_sampled():
    """단발 모드는 소스당 200건에서 자른다. 구간별 분석은 자르지 않는다.

    이 단정이 깨지면 그래프 모드가 단발 모드의 결함을 그대로 물려받은
    것이므로, 분할 자체가 무의미해진다.
    """
    logs = [_log(9, 0, marker=f"line-{i}") for i in range(250)]
    llm = _Recorder()

    _run(llm, logs)

    prompt = llm.minute_prompts[0]
    assert "line-0" in prompt, "가장 오래된 줄이 잘려나갔다"
    assert "line-249" in prompt
    assert prompt.count("line-") == 250


def test_findings_reach_synthesis_in_chronological_order():
    """팬아웃은 완료 순서를 보장하지 않으므로 종합 전에 정렬해야 한다."""
    logs = [_log(m, 5) for m in (9, 10, 11, 12)]
    llm = _Recorder()

    _run(llm, logs)

    prompt = llm.synthesis_prompt
    positions = [prompt.index(f"02:{m:02d}") for m in (9, 10, 11, 12)]
    assert positions == sorted(positions)


def test_evidence_lines_reach_the_synthesis_prompt():
    """근거 원문이 종합 단계에 도달해야 최종 리포트가 구체적일 수 있다."""
    llm = _Recorder()

    _run(llm, [_log(9, 5)])

    assert "근거A" in llm.synthesis_prompt
    assert "근거B" in llm.synthesis_prompt


def test_non_json_reply_falls_back_to_text_parsing():
    """스키마가 강제돼도 응답이 JSON이 아닐 수 있다.

    provider가 바뀌거나 structured output이 조용히 무시되는 경우가 있다.
    그때 구간을 통째로 잃는 대신 텍스트로 파 본다.
    """
    llm = _Recorder(minute_reply="요약:\n디스크 포화\n근거:\n  node-1 disk=98%")

    _run(llm, [_log(9, 5)])

    assert "디스크 포화" in llm.synthesis_prompt
    assert "node-1 disk=98%" in llm.synthesis_prompt


def test_free_form_reply_is_kept_whole_rather_than_dropped():
    """형식을 어긴 응답 때문에 구간을 통째로 잃지 않는다."""
    llm = _Recorder(minute_reply="형식을 무시한 자유 서술")

    _run(llm, [_log(9, 5)])

    assert "형식을 무시한 자유 서술" in llm.synthesis_prompt


def test_json_scalar_reply_does_not_crash_the_minute():
    """json.loads가 성공해도 dict가 아닐 수 있다 (숫자·문자열 리터럴).

    .get 호출이 AttributeError로 터지면 그 구간이 통째로 날아간다.
    """
    llm = _Recorder(minute_reply='"특이사항 없음"')

    state = _run(llm, [_log(9, 5)])

    assert state["report"] == FINAL_REPLY
    assert sum(1 for f in state["findings"] if f.failed) == 0


def test_non_string_evidence_does_not_kill_the_minute():
    # structured output이 우회되면 evidence에 dict가 들어올 수 있다.
    # "\n".join(...)이 try 밖에 있어 그대로 터지면 구간이 통째로 날아간다.
    llm = _Recorder(minute_reply=json.dumps(
        {"summary": "느린 쿼리", "evidence": [{"log": "line-1"}, 42]}, ensure_ascii=False
    ))

    state = _run(llm, [_log(9, 5)])

    assert state["report"] == FINAL_REPLY
    assert sum(1 for f in state["findings"] if f.failed) == 0


def test_string_evidence_stays_one_line_instead_of_exploding_into_characters():
    # 스키마는 list[str]을 강제하지만 그것이 우회되면 문자열 하나가 올 수 있다.
    # [str(item) for item in evidence]는 문자열을 순회해 글자 수만큼의 항목을
    # 만들고, 그 쓰레기 줄들이 종합 프롬프트에 그대로 실린다.
    llm = _Recorder(minute_reply=json.dumps(
        {"summary": "느린 쿼리", "evidence": "쿼리 X가 느림"}, ensure_ascii=False
    ))

    state = _run(llm, [_log(9, 5)])

    assert state["findings"][0].evidence == ["쿼리 X가 느림"]
    assert "쿼리 X가 느림" in llm.synthesis_prompt


def test_one_failed_minute_does_not_sink_the_whole_diagnosis():
    """구간 하나가 rate limit에 걸렸다고 나머지 분석까지 버리지 않는다."""

    class _OneFailure(_Recorder):
        def __init__(self):
            super().__init__()
            self._failed_once = False

        def __call__(self, messages, max_tokens):
            if max_tokens == MINUTE_MAX_TOKENS and not self._failed_once:
                self._failed_once = True
                with self._lock:
                    self.calls.append(max_tokens)
                    self.prompts.append(messages[0]["content"])
                raise LlmApiError("LLM provider(nvidia) 호출이 실패했습니다 (status=429)")
            return super().__call__(messages, max_tokens)

    llm = _OneFailure()
    state = _run(llm, [_log(m, 5) for m in (9, 10, 11)])

    assert state["report"] == FINAL_REPLY
    assert sum(1 for f in state["findings"] if f.failed) == 1
    assert "[분석 실패]" in llm.synthesis_prompt, (
        "종합 단계는 빠진 구간이 있다는 사실을 알아야 한다"
    )


def test_all_minutes_failing_raises_rather_than_reporting_nothing_wrong():
    """전 구간 실패를 '특이사항 없음'처럼 돌려주면 운영자가 오해한다."""
    llm = _Recorder(minute_error=LlmApiError("호출 실패 (status=429)"))

    with pytest.raises(LlmApiError, match="모든 구간"):
        _run(llm, [_log(m, 5) for m in (9, 10)])


def test_empty_log_set_still_produces_a_report():
    """로그가 없는 시간 범위도 500이 아니라 리포트로 답해야 한다."""
    llm = _Recorder()

    state = _run(llm, [])

    assert state["report"] == FINAL_REPLY
    assert llm.minute_calls == []
    assert "로그가 없습니다" in llm.synthesis_prompt


def test_minute_analyses_can_run_concurrently():
    """LangGraph의 동기 invoke가 팬아웃 분기를 스레드로 병렬 실행한다.

    주의: 이것은 그래프의 능력이지 프로덕션의 동작이 아니다. 조립부는
    ``config={"max_concurrency": 1}``을 걸어(tools.py) 의도적으로 순차
    실행시킨다 — Gemini 무료 티어의 분당 입력 토큰 한도에 걸리기 때문이다.
    여기서는 config 없이 불러 그래프 자체의 팬아웃을 확인한다. 이 단정이
    깨지면 max_concurrency를 풀어도 병렬이 되지 않는다는 뜻이다.
    """
    llm = _Recorder(delay=0.2)

    _run(llm, [_log(m, 5) for m in (9, 10, 11, 12, 13)])

    assert llm.peak >= 2, f"분별 분석이 순차로 실행됐다 (최대 동시 {llm.peak})"
