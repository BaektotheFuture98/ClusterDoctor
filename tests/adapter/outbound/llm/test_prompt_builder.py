from datetime import datetime

from cluster_doctor.adapter.outbound.llm.prompt_builder import build_prompt
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 10))


def _log(source: str, level: str, idx: int = 0) -> LogEntry:
    return LogEntry(
        timestamp=datetime(2026, 8, 20, 2, 9, 0, microsecond=idx),
        level=level,
        source=source,
        message=f"msg{idx}",
        node="node1",
        component="svc1",
    )


def test_groups_by_source():
    prompt = build_prompt(
        TR,
        [
            _log("slowlog", "SLOWLOG"),
            _log("es_query_log", "SUCCESS"),
            _log("node_metric", "METRIC"),
        ],
    )
    assert "[slowlog]" in prompt
    assert "[es_query_log]" in prompt
    assert "[node_metric]" in prompt


def test_caps_each_source_at_200_and_discloses_the_true_total():
    logs = [_log("slowlog", "SLOWLOG", i) for i in range(250)]
    prompt = build_prompt(TR, logs)
    body_lines = [line for line in prompt.split("\n") if "msg" in line]
    assert len(body_lines) == 200
    assert "총 250건 중 200건 샘플" in prompt


def test_does_not_claim_sampling_when_under_the_cap():
    logs = [_log("slowlog", "SLOWLOG", i) for i in range(3)]
    prompt = build_prompt(TR, logs)
    assert "총 3건 중 3건 샘플" in prompt


def test_contains_analysis_principles():
    prompt = build_prompt(TR, [])
    assert "임계치 초과" in prompt
    assert "특이사항 없음" in prompt
    assert "FAIL" in prompt


def test_contains_known_problem_patterns():
    prompt = build_prompt(TR, [])
    assert "토큰 분석 이슈" in prompt
    assert "광범위 범위 검색" in prompt


def test_contains_response_format_and_all_seven_sections():
    prompt = build_prompt(TR, [])
    assert "마크다운 금지" in prompt
    for section in [
        "요청 이해",
        "발견된 문제점",
        "근본 원인",
        "소스 간 상관관계",
        "문제 쿼리 후보",
        "권장 조치",
        "클러스터 건강 상태",
    ]:
        assert section in prompt


def test_empty_logs_produce_a_placeholder_not_a_malformed_section():
    prompt = build_prompt(TR, [])
    assert "(로그 없음)" in prompt


def test_unknown_source_degrades_without_raising():
    prompt = build_prompt(TR, [_log("mystery_source", "INFO")])
    assert "[mystery_source]" in prompt
