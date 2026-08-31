"""분별 분석 프롬프트.

분별 호출은 structured output(JSON)으로 강제된다. 그런데도 프롬프트가
"요약:/근거: 형식으로 답하라"고 지시하면 모델에게 모순된 형식 계약 두 개가
동시에 전달된다. 스키마 쪽이 이기므로 그 지시는 효과가 없고, 구간마다 토큰만
축내며, 만들 수 없는 형식을 만들라고 시키는 상태가 된다.
"""

from datetime import datetime
from decimal import Decimal

from cluster_doctor.domain.model.log_entry import (
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.prompts import (
    build_minute_prompt,
    format_log_line,
)

# 픽스처는 전부 합성 값이다. 실제 인덱스명·노드명·고객사명·사용자 주소를
# 쓰지 않는다 — 테스트 파일은 커밋되고 공유되므로 운영 식별자나 제3자
# 개인정보가 들어가서는 안 된다. 형식만 실제와 같으면 계약 검증에 충분하다.
SLOWLOG = SlowlogEntry(
    timestamp=datetime(2026, 8, 27, 14, 0, 2),
    index_name="app_index_v1", node="node-a01", took="32.4s",
    total_hits="68 hits", total_shards=902,
    opaque_id="service=web,company=1,user=2", query='{"size":0}',
)
QUERYLOG = QueryLogEntry(
    timestamp=datetime(2026, 8, 27, 14, 0, 3),
    host="10.0.0.11", run_time=Decimal("2.12"), success=True, cmd="agg",
    service="web", env="prod", project="search_app", cluster="main",
    keywords=("검색어A",), company="acme", user="user01@example.com",
)
METRIC = NodeMetricEntry(
    timestamp=datetime(2026, 8, 27, 14, 0, 2),
    node_name="node-b02", node_ip="10.0.0.12",
    os_cpu_percent=1, os_mem_used_percent=99, process_cpu_percent=0,
    jvm_heap_used_percent=59, search_active=0, search_queue=0, search_rejected=0,
    write_active=0, write_queue=0, write_rejected=0,
)
LOGS = [SLOWLOG]


# 소스별 dataclass로 나누기 전과 프롬프트 출력이 한 글자도 달라지면 안 된다.
# 이 단계는 순수 리팩터링이고, keyword 절단 같은 실제 변경은 그다음이다.
def test_slowlog_line_is_unchanged():
    assert format_log_line(SLOWLOG) == (
        "  2026-08-27 14:00:02 [SLOWLOG] node=node-a01 comp=app_index_v1 "
        "took=32.4s, 68 hits, shards=902, "
        "id=service=web,company=1,user=2, query={\"size\":0}"
    )


# --------------------------------------------------------------------------
# keyword 절단
#
# 한 쿼리가 키워드 200개 넘게 실어 보내는 경우가 있다(실측: 한 줄 2,029자).
# 같은 유저가 agg와 count로 같은 목록을 두 번 보내면 4,000자가 연달아 들어간다.
# 진단에 필요한 것은 "이 유저가 어떤 주제를 검색했나"이지 전량이 아니다.
# --------------------------------------------------------------------------

def _with_keywords(*kw) -> QueryLogEntry:
    return QueryLogEntry(**{**QUERYLOG.__dict__, "keywords": tuple(kw)})


def test_short_keyword_list_is_shown_whole():
    line = format_log_line(_with_keywords("검색어A", "검색어B"))
    assert "keyword=['검색어A', '검색어B']" in line
    assert "외" not in line


def test_long_keyword_list_is_truncated_with_the_total():
    # 잘렸다는 사실과 원래 몇 개였는지를 함께 알려야, 모델이 "이 쿼리는
    # 키워드가 215개였다"는 것을 근거로 쓸 수 있다.
    line = format_log_line(_with_keywords(*[f"kw{i}" for i in range(215)]))
    assert "keyword=['kw0', 'kw1', 'kw2', 'kw3', 'kw4'] 외 210개" in line
    assert "kw5" not in line


def test_exactly_the_limit_is_not_marked_as_truncated():
    line = format_log_line(_with_keywords("a", "b", "c", "d", "e"))
    assert "외" not in line


def test_empty_keyword_list_renders_as_before():
    assert "keyword=[]" in format_log_line(_with_keywords())


def test_truncation_is_presentation_only():
    # 도메인은 전량을 그대로 들고 있어야 한다. 나중에 집계하거나 상관관계를
    # 볼 때 필요한 값이다.
    entry = _with_keywords(*[f"kw{i}" for i in range(215)])
    assert len(entry.keywords) == 215


def test_long_keyword_list_shrinks_the_line_dramatically():
    long_entry = _with_keywords(*[f"키워드{i}" for i in range(215)])
    short_entry = _with_keywords("검색어A")
    assert len(format_log_line(long_entry)) < len(format_log_line(short_entry)) + 120


def test_query_log_line_is_unchanged():
    assert format_log_line(QUERYLOG) == (
        "  2026-08-27 14:00:03 [SUCCESS] node=10.0.0.11 comp=web "
        "[agg] project=search_app env=prod cluster=main runtime=2.12s "
        "keyword=['검색어A'] company=acme user=user01@example.com"
    )


def test_failed_query_renders_as_FAIL():
    line = format_log_line(QueryLogEntry(**{**QUERYLOG.__dict__, "success": False}))
    assert "[FAIL]" in line


def test_node_metric_line_is_unchanged():
    assert format_log_line(METRIC) == (
        "  2026-08-27 14:00:02 [METRIC] node=node-b02 (10.0.0.12) comp=- "
        "cpu=1% mem=99% proc_cpu=0% jvm_heap=59% "
        "search(active=0,queue=0,rejected=0) write(active=0,queue=0,rejected=0)"
    )


def test_prompt_groups_by_source():
    prompt = build_minute_prompt([SLOWLOG, QUERYLOG, METRIC], "2026-08-27 14:00")
    assert "[slowlog]" in prompt
    assert "[es_query_log]" in prompt
    assert "[node_metric]" in prompt


def _prompt() -> str:
    return build_minute_prompt(LOGS, "2026-08-27 14:00")


def test_does_not_dictate_a_text_response_format():
    # 스키마가 형식을 강제하므로 형식 지시는 죽은 텍스트다.
    prompt = _prompt()
    assert "요약:" not in prompt
    assert "근거:" not in prompt
    assert "마크다운 금지" not in prompt


def test_describes_what_each_output_field_must_carry():
    # 형식 대신 내용을 지시한다. evidence에 원문이 들어가지 않으면 종합
    # 단계가 쿼리·노드명·수치를 인용할 수 없다.
    prompt = _prompt()
    assert "summary" in prompt
    assert "evidence" in prompt


def test_still_carries_the_logs_and_the_minute_label():
    prompt = _prompt()
    assert "2026-08-27 14:00" in prompt
    assert "node-a01" in prompt
    assert "took=32.4s" in prompt
    assert "1건 (전량)" in prompt


def test_unknown_entry_type_is_rejected_loudly():
    # 새 소스를 추가하면서 렌더러 등록을 잊으면 조용히 빈 줄이 들어가는 대신
    # 여기서 터져야 한다.
    import pytest

    with pytest.raises(TypeError):
        format_log_line(object())


def test_still_forbids_inventing_problems():
    # 억지로 문제를 만들어내면 리포트 전체가 신뢰를 잃는다.
    assert "특이사항 없음" in _prompt()
