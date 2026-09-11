"""5단계 보조 조사의 경로 분기.

마스터 노드 로그는 ClickHouse(search_node_logs)에서, 데이터 노드 로그는
SSH(get_node_logs)에서 얻는다. 이 분기는 적재 범위에서 온 것이다 —
loki_logs에는 마스터 로그만 들어오므로 데이터 노드를 그쪽에서 찾으면 0건이다.
프롬프트가 한때 데이터 노드까지 ClickHouse로 보내고 SSH를 실패 폴백으로
강등한 적이 있어, 그 회귀를 막으려고 고정한다.
"""

from cluster_doctor.infrastructure.outbound.llm.deepagent.prompts import SYSTEM_PROMPT


def _step(letter: str) -> str:
    """5단계의 한 항목만 잘라낸다."""
    start = SYSTEM_PROMPT.index(f"\n{letter}. ")
    tail = SYSTEM_PROMPT[start + 1 :]
    nxt = tail.find("\n\n")
    return tail[:nxt] if nxt != -1 else tail


def test_master_logs_come_from_clickhouse():
    step_c = _step("c")
    assert "search_node_logs" in step_c
    assert 'node_role="master"' in step_c


def test_data_node_logs_come_from_ssh_not_clickhouse():
    step_d = _step("d")
    assert "get_node_logs" in step_d
    assert "search_node_logs는" in step_d  # 그쪽에 없다는 안내
    # 데이터 노드를 ClickHouse로 조회하라고 시키면 0건만 돌아온다.
    assert 'search_node_logs(start_iso' not in step_d


def test_node_id_comes_from_the_master_log():
    step_d = _step("d")
    assert "마스터 로그에서 확인한 노드 ID" in step_d


def test_prompt_says_the_tool_resolves_ssh_details_itself():
    # get_node_logs가 내부에서 GET /_nodes/<id>로 ip·로그 경로를 얻는다.
    # 이것을 적어 두지 않으면 모델이 get_node_info를 먼저 부르려 한다.
    step_d = _step("d")
    assert "_nodes/" in step_d
    assert "따로 구할 필요가 없다" in step_d


def test_ssh_is_also_the_fallback_for_master_logs():
    step_e = _step("e")
    assert 'get_node_logs("_master"' in step_e
