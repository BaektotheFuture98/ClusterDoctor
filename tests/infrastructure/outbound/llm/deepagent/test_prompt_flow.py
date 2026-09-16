"""5단계 보조 조사의 경로 분기.

마스터 노드 로그는 ClickHouse(search_node_logs)에서, 데이터 노드 로그는
SSH(get_node_logs)에서 얻는다. 이 분기는 적재 범위에서 온 것이다 —
loki_logs에는 마스터 로그만 들어오므로 데이터 노드를 그쪽에서 찾으면 0건이다.
프롬프트가 데이터 노드까지 ClickHouse로 보내고 SSH를 실패 폴백으로 강등하면
그 분기가 무너지므로, 순서를 고정한다.
"""

import re

from cluster_doctor.infrastructure.outbound.llm.deepagent.prompts import SYSTEM_PROMPT

_ITEM = re.compile(r"^[a-z]\. ", re.M)


def _step(anchor: str) -> str:
    """5단계에서 ``anchor``를 담은 항목 하나를 잘라낸다.

    항목 문자(a/b/c…)로 찾지 않는다. 항목 하나가 빠지면 뒤가 전부 밀려
    엉뚱한 항목을 검사하게 되는데, 그래도 테스트가 통과할 수 있다 —
    get_index_summary를 지우면서 실제로 그랬다. 내용을 기준으로 찾는다.
    """
    idx = SYSTEM_PROMPT.index(anchor)
    start = max(m.start() for m in _ITEM.finditer(SYSTEM_PROMPT) if m.start() <= idx)
    nxt = _ITEM.search(SYSTEM_PROMPT, idx)
    section = SYSTEM_PROMPT.find("\n### ", idx)
    ends = [e for e in ((nxt.start() if nxt else -1), section) if e != -1]
    return SYSTEM_PROMPT[start : min(ends)] if ends else SYSTEM_PROMPT[start:]


_MASTER = "마스터 노드 로그를 먼저 본다"
_DATA_NODE = "지목된 노드의 로그를 SSH로 수집한다"


def test_master_logs_come_from_clickhouse():
    step = _step(_MASTER)
    assert "search_node_logs" in step
    assert 'node_role="master"' in step


def test_data_node_logs_come_from_ssh_not_clickhouse():
    step = _step(_DATA_NODE)
    assert "get_node_logs" in step
    assert "search_node_logs는" in step  # 그쪽에 없다는 안내
    # 데이터 노드를 ClickHouse로 조회하라고 시키면 0건만 돌아온다.
    assert "search_node_logs(start_iso" not in step


def test_node_id_comes_from_the_master_log():
    assert "마스터 로그에서 확인한 노드 ID" in _step(_DATA_NODE)


def test_prompt_says_the_tool_resolves_ssh_details_itself():
    # get_node_logs가 내부에서 GET /_nodes/<id>로 ip·로그 경로를 얻는다.
    # 이것을 적어 두지 않으면 모델이 노드 정보를 먼저 조회하려 한다.
    step = _step(_DATA_NODE)
    assert "_nodes/" in step
    assert "따로 구할 필요가 없다" in step


def test_ssh_is_also_the_fallback_for_master_logs():
    assert 'get_node_logs("_master"' in _step('get_node_logs("_master"')


def test_the_item_letters_stay_consistent():
    """항목 문자와 본문의 상호 참조가 어긋나지 않는다.

    get_index_summary를 지웠을 때 a가 빠져 b~e가 한 칸씩 밀렸는데, 본문의
    "c단계에서 지목된"·"d단계에서 쓴다" 같은 참조는 그대로 남아 서로 다른
    항목을 가리켰다. 프롬프트는 실행되지 않으므로 이런 어긋남은 드러나지
    않는다.
    """
    step5 = SYSTEM_PROMPT.split("### 5단계")[1].split("### 6단계")[0]
    letters = _ITEM.findall(step5)
    assert letters == [f"{c}. " for c in "abcd"], letters

    # 마스터 로그 항목의 문자를, 그것을 참조하는 문장이 맞게 가리키는가.
    master_letter = _step(_MASTER)[0]
    assert f"{master_letter}단계에서 지목된" in step5
    assert f"{master_letter}단계 마스터 로그에서" in step5
    assert f"{master_letter}단계 조회가" in step5
