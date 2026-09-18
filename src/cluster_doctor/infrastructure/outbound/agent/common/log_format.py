"""로그 항목 한 줄을 사람이 읽는 문자열로 그린다.

프롬프트와 리포트가 같은 줄 모양을 써야 모델이 본 수치와 운영자가 읽는
수치가 갈리지 않는다. Main Agent(``supervisor``)와 분석 workflow가 함께
쓰므로 어느 한쪽에 두지 않는다.
"""

from functools import singledispatch

from cluster_doctor.domain.model.log_entry import (
    NodeLogEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.domain.model.node_metric import NodeMetricEntry

# 한 쿼리가 키워드 200개 넘게 싣고 오는 경우가 있다. 그대로 그리면 한 줄이
# 2,000자를 넘고(실측 2,029자), 같은 유저가 agg와 count로 같은 목록을 두 번
# 보내면 4,000자가 연달아 들어간다. 분당 입력 토큰 한도를 넘긴 주범이었다.
#
# 진단에 필요한 것은 "이 유저가 어떤 주제를 검색했나"이지 전량이 아니다.
# 앞 몇 개면 주제가 드러나고, 전체 개수는 따로 알려 주면 "키워드 215개짜리
# 쿼리"라는 사실도 근거로 쓸 수 있다.
_MAX_KEYWORDS_SHOWN = 5


def _format_keywords(keywords: tuple[str, ...]) -> str:
    if len(keywords) <= _MAX_KEYWORDS_SHOWN:
        return f"keyword={list(keywords)}"
    shown = list(keywords[:_MAX_KEYWORDS_SHOWN])
    return f"keyword={shown} 외 {len(keywords) - _MAX_KEYWORDS_SHOWN}개"


@singledispatch
def format_log_line(entry) -> str:
    """한 항목을 프롬프트 한 줄로 그린다. 소스마다 그리는 법이 다르다.

    표현을 도메인 모델이 아니라 여기 두는 이유: 어떤 값을 어떻게 보여줄지는
    프롬프트의 관심사다. 모델은 값을 값인 채로 들고 있기만 하면 된다.

    등록되지 않은 타입은 조용히 넘기지 않고 터뜨린다 — 새 소스를 추가하며
    렌더러를 잊으면 프롬프트에 빈 줄이 들어가고, 그 사실이 아무 데도 남지
    않는다.
    """
    raise TypeError(f"프롬프트 줄로 그릴 수 없는 항목입니다: {type(entry).__name__}")


@format_log_line.register
def _format_slowlog(entry: SlowlogEntry) -> str:
    return (
        f"  {entry.timestamp} [SLOWLOG] node={entry.node or '-'} "
        f"comp={entry.index_name or '-'} "
        f"took={entry.took}, {entry.total_hits}, shards={entry.total_shards}, "
        f"id={entry.opaque_id}, query={entry.query}"
    )


@format_log_line.register
def _format_query_log(entry: QueryLogEntry) -> str:
    line = (
        f"  {entry.timestamp} [{'SUCCESS' if entry.success else 'FAIL'}] "
        f"node={entry.host or '-'} comp={entry.service or '-'} "
        f"[{entry.cmd}] project={entry.project} env={entry.env} "
        f"cluster={entry.cluster} runtime={entry.run_time}s "
        f"{_format_keywords(entry.keywords)}"
    )
    if entry.company or entry.user:
        line += f" company={entry.company or '-'} user={entry.user or '-'}"
    return line


@format_log_line.register
def _format_node_log(entry: NodeLogEntry) -> str:
    """노드 로그 한 줄.

    ``level``이 비어 있으면 ``detected_level``로 대체한다 — 수집기에 따라
    한쪽만 채워진다. ``line``은 손대지 않는다. 스택 트레이스든 GC 통계든
    진단에 쓰이는 것은 원문 그대로이고, 잘라내면 근거로 인용할 수 없다.
    """
    level = (entry.level or entry.detected_level or "-").strip()
    role = f"/{entry.node_role}" if entry.node_role else ""
    return (
        f"  {entry.timestamp} [{level}] "
        f"node={entry.node or '-'}{role} comp={entry.filename or '-'} "
        f"logger={entry.logger or '-'} {entry.line}"
    )


@format_log_line.register
def _format_node_metric(entry: NodeMetricEntry) -> str:
    """노드 메트릭 한 줄.

    ``mem``이 아니라 ``os_mem(캐시포함)``으로 그린다. 이 값은
    ``GET _nodes/stats``의 ``os.mem.used_percent``이고 페이지 캐시를 포함한
    OS 전체 메모리다. ES는 남는 RAM을 파일시스템 캐시로 쓰므로 95~99%가
    정상인데, ``mem``이라고만 적어 두면 모델이 그것을 메모리 부족으로 읽고
    "메모리 사용률 95~99%로 매우 높음"을 문제점으로 써 올린다(실제로 그랬다).

    지시문으로도 같은 내용을 넣지만, 모델이 실제로 읽는 것은 이 줄이다.
    레이블을 고치는 편이 산문 한 줄보다 확실하고 토큰도 늘지 않는다.
    """
    return (
        f"  {entry.timestamp} [METRIC] "
        f"node={entry.node_name} ({entry.node_ip}) comp=- "
        f"cpu={entry.os_cpu_percent}% os_mem(캐시포함)={entry.os_mem_used_percent}% "
        f"proc_cpu={entry.process_cpu_percent}% "
        f"jvm_heap={entry.jvm_heap_used_percent}% "
        f"search(active={entry.search_active},queue={entry.search_queue},"
        f"rejected={entry.search_rejected}) "
        f"write(active={entry.write_active},queue={entry.write_queue},"
        f"rejected={entry.write_rejected})"
    )
