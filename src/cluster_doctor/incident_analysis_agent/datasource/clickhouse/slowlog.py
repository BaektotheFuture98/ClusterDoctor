"""slowlog datasource: ClickHouse 조회 + 선별 설정 + 레코드 변환.

slowlog는 임계치를 넘은 쿼리만 기록된다. 그래서 "느린 쿼리가 있다"는 것 자체는
정보가 아니다 — 전부 느리다. 의미는 얼마나, 언제부터, 어느 노드·인덱스에
몰렸는가에 있다.
"""

from __future__ import annotations

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    query_segment,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import EvidenceSource
from cluster_doctor.incident_analysis_agent.model.basemodel.log_entries import LogEntry, SlowlogEntry
from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.spec import AnalysisSpec
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.state import RawRecord

SPEC = AnalysisSpec(
    source=EvidenceSource.SLOWLOG,
    label="slowlog (느린 쿼리 로그)",
    what_matters=(
        "- 그 구간에서 유독 오래 걸린 쿼리. took이 다른 줄보다 한 자릿수 이상 큰 것.\n"
        "- 특정 인덱스나 노드에 몰린 쿼리.\n"
        "- 같은 형태의 쿼리가 갑자기 느려진 시점.\n"
        "- total_shards가 유독 큰 광역 검색."
    ),
    what_is_noise=(
        "- 임계치를 조금 넘긴 정도의, 그 구간 내내 비슷하게 나오는 쿼리.\n"
        "- 내용이 같고 시각만 다른 반복 쿼리 (대표 한 줄만 남긴다).\n"
        "- 이 소스에는 애초에 임계 초과 쿼리만 들어온다. '느리다'는 사실만으로는\n"
        "  고를 이유가 되지 않는다."
    ),
)


def fetch(client, table: str, tr: TimeRange) -> list[LogEntry]:
    """slowlog를 *발생* 시각 기준으로 조회한다.

    ``ch_ingested_at``이 아니라 ``_source.@timestamp``로 거르는 이유:
    전자는 ClickHouse 적재 시각이고 후자가 ES가 slowlog를 남긴 실제 시각이다.
    실측 지연은 23~41초(평균 31초)로, 분 경계를 넘기는 것만으로 트리거를
    유발한 바로 그 slowlog가 조회 구간에서 빠진다.

    ``_source``를 통째로 가져오지 않는 이유는 두 가지다. clickhouse-connect는
    JSON 타입을 ``dict``로 돌려주므로 ``SlowlogEntry``의 문자열 필드에 dict가
    들어가고, 행당 2.7KB 중 진단에 쓰이는 것은 27%뿐이다 -- 나머지는 프롬프트
    토큰만 먹는다. 필요한 서브컬럼만 이름으로 투영한다.
    """
    sql = (
        "SELECT _source.`@timestamp`, "
        "_source.elasticsearch.index.name, _source.elasticsearch.node.name, "
        "_source.elasticsearch.slowlog.took, _source.elasticsearch.slowlog.total_hits, "
        "_source.elasticsearch.slowlog.total_shards, _source.elasticsearch.slowlog.id, "
        f"_source.elasticsearch.slowlog.source FROM {table} "
        "WHERE _source.`@timestamp` >= %(from_)s AND _source.`@timestamp` < %(to)s "
        f"LIMIT {MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
    )
    # row 인덱스: 0=발생 시각, 1=인덱스명, 2=노드명, 3=took,
    #             4=total_hits, 5=total_shards, 6=x-opaque-id, 7=쿼리 원문
    return [
        SlowlogEntry(
            timestamp=row[0],
            index_name=row[1],
            node=row[2],
            took=row[3],
            total_hits=row[4],
            total_shards=row[5],
            opaque_id=row[6],
            query=row[7],
        )
        for row in query_segment(client, sql, tr, "slowlog")
    ]


def to_records(entries: list[SlowlogEntry]) -> list[RawRecord]:
    """slowlog 항목을 선별 레코드로. 시간순 번호를 붙인다."""
    ordered = sorted(entries, key=lambda entry: entry.timestamp)
    return [
        RawRecord(
            record_id=index,
            event_time=entry.timestamp,
            line=(
                f"took={entry.took or '?'} index={entry.index_name or '?'} "
                f"node={entry.node or '?'} hits={entry.total_hits or '?'} "
                f"shards={entry.total_shards} query={entry.query or '(없음)'}"
            ),
            node_name=entry.node or None,
        )
        for index, entry in enumerate(ordered, start=1)
    ]
