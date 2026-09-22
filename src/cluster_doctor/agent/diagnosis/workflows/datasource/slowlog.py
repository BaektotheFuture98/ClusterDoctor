"""slowlog datasource의 Triage 설정과 레코드 변환.

slowlog는 임계치를 넘은 쿼리만 기록된다. 그래서 "느린 쿼리가 있다"는 것 자체는
정보가 아니다 — 전부 느리다. 의미는 얼마나, 언제부터, 어느 노드·인덱스에
몰렸는가에 있다.
"""

from __future__ import annotations

from cluster_doctor.contracts.evidence import EvidenceSource
from cluster_doctor.agent.integrations.clickhouse.models import SlowlogEntry
from cluster_doctor.agent.diagnosis.workflows.triage.spec import TriageSpec
from cluster_doctor.agent.diagnosis.workflows.triage.state import RawRecord

SPEC = TriageSpec(
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


def to_records(entries: list[SlowlogEntry]) -> list[RawRecord]:
    """slowlog 항목을 Triage 레코드로. 시간순 번호를 붙인다."""
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
