"""ClusterRepository 포트의 Elasticsearch 구현.

포트는 agent 도구가 실제로 쓰는 세 가지 ES 조회를 모두 덮는다. health()만
포트에 두면 나머지 둘은 여전히 raw 클라이언트를 직접 쓰게 되어, 포트를 두는
의미가 없어진다.
"""

from unittest.mock import MagicMock

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.infrastructure.outbound.elasticsearch.es_cluster_adapter import (
    ElasticsearchClusterAdapter,
)


def test_adapter_satisfies_the_port():
    # 추상 메서드가 하나라도 남아 있으면 인스턴스화에서 TypeError가 난다.
    adapter = ElasticsearchClusterAdapter(MagicMock())
    assert isinstance(adapter, ClusterRepository)


def test_health_returns_a_plain_dict():
    # ES 클라이언트는 ObjectApiResponse를 돌려준다. 포트 밖으로는 dict만 나간다.
    client = MagicMock()
    client.cluster.health.return_value = {"status": "green", "number_of_nodes": 3}

    result = ElasticsearchClusterAdapter(client).health()

    assert result == {"status": "green", "number_of_nodes": 3}
    assert type(result) is dict
    client.cluster.health.assert_called_once_with()


def test_explain_allocation_returns_a_plain_dict():
    client = MagicMock()
    client.cluster.allocation_explain.return_value = {"index": "x", "can_allocate": "no"}

    result = ElasticsearchClusterAdapter(client).explain_allocation()

    assert result == {"index": "x", "can_allocate": "no"}
    assert type(result) is dict
    client.cluster.allocation_explain.assert_called_once_with()


def test_index_summary_requests_only_the_diagnostic_columns():
    client = MagicMock()
    client.cat.indices.return_value = [
        {"index": "logs-1", "health": "green", "docs.count": "42"}
    ]

    result = ElasticsearchClusterAdapter(client).index_summary("logs-*")

    assert result == [{"index": "logs-1", "health": "green", "docs.count": "42"}]
    kwargs = client.cat.indices.call_args.kwargs
    assert kwargs["index"] == "logs-*"
    assert kwargs["format"] == "json"
    # cat API는 h를 주지 않으면 20여 개 컬럼을 전부 돌려준다. 진단에 쓰는
    # 것만 요청해 프롬프트에 실리는 양을 줄인다.
    assert kwargs["h"] == [
        "index", "health", "status", "docs.count", "store.size", "segments.count"
    ]


def test_index_summary_returns_plain_dicts():
    client = MagicMock()
    client.cat.indices.return_value = [{"index": "a"}]

    rows = ElasticsearchClusterAdapter(client).index_summary("a*")

    assert all(type(r) is dict for r in rows)
