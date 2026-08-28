from elasticsearch import Elasticsearch

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository

# cat API는 h를 주지 않으면 20여 개 컬럼을 전부 돌려준다. 이 결과는 그대로
# LLM 프롬프트에 실리므로 진단에 쓰는 것만 요청한다.
_INDEX_SUMMARY_COLUMNS = [
    "index", "health", "status", "docs.count", "store.size", "segments.count",
]


class ElasticsearchClusterAdapter(ClusterRepository):
    """ClusterRepository의 Elasticsearch 구현.

    포트 밖으로는 항상 순수 dict/list를 내보낸다. elasticsearch-py는
    ``ObjectApiResponse``를 돌려주는데, 그것이 그대로 새어 나가면 포트를 둔
    의미가 사라지고 호출자가 인프라 타입에 묶인다.
    """

    def __init__(self, client: Elasticsearch):
        self._client = client

    def health(self) -> dict:
        return dict(self._client.cluster.health())

    def explain_allocation(self) -> dict:
        # 미할당 샤드가 없으면 ES가 400을 돌려준다. 그것을 여기서 삼키지
        # 않는다 — "샤드 문제 없음"과 "ES에 못 붙었음"은 다른 사실이고,
        # 무엇을 리포트에 쓸지는 호출자가 판단한다.
        return dict(self._client.cluster.allocation_explain())

    def index_summary(self, index_pattern: str) -> list[dict]:
        rows = self._client.cat.indices(
            index=index_pattern,
            h=_INDEX_SUMMARY_COLUMNS,
            format="json",
        )
        return [dict(row) for row in rows]
