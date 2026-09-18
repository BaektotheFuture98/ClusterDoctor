from elasticsearch import Elasticsearch

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository


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
