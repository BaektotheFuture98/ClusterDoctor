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

    def node_info(self, node_id: str) -> dict:
        # filter_path에 node_id를 리터럴로 넣으면 id에 포함된 '-' 등이
        # dot notation과 충돌할 수 있다. node_id는 nodes.info()의
        # 경로 파라미터로 이미 특정 노드를 지정하므로 응답은 단일 항목이며,
        # filter_path는 와일드카드 nodes.*로 두는 것이 안전하다.
        resp = self._client.nodes.info(
            node_id=node_id,
            filter_path=[
                "nodes.*.ip",
                "nodes.*.settings.path.logs",
                "nodes.*.settings.cluster.name",
            ],
        )
        nodes = dict(resp).get("nodes", {})
        data = next(iter(nodes.values()), {})
        if not data:
            return {}
        settings = data.get("settings") or {}
        return {
            "ip":           data.get("ip", ""),
            "log_path":     settings.get("path", {}).get("logs", ""),
            "cluster_name": settings.get("cluster", {}).get("name", ""),
        }
