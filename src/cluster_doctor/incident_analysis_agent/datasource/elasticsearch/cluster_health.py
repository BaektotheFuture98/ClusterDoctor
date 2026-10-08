"""클러스터 상태 조회.

진단이 실제로 쓰는 두 가지 조회를 덮는다. 노드 주소를 푸는 일은
``node_resolver.py``가 따로 맡는다 — 돌려주는 타입이 다르고(``ResolvedNode``),
쓰는 쪽도 Node Investigation 하나뿐이다.
"""

from abc import ABC, abstractmethod

from elasticsearch import Elasticsearch


class ClusterRepository(ABC):
    """클러스터 상태 조회 포트."""

    @abstractmethod
    def health(self) -> dict:
        """클러스터 헬스. status(green/yellow/red), 샤드 수, 노드 수를 담는다."""
        ...

    @abstractmethod
    def explain_allocation(self) -> dict:
        """미할당 샤드가 배정되지 못한 이유.

        할당 문제가 없으면 구현체가 예외를 올릴 수 있다. 그것을 무엇으로
        번역할지는 호출자가 정한다.
        """
        ...


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
        # 미할당 샤드가 없으면 ES가 400을 돌려준다. 여기서 삼키지 않는다 —
        # "샤드 문제 없음"과 "ES에 못 붙었음"은 다른 사실이다.
        return dict(self._client.cluster.allocation_explain())
