"""노드 식별자를 접속 가능한 주소로 푸는 포트.

**LLM reasoning으로 구현하지 않는다.** 노드 주소는 추론할 것이 아니라 조회할
것이고, 모델이 IP를 지어내면 SSH가 엉뚱한 호스트에 붙거나(더 나쁘게는) 붙는다.

``ClusterRepository.node_info``가 이미 같은 조회를 하지만 계약이 다르다. 저쪽은
dict를 돌려주는 범용 ES 조회이고, 이쪽은 Node Investigation이 필요로 하는
``ResolvedNode``를 돌려준다 — 그 타입이 ``is_reachable()``로 "붙어 볼 수 있는
값이 다 있는가"를 한 자리에서 답한다.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from cluster_doctor.domain.model.elasticsearch.resolved_node import ResolvedNode


@runtime_checkable
class NodeResolver(Protocol):
    def resolve(self, node_id: str) -> ResolvedNode | None:
        """노드를 찾지 못하면 ``None``.

        예외를 올리지 않는 것은 "그런 노드가 없다"가 정상적인 답이기 때문이다.
        모델이 마스터 로그에서 뽑은 이름은 이미 클러스터를 떠난 노드일 수 있다.
        접속 실패와는 다르다 — 그쪽은 ``NodeLogFetcher``가 예외로 알린다.
        """
        ...
