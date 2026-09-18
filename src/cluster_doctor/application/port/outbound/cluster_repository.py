from abc import ABC, abstractmethod


class ClusterRepository(ABC):
    """클러스터 상태 조회 포트.

    진단이 실제로 쓰는 두 가지 조회를 덮는다. 노드 주소를 푸는 일은
    ``NodeResolver``가 따로 맡는다 — 돌려주는 타입이 다르고(``ResolvedNode``),
    쓰는 쪽도 Node Investigation 하나뿐이다.
    """

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
