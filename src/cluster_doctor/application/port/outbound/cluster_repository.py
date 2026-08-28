from abc import ABC, abstractmethod


class ClusterRepository(ABC):
    """클러스터 상태 조회 포트.

    agent 도구가 실제로 쓰는 세 가지 조회를 모두 덮는다. health()만 두면
    나머지 둘은 여전히 인프라 클라이언트를 직접 쓰게 되어, 포트를 두는 의미가
    사라진다 — 실제로 그런 상태였다.
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

    @abstractmethod
    def index_summary(self, index_pattern: str) -> list[dict]:
        """인덱스 패턴에 걸리는 인덱스들의 상태 요약."""
        ...
