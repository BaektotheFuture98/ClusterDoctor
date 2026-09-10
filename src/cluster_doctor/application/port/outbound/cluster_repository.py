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

    @abstractmethod
    def node_info(self, node_id: str) -> dict:
        """단일 노드의 IP와 로그 디렉터리 경로를 반환한다.

        반환 키:
          ip           — SSH 접속에 쓸 transport 주소
          log_path     — nodes.*.settings.path.logs
          cluster_name — nodes.*.settings.cluster.name (로그 파일명 구성에 사용)

        노드를 찾지 못하면 빈 dict를 돌려준다.
        """
        ...
