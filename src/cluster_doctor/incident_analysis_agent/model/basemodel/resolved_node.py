from pydantic import BaseModel, ConfigDict


class ResolvedNode(BaseModel):
    """노드 식별자를 접속 가능한 주소로 푼 결과.

    ``NodeResolver``가 만든다. LLM은 이 값을 만들지 않는다 — 노드 주소는 추론할
    것이 아니라 조회할 것이다.
    """

    model_config = ConfigDict(frozen=True)

    node_id: str
    node_name: str = ""
    host: str = ""
    log_path: str | None = None
    cluster_name: str = ""

    def is_reachable(self) -> bool:
        """SSH로 붙어 볼 만한 값이 다 있는가."""
        return bool(self.host and self.log_path)
