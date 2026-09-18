"""노드 식별자를 접속 정보로 푸는 Elasticsearch 어댑터.

``GET /_nodes/<id>``는 노드 id와 **이름**을 모두 받는다. 마스터 로그가 지목하는
것은 대개 이름(``es-data-03``)이므로 그 성질이 중요하다 — 이름을 id로 바꾸는
단계를 따로 두면 그 단계가 또 하나의 실패 지점이 된다.

찾지 못하면 ``None``이다. 예외를 올리지 않는 이유: 마스터 로그에 남은 노드가
이미 클러스터를 떠났을 수 있고, 그것은 조회의 실패가 아니라 조회의 답이다.
"""

from __future__ import annotations

import logging

from elasticsearch import Elasticsearch

from cluster_doctor.domain.model.evidence import ResolvedNode

_logger = logging.getLogger(__name__)

_REQUEST_FIELDS = [
    "nodes.*.name",
    "nodes.*.ip",
    "nodes.*.settings.path.logs",
    "nodes.*.settings.cluster.name",
]


class ElasticsearchNodeResolver:
    def __init__(self, client: Elasticsearch) -> None:
        self._client = client

    def resolve(self, node_id: str) -> ResolvedNode | None:
        identifier = (node_id or "").strip()
        if not identifier:
            return None

        # filter_path에 node_id를 리터럴로 넣지 않는다. id에 포함된 '-' 등이
        # dot notation과 충돌한다. 경로 파라미터가 이미 단일 노드를 지정하므로
        # 와일드카드 nodes.*로 두는 것이 안전하다.
        response = self._client.nodes.info(
            node_id=identifier, filter_path=_REQUEST_FIELDS
        )
        nodes = dict(response).get("nodes", {})
        if not nodes:
            _logger.info("[resolver] 노드를 찾지 못했다: %s", identifier)
            return None

        es_node_id, data = next(iter(nodes.items()))
        settings = data.get("settings") or {}
        return ResolvedNode(
            node_id=es_node_id,
            node_name=data.get("name", "") or identifier,
            host=data.get("ip", ""),
            log_path=(settings.get("path") or {}).get("logs") or None,
            cluster_name=(settings.get("cluster") or {}).get("name", ""),
        )
