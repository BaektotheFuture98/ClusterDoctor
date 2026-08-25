from elasticsearch import Elasticsearch

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository


class ElasticsearchClusterAdapter(ClusterRepository):
    def __init__(self, client: Elasticsearch):
        self._client = client

    def health(self) -> dict:
        return dict(self._client.cluster.health())
