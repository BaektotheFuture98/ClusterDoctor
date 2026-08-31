"""테스트용 slowlog 메시지를 Kafka 토픽에 전송한다.

브로커 주소와 토픽은 .env에서 읽는다. 여기에 하드코딩하면 운영 인프라
주소가 커밋되어 저장소를 보는 누구에게나 노출된다.
"""
import asyncio
import json
import os
import sys

from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "slowlog")

TEST_MESSAGE = {
    "_index": ".ds-logs-elasticsearch.slowlog-default-2026.08.27-000001",
    "_id": "test-message-001",
    "_score": None,
    "_source": {
        "log": {
            "level": "WARN",
            "logger": "index.search.slowlog.query",
        },
        "elasticsearch": {
            "node": {"name": "test-node-01", "id": "abc123"},
            "cluster": {"name": "test-cluster", "uuid": "test-uuid"},
            "slowlog": {
                "took": "5.2s",
                "total_shards": 10,
                "total_hits": "42 hits",
                "stats": "[]",
                "source": '{"size":10,"query":{"match_all":{}}}',
                "id": "service=web,project=test,env=dev,company=1,user=100,action=search,ip=127.0.0.1",
                "search_type": "QUERY_THEN_FETCH",
            },
            "index": {"name": "test-index-v1"},
            "shard": {"id": "0"},
        },
        "@timestamp": "2026-08-27T05:00:00.000Z",
        "message": "[test-index-v1][0]",
    },
}


async def main() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        # value가 JSON string 형태로 전송 (실제 파이프라인과 동일)
        value = json.dumps(json.dumps(TEST_MESSAGE, ensure_ascii=False)).encode("utf-8")
        await producer.send_and_wait(TOPIC, value=value)
        print(f"✓ 테스트 메시지 전송 완료 → topic={TOPIC}")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
