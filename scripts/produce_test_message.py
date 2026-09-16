"""테스트용 slowlog 메시지를 Kafka 토픽에 전송한다.

브로커 주소와 토픽은 .env에서 읽는다. 여기에 하드코딩하면 운영 인프라
주소가 커밋되어 저장소를 보는 누구에게나 노출된다.

시각을 인자로 받는 이유: 예전에는 ``@timestamp``가 본문에 UTC 문자열로
박혀 있었다. 과거 구간을 재현하려면 그 줄을 매번 고쳐야 했고, ``Z``가
UTC라는 것을 잊으면 의도한 KST 시각에서 정확히 9시간 어긋난 구간을
분석하게 된다 — 조회가 성공하고 결과만 틀리므로 어디에서도 드러나지 않는
종류의 오류다. ``--at``은 오프셋이 없으면 KST로 읽고, ``--dry-run``이
보내기 전에 양쪽 표기를 모두 찍는다.

Kafka 없이 진단만 한 번 돌리려면 run_diagnosis.py를 쓴다. 시각 인자는
둘이 같다(_timeargs).

사용 예:
    uv run python scripts/produce_test_message.py
    uv run python scripts/produce_test_message.py --at "2026-08-27T14:00:00"
    uv run python scripts/produce_test_message.py --at "2026-08-27T14:00:00" --count 5 --span 3m
"""
import argparse
import asyncio
import json
import os
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

import _timeargs

load_dotenv()

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "slowlog")


def build_message(moment: datetime, seq: int) -> dict:
    """slowlog 한 건. ClusterGuard가 실제로 읽는 것은 ``@timestamp`` 뿐이다.

    나머지 필드는 실제 파이프라인 형태를 흉내 낸 것이고 진단에는 쓰이지
    않는다 — 내용 분석은 ClickHouse를 조회해서 하기 때문이다
    (consumer._parse_message를 볼 것).
    """
    return {
        "_index": ".ds-logs-elasticsearch.slowlog-default-2026.08.27-000001",
        "_id": f"test-message-{seq:03d}",
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
                    "id": (
                        "service=web,project=test,env=dev,company=1,user=100,"
                        "action=search,ip=127.0.0.1"
                    ),
                    "search_type": "QUERY_THEN_FETCH",
                },
                "index": {"name": "test-index-v1"},
                "shard": {"id": "0"},
            },
            # 파이프라인이 보내는 형태와 같게 UTC로 직렬화한다. consumer는
            # 오프셋이 붙은 값을 그대로 읽으므로 어느 표기든 동작하지만,
            # 실제 메시지와 다른 모양으로 테스트하면 그 차이가 드러나지 않는다.
            "@timestamp": moment.astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z",
            "message": "[test-index-v1][0]",
        },
    }


async def send(moments: list[datetime]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        for seq, moment in enumerate(moments, 1):
            message = build_message(moment, seq)
            # value가 JSON string 형태로 전송 (실제 파이프라인과 동일)
            value = json.dumps(json.dumps(message, ensure_ascii=False)).encode("utf-8")
            await producer.send_and_wait(TOPIC, value=value)
    finally:
        await producer.stop()
    print(f"\n✓ 테스트 메시지 {len(moments)}건 전송 완료 → topic={TOPIC}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="테스트용 slowlog 메시지를 Kafka에 보낸다.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "ClusterGuard consumer는 auto_offset_reset=latest다.\n"
            "먼저 `uv run python -m cluster_doctor.main`을 띄운 뒤 보낼 것."
        ),
    )
    _timeargs.add_arguments(parser)
    args = parser.parse_args()

    moments = _timeargs.resolve(args)

    print(f"  대상 토픽 : {TOPIC}")
    print(f"  브로커    : {BOOTSTRAP_SERVERS}")
    _timeargs.describe_moments(moments)

    if args.dry_run:
        print("\n(dry-run: 전송하지 않았다)")
        return
    asyncio.run(send(moments))


if __name__ == "__main__":
    main()
