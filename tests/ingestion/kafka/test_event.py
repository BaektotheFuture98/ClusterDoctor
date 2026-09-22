from datetime import datetime, timezone

from cluster_doctor.ingestion.kafka.event import SlowlogTriggerEvent


def test_slowlog_trigger_event_holds_only_timestamp():
    """트리거 이벤트는 발생 시각 하나만 안다.

    ClickHouse 상세 SlowlogEntry를 재사용하지 않는다는 것을 필드 집합으로
    고정한다 — 누군가 나중에 index_name 등을 이 타입에 슬쩍 추가하면 이
    테스트가 실패해, "트리거는 시각만 안다"는 경계가 다시 무너지는 것을 잡는다.
    """
    now = datetime(2026, 9, 22, 13, 0, 0, tzinfo=timezone.utc)
    event = SlowlogTriggerEvent(timestamp=now)

    assert event.timestamp == now
    assert {f.name for f in __import__("dataclasses").fields(event)} == {"timestamp"}
