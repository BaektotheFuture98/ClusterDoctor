"""Kafka 메시지 → SlowlogEntry 파싱.

timestamp는 반드시 timezone-aware여야 한다. naive가 하나라도 섞이면
check_new_slowlogs의 sorted()가 TypeError로 터져 agent 실행이 통째로
죽고, 그 시점엔 큐가 이미 비워진 뒤라 인시던트가 사라진다. astimezone도
naive 값에 대해서는 호스트 로컬 시간대를 가정해 구간을 9시간 어긋나게 한다.
"""

from datetime import timezone

import pytest

from cluster_doctor.infrastructure.inbound.kafka.consumer import _parse_message

KST_ISO = "2026-08-28T10:20:15.123000+09:00"
UTC_ISO = "2026-08-28T01:20:15.123000Z"
NAIVE_ISO = "2026-08-28T10:20:15.123000"


@pytest.mark.parametrize(
    "raw",
    [KST_ISO, UTC_ISO, NAIVE_ISO, 1788000015123, 1788000015123.0, None],
    ids=["kst", "utc-z", "naive", "epoch-int", "epoch-float", "없음"],
)
def test_timestamp_is_always_aware(raw):
    entry = _parse_message({"_source": {"@timestamp": raw}})
    assert entry.timestamp.utcoffset() is not None, f"naive가 새어 나왔다: {raw!r}"


def test_malformed_timestamp_raises_so_the_caller_can_warn():
    # 여기서 삼키면 안 된다. run()이 이미 파싱 실패를 잡아
    # "partition=... 파싱 실패, 수신 시각으로 폴백" 경고를 남기고 aware 시각으로
    # 대체한다(consumer.py의 run()). _parse_message가 조용히 폴백하면 그 경고가
    # 영영 뜨지 않아, 형식이 깨진 메시지가 계속 들어와도 알 수 없게 된다.
    with pytest.raises(ValueError):
        _parse_message({"_source": {"@timestamp": "말이 안 되는 값"}})


def test_offset_bearing_input_keeps_its_instant():
    # +09:00 10:20:15 와 UTC 01:20:15 는 같은 순간이다.
    assert _parse_message({"_source": {"@timestamp": KST_ISO}}).timestamp == \
        _parse_message({"_source": {"@timestamp": UTC_ISO}}).timestamp


def test_naive_input_is_read_as_utc():
    entry = _parse_message({"_source": {"@timestamp": NAIVE_ISO}})
    assert entry.timestamp.utcoffset().total_seconds() == 0
    assert entry.timestamp.hour == 10


def test_source_wrapper_is_optional():
    # 커넥터가 ES 검색 hit을 그대로 보내므로 보통 _source가 있지만,
    # 평평한 형태로 오는 경우도 받아준다.
    assert _parse_message({"@timestamp": KST_ISO}).timestamp.utcoffset() is not None


def test_alternative_timestamp_keys_are_accepted():
    for key in ("timestamp", "time"):
        entry = _parse_message({"_source": {key: KST_ISO}})
        assert entry.timestamp.utcoffset() is not None
