"""유입 시각 기준과 정착 판정.

기준 시각은 리포트의 "사용한 시각 기준" 필드로 그대로 간다. 그래야 그 값이
모델의 주장이 아니라 관측된 사실이 된다.
"""

from datetime import datetime, timedelta

from cluster_doctor.incident.inflow import (
    SETTLED_ZERO_STREAK,
    InflowTracker,
    base_time,
)
from cluster_doctor.agent.integrations.clickhouse.models import SlowlogEntry
from tests.contracts.test_time_range_spans import KST

TRIGGER = datetime(2026, 9, 17, 3, 0, tzinfo=KST)


def entry(at: datetime) -> SlowlogEntry:
    return SlowlogEntry(timestamp=at)


class TestBaseTime:
    def test_보통은_slowlog_시각을_쓴다(self):
        moment, basis = base_time(TRIGGER, TRIGGER + timedelta(seconds=30))

        assert moment == TRIGGER
        assert basis == "slowlog_timestamp"

    def test_수신보다_미래면_clock_skew로_본다(self):
        """slowlog가 수신보다 미래일 수는 없다."""
        received = TRIGGER - timedelta(minutes=1)

        moment, basis = base_time(TRIGGER, received)

        assert moment == received
        assert "clock skew" in basis

    def test_30분_넘게_늦으면_파이프라인_지연으로_본다(self):
        received = TRIGGER + timedelta(minutes=31)

        moment, basis = base_time(TRIGGER, received)

        assert moment == received
        assert "파이프라인 지연" in basis

    def test_30분_이내_지연은_slowlog_시각을_그대로_쓴다(self):
        _moment, basis = base_time(TRIGGER, TRIGGER + timedelta(minutes=29))

        assert basis == "slowlog_timestamp"


class TestTracker:
    def test_아무것도_안_왔으면_연속_0을_센다(self):
        tracker = InflowTracker.from_trigger(TRIGGER, TRIGGER)

        tracker.observe([], now=TRIGGER)

        assert tracker.zero_streak == 1
        assert tracker.settled is False

    def test_연속_2회_0이면_정착으로_본다(self):
        """1로는 부족하다 — 커넥터 폴링 주기 때문에 유입이 계속되는 중에도
        한 번은 0건이 나올 수 있다."""
        tracker = InflowTracker.from_trigger(TRIGGER, TRIGGER)

        for _ in range(SETTLED_ZERO_STREAK):
            tracker.observe([], now=TRIGGER)

        assert tracker.settled is True

    def test_유입이_다시_오면_연속_0이_풀린다(self):
        tracker = InflowTracker.from_trigger(TRIGGER, TRIGGER)
        tracker.observe([], now=TRIGGER)

        tracker.observe([entry(TRIGGER)], now=TRIGGER)

        assert tracker.zero_streak == 0

    def test_관측된_것이_더_이르면_시작을_당긴다(self):
        """재트리거로 실행된 경우 기준 시각이 실제 발생보다 늦을 수 있다."""
        tracker = InflowTracker.from_trigger(TRIGGER, TRIGGER)
        earlier = TRIGGER - timedelta(minutes=3)

        tracker.observe([entry(earlier)], now=TRIGGER)

        assert tracker.first_seen == earlier

    def test_끝은_바깥쪽으로_넓힌다(self):
        tracker = InflowTracker.from_trigger(TRIGGER, TRIGGER)
        later = TRIGGER + timedelta(minutes=2)

        tracker.observe([entry(later)], now=later)

        assert tracker.last_seen == later

    def test_미래_시각은_현재로_눌러_쓴다(self):
        """노드 시계가 앞서 있으면 last_seen이 미래가 되고, first_seen은 clock
        skew를 잡아 눌러 둔 값이라 구간이 "수신 시각 ~ 미래"로 벌어진다 —
        실측에서 3시간짜리 구간이 됐다."""
        tracker = InflowTracker.from_trigger(TRIGGER, TRIGGER)
        future = TRIGGER + timedelta(hours=3)

        tracker.observe([entry(future)], now=TRIGGER)

        assert tracker.last_seen == TRIGGER
