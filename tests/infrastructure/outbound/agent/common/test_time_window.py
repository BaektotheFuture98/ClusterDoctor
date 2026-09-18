from datetime import datetime, timezone

from cluster_doctor.infrastructure.outbound.agent.common.time_window import (
    KST,
    fmt,
    parse_kst,
)

UTC = timezone.utc


def test_naive_iso_is_read_as_kst():
    assert parse_kst("2026-09-17T03:00:00") == datetime(2026, 9, 17, 3, tzinfo=KST)


def test_utc_iso_is_converted_not_overwritten():
    # 덮어쓰기였다면 03:00 KST가 되어 9시간 어긋난다.
    assert parse_kst("2026-09-17T03:00:00+00:00") == datetime(2026, 9, 17, 12, tzinfo=KST)


def test_fmt_renders_in_kst():
    assert fmt(datetime(2026, 9, 17, 3, tzinfo=UTC)) == "2026-09-17T12:00:00"


