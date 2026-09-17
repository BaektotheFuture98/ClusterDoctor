from datetime import datetime, timedelta, timezone

from cluster_doctor.infrastructure.outbound.llm.deepagent.time_window import (
    KST,
    fmt,
    merge_intervals,
    parse_kst,
    parse_window,
)

UTC = timezone.utc


def test_naive_iso_is_read_as_kst():
    assert parse_kst("2026-09-17T03:00:00") == datetime(2026, 9, 17, 3, tzinfo=KST)


def test_utc_iso_is_converted_not_overwritten():
    # 덮어쓰기였다면 03:00 KST가 되어 9시간 어긋난다.
    assert parse_kst("2026-09-17T03:00:00+00:00") == datetime(2026, 9, 17, 12, tzinfo=KST)


def test_parse_window_returns_error_string_instead_of_raising():
    start, end, error = parse_window("not-a-time", "2026-09-17T03:00:00")
    assert (start, end) == (None, None)
    assert "시각 파싱 오류" in error


def test_fmt_renders_in_kst():
    assert fmt(datetime(2026, 9, 17, 3, tzinfo=UTC)) == "2026-09-17T12:00:00"


def test_merge_intervals_joins_overlaps():
    base = datetime(2026, 9, 17, 3, tzinfo=KST)
    merged = merge_intervals([
        (base, base + timedelta(minutes=10)),
        (base + timedelta(minutes=5), base + timedelta(minutes=20)),
        (base + timedelta(minutes=40), base + timedelta(minutes=50)),
    ])
    assert merged == [
        (base, base + timedelta(minutes=20)),
        (base + timedelta(minutes=40), base + timedelta(minutes=50)),
    ]
