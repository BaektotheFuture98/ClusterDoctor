"""KST 시각 변환과 구간 계산.

``tools.py``와 ``diagnosis_state.py``가 함께 쓴다. 두 모듈이 서로를
import하지 않아야 하므로 공통분을 여기에 둔다.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


def parse_kst(iso: str) -> datetime:
    """ISO 문자열을 KST-aware datetime으로 만든다.

    ``replace(tzinfo=KST)``를 쓰면 안 된다. 그것은 변환이 아니라 덮어쓰기라
    ``"...Z"``나 ``"+00:00"``이 붙어 온 순간을 같은 벽시계의 KST로 재해석해
    정확히 9시간 어긋난 구간을 조회한다. 프롬프트가 KST를 지시하더라도
    모델이 지시를 어길 수 있고, 이 오류는 조회가 성공하고 결과만 틀리므로
    어디에서도 드러나지 않는다.
    """
    parsed = datetime.fromisoformat(iso)
    if parsed.utcoffset() is None:
        return parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def parse_window(
    start_iso: str, end_iso: str
) -> tuple[datetime | None, datetime | None, str | None]:
    """두 ISO 문자열을 KST 구간으로 만든다. ``(start, end, 오류문자열)``.

    실패를 예외가 아니라 세 번째 항목으로 돌려주는 이유: 호출자는 어차피
    문자열을 반환해야 한다. tool에서 예외가 새면 agent 실행 전체가 중단되므로
    각 tool이 반드시 잡아야 하는데, 그러면 잡는 코드가 다시 여러 벌이 된다.
    """
    try:
        return parse_kst(start_iso), parse_kst(end_iso), None
    except ValueError as exc:
        return None, None, f"시각 파싱 오류: {exc}"


def fmt(moment: datetime) -> str:
    """프롬프트로 나가는 시각 표기. analyze_logs가 받는 형식과 같다."""
    return moment.astimezone(KST).strftime("%Y-%m-%dT%H:%M:%S")


def merge_intervals(
    intervals: list[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    """겹치거나 맞닿은 구간을 합친다."""
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged
