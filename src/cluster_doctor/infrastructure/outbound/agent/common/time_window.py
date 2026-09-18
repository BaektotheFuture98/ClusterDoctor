"""KST 시각 변환과 구간 계산.

Supervisor와 진단 쪽이 함께 쓴다. 두 계층이 서로를 import하지 않아야
하므로 공통분을 여기에 둔다.
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


def fmt(moment: datetime) -> str:
    """시각 한 개의 표기. 프롬프트와 로그가 같은 모양을 쓰게 한다.

    Supervisor 스냅샷과 진단 쪽이 함께 쓴다. 각자 ``strftime``을 부르면 한쪽만
    형식이 바뀌는 날이 오고, 그때 두 값이 같은 시각인지 눈으로 봐서는 알 수
    없다.
    """
    return moment.astimezone(KST).strftime("%Y-%m-%dT%H:%M:%S")
