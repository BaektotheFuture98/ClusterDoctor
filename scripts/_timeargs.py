"""시각 인자 처리. produce_test_message와 run_diagnosis가 함께 쓴다.

두 스크립트가 같은 파싱을 각자 들고 있으면 한쪽만 고쳤을 때 서로 다른
시각을 뜻하게 된다 — 같은 이유로 tools._parse_window가 세 벌의 복붙을
한 곳으로 모았다. 특히 "오프셋이 없으면 KST"라는 규칙은 두 스크립트가
반드시 같아야 한다. 어긋나면 프로듀서가 보낸 시각과 단독 실행이 분석한
시각이 9시간 벌어지고, 양쪽 다 성공하므로 드러나지 않는다.
"""
import argparse
import re
import sys
from datetime import datetime, timedelta, timezone


def force_utf8_console() -> None:
    """콘솔 출력을 UTF-8로 고정한다. import 시점에 부른다.

    Windows Python은 stdout 인코딩을 콘솔 코드페이지가 아니라 **로케일**로
    정한다. 한국어 Windows에서는 cp949라, 콘솔이 이미 UTF-8(chcp 65001)
    이어도 한글이 깨져 나온다(실측). 예전에는 실행할 때마다
    ``$env:PYTHONIOENCODING = "utf-8"``을 치게 안내했는데, 그것은 파이썬이
    뜨기 전에 정해져야 하는 값이라 .env로는 해결되지 않고, 빠뜨리면
    조용히 깨진 글자만 남는다.

    ``errors="replace"``를 주는 이유는 html_file_notifier._scrub과 같다 —
    인코딩할 수 없는 문자 하나가 출력 전체를 죽이면 안 된다.

    import 시점에 부르는 것은 side effect지만, 이 모듈을 쓰는 것은 같은
    디렉터리의 스크립트 둘뿐이고 둘 다 한국어를 찍는다. 호출을 각 스크립트에
    맡기면 한쪽이 빠뜨렸을 때 그 스크립트만 조용히 깨진다.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError, ValueError):
            # reconfigure가 없는 스트림(교체된 stdout 등)이거나 이미 닫힌 경우.
            # 출력 인코딩을 못 고치는 것이 스크립트를 막을 이유는 아니다.
            pass


# 두 스크립트도 아무 것도 찍기 전에 이 모듈을 import한다.
force_utf8_console()

KST = timezone(timedelta(hours=9))

# tools._base_time이 기준 시각을 kafka_receive_time으로 바꾸는 경계.
# 같은 값이어야 안내가 실제 동작과 맞는다.
PIPELINE_LAG_THRESHOLD = timedelta(minutes=30)

_SPAN_UNITS = {"s": 1, "m": 60, "h": 3600}


def parse_at(value: str) -> datetime:
    """ISO 8601 문자열을 timezone-aware로 만든다. 오프셋이 없으면 KST로 읽는다.

    ``replace(tzinfo=KST)``를 무조건 쓰지 않는 이유는 tools._parse_kst와
    같다. 그것은 변환이 아니라 덮어쓰기라, ``"...Z"``나 ``"+00:00"``이 붙어
    온 순간을 같은 벽시계의 KST로 재해석해 9시간 어긋나게 한다.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"시각을 읽을 수 없다: {value!r} ({exc})"
        ) from None
    if parsed.utcoffset() is None:
        return parsed.replace(tzinfo=KST)
    return parsed


def parse_span(value: str) -> timedelta:
    """``"90"`` ``"3m"`` ``"1h"`` 형태를 timedelta로. 단위가 없으면 초다."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([smh]?)", value.strip())
    if not match:
        raise argparse.ArgumentTypeError(
            f"구간을 읽을 수 없다: {value!r} (예: 90, 3m, 1h)"
        )
    return timedelta(
        seconds=float(match.group(1)) * _SPAN_UNITS[match.group(2) or "s"]
    )


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """--at / --count / --span / --dry-run 을 붙인다.

    두 스크립트의 인자 이름과 기본값이 같아야 한다. 프로듀서로 보낸 것과
    같은 인자를 단독 실행에 그대로 붙여넣을 수 있어야 비교가 성립한다.
    """
    parser.add_argument(
        "--at",
        type=parse_at,
        default=None,
        help="slowlog 발생 시각. ISO 8601. 오프셋이 없으면 KST로 읽는다. "
        '예: "2026-08-27T14:00:00" (기본값: 현재 시각)',
    )
    parser.add_argument("--count", type=int, default=1, help="건수 (기본값: 1)")
    parser.add_argument(
        "--span",
        type=parse_span,
        default=timedelta(),
        help="건수를 흩뿌릴 구간. 예: 90, 3m, 1h (기본값: 0 = 전부 같은 시각)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실행하지 않고 계획만 출력한다.",
    )


def plan_moments(at: datetime, count: int, span: timedelta) -> list[datetime]:
    """``count``건을 ``at``부터 ``span``에 균등하게 배치한다.

    ``span``이 0이면 전부 같은 시각이다 — 한 순간에 몰린 유입을 뜻하며,
    그것도 유효한 시나리오다.
    """
    if count == 1 or not span:
        return [at] * count
    step = span / (count - 1)
    return [at + step * i for i in range(count)]


def resolve(args) -> list[datetime]:
    """파싱된 인자를 시각 목록으로. count 검증까지 여기서 한다."""
    if args.count < 1:
        raise SystemExit("오류: --count는 1 이상이어야 한다.")
    at = args.at or datetime.now(timezone.utc)
    return plan_moments(at, args.count, args.span)


def describe_moments(moments: list[datetime]) -> None:
    """시각 목록을 KST/UTC 양쪽으로 찍고, 30분 경계를 넘으면 알린다."""
    print(f"  건수      : {len(moments)}")
    print("  시각 (KST → UTC)")
    for i, moment in enumerate(moments, 1):
        kst = moment.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
        utc = moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"    {i:3d}. {kst} KST  →  {utc}")

    lag = datetime.now(timezone.utc) - min(moments)
    if lag > PIPELINE_LAG_THRESHOLD:
        print()
        print(
            f"  주의: 가장 이른 시각이 현재보다 {lag} 앞이라 30분 경계를 넘는다.\n"
            "        리포트의 '사용한 시각 기준'은 'kafka_receive_time\n"
            "        (파이프라인 지연 30분 초과)'로 표기된다. 다만 분석 구간\n"
            "        자체는 check_new_slowlogs가 큐 항목의 실제 시각으로\n"
            "        first_seen을 당기므로 위 시각을 따라간다."
        )
