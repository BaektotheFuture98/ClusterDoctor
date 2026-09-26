"""관측값에서 따라 나오는 심각도.

모델이 ``severity``를 채우지 않는 일이 반복됐다 — 프롬프트에 "반드시
고른다"를 두 군데 넣은 뒤에도 그랬고, 그래서 노드 이탈과 GREEN→YELLOW
전환이 분류 없이 리포트에 실렸다(실측 2026-09-16 04:22 구간).

기본값을 ``"Info"``로 되돌리는 것은 이미 실패한 길이다. 대신 **코드가 아는
사실로** 심각도를 낸다. 규칙이 전부 구조화된 필드에서 오는지, 특히 분석
구간 밖의 관측이 끼어들지 않는지를 여기서 못 박는다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cluster_doctor.domain.diagnosis.observations import MasterEvent, NodeMetricRow, Observations, TimelineRow, observed_severity
from cluster_doctor.domain.diagnosis.health_point import HealthPoint

KST = timezone(timedelta(hours=9))
_WINDOW = (datetime(2026, 9, 16, 4, 17, tzinfo=KST), datetime(2026, 9, 16, 4, 23, tzinfo=KST))


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 16, hour, minute, tzinfo=KST)


def _event(level: str) -> MasterEvent:
    return MasterEvent(timestamp=None, level=level, line="x", rendered="x")


def _health(status: str, at: datetime) -> Observations:
    return Observations(
        requested=(_WINDOW,),
        health=(HealthPoint(at=at, until=at, status=status),),
    )


def test_아무_신호도_없으면_등급을_매기지_않는다():
    # "Info"로 채우면 "이상 없음"과 "분류하지 않음"이 구별되지 않는다.
    level, reasons = observed_severity(Observations())

    assert level == ""
    assert reasons == ()


def test_rejected가_있으면_Critical이다():
    # 요청이 실제로 거절됐다는 뜻이고, 그것은 사용자가 받은 오류다.
    obs = Observations(nodes=(NodeMetricRow(node="es-01", search_rejected_max=4),))

    level, reasons = observed_severity(obs)

    assert level == "Critical"
    assert reasons == ("search/write rejected 4건",)


def test_마스터_로그_ERROR는_Warning이다():
    level, reasons = observed_severity(Observations(master_events=(_event("ERROR"),)))

    assert level == "Warning"
    assert "ERROR 1건" in reasons[0]


def test_마스터_로그_WARN만_있으면_Info다():
    level, _ = observed_severity(Observations(master_events=(_event("WARN"),)))

    assert level == "Info"


def test_분석하지_못한_분이_있으면_Warning이다():
    # 그 시각의 근거가 리포트에 없다는 뜻이다.
    obs = Observations(
        timeline=(TimelineRow(minute=_at(4, 22), counts={}, failed=True),)
    )

    level, reasons = observed_severity(obs)

    assert level == "Warning"
    assert "분석하지 못한 분 1개" in reasons


def test_더_무거운_신호가_등급을_가져간다():
    obs = Observations(
        nodes=(NodeMetricRow(node="es-01", write_rejected_max=1),),
        master_events=(_event("ERROR"), _event("WARN")),
    )

    level, reasons = observed_severity(obs)

    assert level == "Critical"
    # 등급은 하나지만 근거는 전부 남는다. 무엇을 보고 매겼는지 없이는
    # 등급 자체가 "옮겨 적은 판단"과 다를 바 없다.
    assert len(reasons) == 3


def test_분석_구간_밖의_클러스터_상태는_세지_않는다():
    """``cluster_health``는 ES 실시간 API라 과거를 모른다.

    과거 사고를 분석하면 그 값은 진단을 돌린 시각의 상태다. 리포트도 그렇게
    경고한다 — 그 값으로 심각도를 매기면 사고와 무관한 시각의 green이 "정상"
    판정을 만들고, 반대로 지금의 yellow가 사고 당시의 문제로 읽힌다.
    """
    level, reasons = observed_severity(_health("yellow", _at(8, 0)))

    assert level == ""
    assert reasons == ()


def test_분석_구간_안의_yellow는_Warning이다():
    level, reasons = observed_severity(_health("yellow", _at(4, 20)))

    assert level == "Warning"
    assert "yellow" in reasons[0]


def test_분석_구간_안의_red는_Critical이다():
    level, _ = observed_severity(_health("red", _at(4, 20)))

    assert level == "Critical"


def test_요청한_구간이_없으면_상태를_세지_않는다():
    # 구간을 모르면 "안"인지 "밖"인지 판정할 수 없다. 모르면 세지 않는다.
    obs = Observations(health=(HealthPoint(at=_at(4, 20), until=_at(4, 20), status="red"),))

    assert observed_severity(obs)[0] == ""


def test_green은_구간_안에_있어도_신호가_아니다():
    assert observed_severity(_health("green", _at(4, 20)))[0] == ""


def test_타임라인만_있어도_rejected를_센다():
    # 노드 요약이 비어 있는 실행도 있다(조회는 됐는데 메트릭이 없는 분).
    obs = Observations(
        timeline=(
            TimelineRow(minute=_at(4, 22), counts={}, search_rejected_max=2),
        )
    )

    assert observed_severity(obs)[0] == "Critical"
