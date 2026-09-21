"""리포트 줄을 그리는 함수들.

이 모듈에는 테스트가 없었고, 경계 조건이 몰려 있는 자리다. 실제로 그 사이에
SSH 폴백으로 들어온 마스터 로그가 312줄에서 1줄로 붕괴하고 있었다 — 묶는 키가
모든 줄에 대해 같은 값이었고 묶음마다 대표 한 줄만 실었기 때문이다.

HTML 쪽 테스트가 이 함수들을 간접적으로 몇 개 건드리긴 하지만, 그것은 HTML이
깨지는지를 보는 것이지 줄이 맞는지를 보는 것이 아니다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport, MasterEvent, NodeMetricRow, Observations, TimelineRow
from cluster_doctor.domain.model.elasticsearch.health_point import HealthPoint
from cluster_doctor.infrastructure.outbound.notifier.report_text import (
    health_lines,
    master_log_lines,
    node_lines,
    render_text,
    timeline_line,
)

KST = timezone(timedelta(hours=9))


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 10, hour, minute, second, tzinfo=KST)


# ──────────────────────────── timeline_line ────────────────────────────


def test_소스별_건수가_각자_칸을_갖는다():
    # slowlog가 0건인데 264가 그 칸에 실린 것이 이 변경의 출발점이다.
    line = timeline_line(
        TimelineRow(minute=_at(15, 23), counts={"slowlog": 0, "es_query_log": 264})
    )

    # 소스 이름은 줄여 쓴다(es_query_log → query). 칸이 갈려 있는 것이 요점이다.
    assert "slowlog=0" in line
    assert "query=264" in line


def test_값이_없는_지표는_줄에_없는_채로_그린다():
    line = timeline_line(TimelineRow(minute=_at(15, 23), counts={}))

    assert "15:23" in line
    assert "None" not in line


# ──────────────────────────── master_log_lines ────────────────────────────


def _event(line: str, *, at: datetime | None = None, logger: str = "") -> MasterEvent:
    return MasterEvent(timestamp=at, logger=logger, line=line, rendered=line)


def test_빈_입력은_섹션을_만들지_않는다():
    assert master_log_lines(()) == []


def test_같은_action은_한_묶음으로_접힌다():
    # 실측에서 24줄 중 20줄이 같은 follower_check 타임아웃이었다.
    events = tuple(
        _event(
            f"failed to execute action [internal:coordination/fault_detection/"
            f"follower_check], node [{{RC17-0{i}}}{{abc}}]",
            at=_at(15, 27, i),
        )
        for i in range(5)
    )

    lines = master_log_lines(events)

    assert "사건 1종" in lines[0]
    assert any("5건" in line for line in lines)


def test_action_이름_안의_대괄호가_묶음을_깨지_않는다():
    # cluster:monitor/nodes/stats[n]처럼 action 이름 자체에 대괄호가 들어간다.
    events = (
        _event("failed to execute action [cluster:monitor/nodes/stats[n]], node [{a}{b}]"),
        _event("failed to execute action [indices:monitor/stats[n]], node [{a}{b}]"),
    )

    lines = master_log_lines(events)

    assert "사건 2종" in lines[0]


def test_묶지_못한_줄은_한_줄로_줄이지_않는다():
    # SSH 폴백은 원문 덩어리라 logger를 못 뽑는 줄이 섞인다. 그 줄들은 서로
    # 다른 사건이므로 대표 한 줄로 줄이면 나머지가 통째로 사라진다.
    events = tuple(_event(f"        at org.elasticsearch.Foo.bar({i})") for i in range(14))

    lines = master_log_lines(events)

    rendered = [line for line in lines if "org.elasticsearch.Foo.bar" in line]
    assert len(rendered) > 1
    assert any("외 4건" in line for line in lines)


def test_잘라낸_사실을_줄로_남긴다():
    events = tuple(_event(f"line {i}", logger="o.e.c.C") for i in range(9))

    lines = master_log_lines(events)

    assert any("외 8건" in line for line in lines)


def test_전체_건수가_더_많으면_머리글에_밝힌다():
    lines = master_log_lines((_event("한 줄", logger="o.e.c.C"),), total=80)

    assert "전체 80건 중" in lines[0]


def test_시각이_없는_묶음은_범위를_지어내지_않는다():
    lines = master_log_lines((_event("스택", logger="o.e.c.C"),))

    assert "~" not in lines[2]


def test_시각이_섞여_있어도_있는_것만으로_범위를_잡는다():
    events = (
        _event("a", logger="o.e.c.C", at=_at(15, 27, 1)),
        _event("b", logger="o.e.c.C"),
        _event("c", logger="o.e.c.C", at=_at(15, 27, 9)),
    )

    lines = master_log_lines(events)

    assert "15:27:01 ~ 15:27:09" in lines[2]


# ──────────────────────────── node_lines ────────────────────────────


def _node(name: str, **kwargs) -> NodeMetricRow:
    return NodeMetricRow(node=name, **kwargs)


def test_노드가_없으면_섹션을_만들지_않는다():
    assert node_lines(()) == []


def test_rejected가_하나도_없으면_목록_대신_요약을_싣는다():
    # 107대 중 유의미한 것이 하나도 없는데 20줄을 실으면 전부 같은 말을 한다.
    rows = tuple(
        _node(f"es-{i:02d}", jvm_heap_max=40 + i, cpu_max=10 + i) for i in range(30)
    )

    lines = node_lines(rows)

    assert len(lines) == 3
    assert "30대" in lines[0]
    assert "es-29" in lines[1]


def test_rejected가_하나라도_있으면_그_노드를_싣는다():
    rows = (
        _node("es-01", jvm_heap_max=50),
        _node("es-02", jvm_heap_max=50, search_rejected_max=3),
    )

    lines = node_lines(rows)

    assert "1대" in lines[0]
    assert any("es-02" in line for line in lines[1:])
    assert not any("es-01" in line for line in lines[1:])


def test_노드가_하나뿐이어도_최대값을_잡는다():
    lines = node_lines((_node("es-01", jvm_heap_max=93, cpu_max=71),))

    assert "93%" in lines[1]
    assert "71%" in lines[2]


# ──────────────────────────── health_lines ────────────────────────────


def test_상태_이력이_없으면_섹션을_만들지_않는다():
    assert health_lines((), ()) == []


def test_요청_구간이_없으면_경고_없이_값만_싣는다():
    point = HealthPoint(at=_at(11, 40), until=_at(11, 40), status="green")

    lines = health_lines((point,), ())

    assert len(lines) == 1
    assert "green" in lines[0]


def test_조회_시점이_분석_구간_밖이면_그_사실을_먼저_밝힌다():
    # ES는 과거 상태를 모른다. 9/10 15:27 사고 리포트에 11:40 green이 실렸다.
    point = HealthPoint(at=_at(11, 40), until=_at(11, 40), status="green")

    lines = health_lines((point,), ((_at(15, 23), _at(15, 30)),))

    assert "과거 클러스터 상태를 보관하지 않는다" in lines[0]
    assert any("green" in line for line in lines)


def test_조회_시점이_구간_안이면_경고하지_않는다():
    point = HealthPoint(at=_at(15, 25), until=_at(15, 25), status="red")

    lines = health_lines((point,), ((_at(15, 23), _at(15, 30)),))

    assert not any("보관하지 않는다" in line for line in lines)


# ──────────────────────────── render_text ────────────────────────────


def test_빈_섹션이_있어도_번호가_뛰지_않는다():
    # 번호를 제목에 박아 두면 빈 섹션 하나에 6 다음이 8이 된다.
    report = DiagnosisReport(
        observations=Observations(
            timeline=(TimelineRow(minute=_at(15, 23), counts={"slowlog": 1}),),
        ),
        narrative_text="평문 리포트",
    )

    numbers = [
        int(line.split(".")[0])
        for line in render_text(report).splitlines()
        if line[:1].isdigit() and ". " in line[:4]
    ]

    assert numbers == list(range(1, len(numbers) + 1))


def test_관측값이_전혀_없어도_렌더링이_터지지_않는다():
    text = render_text(DiagnosisReport(observations=Observations()))

    assert isinstance(text, str)


# ──────────────────────────── severity_line ────────────────────────────


def test_코드_판정임을_줄에_밝힌다():
    # 모델의 severity와 나란히 놓이면 운영자가 둘을 같은 것으로 읽는다.
    from cluster_doctor.domain.model.diagnosis_report import MasterEvent
    from cluster_doctor.infrastructure.outbound.notifier.report_text import severity_line

    obs = Observations(
        master_events=(MasterEvent(timestamp=None, level="ERROR", line="x", rendered="x"),)
    )

    line = severity_line(obs)

    assert "코드 판정" in line
    assert "Warning" in line


def test_판정_근거를_반드시_붙인다():
    from cluster_doctor.domain.model.diagnosis_report import MasterEvent
    from cluster_doctor.infrastructure.outbound.notifier.report_text import severity_line

    obs = Observations(
        master_events=(MasterEvent(timestamp=None, level="WARN", line="x", rendered="x"),)
    )

    assert "마스터 로그 WARN 1건" in severity_line(obs)


def test_신호가_없으면_없다고_말한다():
    # 빈 줄로 두면 "측정하지 않았다"와 "이상이 없다"가 구별되지 않는다.
    from cluster_doctor.infrastructure.outbound.notifier.report_text import severity_line

    assert "없음" in severity_line(Observations())


def test_개요에_심각도_줄이_들어간다():
    from cluster_doctor.infrastructure.outbound.notifier.report_text import (
        SEVERITY_PREFIX,
        overview_lines,
    )

    lines = overview_lines(Observations(time_basis="slowlog_timestamp"))

    assert any(line.startswith(SEVERITY_PREFIX) for line in lines)


def test_모델이_분류하지_않은_문제는_그_사실을_적는다():
    from cluster_doctor.domain.model.diagnosis_report import Finding, Narrative

    report = DiagnosisReport(
        observations=Observations(
            timeline=(TimelineRow(minute=_at(4, 22), counts={"slowlog": 1}),),
        ),
        narrative=Narrative(
            headline="노드 이탈",
            findings=(Finding(severity="", title="RC6-09 응답 불능"),),
        ),
    )

    text = render_text(report)

    assert "(모델이 분류하지 않음): RC6-09 응답 불능" in text
