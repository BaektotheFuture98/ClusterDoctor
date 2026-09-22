"""한 window에서 코드가 관측한 사실.

Evidence와 다르다 — Evidence는 모델이 골라낸 줄이고, 여기 모이는 것은 코드가
센 숫자다. 둘을 나누는 이유는 신뢰의 출처가 다르기 때문이고, 그래서 모델이
실패해도 이 값들은 리포트에 남는다.
"""

from datetime import datetime, timedelta

from cluster_doctor.agent.integrations.clickhouse.models import NodeLogEntry, SlowlogEntry
from cluster_doctor.agent.common.kst import KST
from cluster_doctor.infrastructure.outbound.agent.diagnosis.run_state import (
    AnalysisRunState,
)
from tests.contracts.test_time_range_spans import span

TRIGGER = datetime(2026, 9, 17, 3, 0, tzinfo=KST)
WINDOW = span(14, 0, 14, 10)


def state() -> AnalysisRunState:
    return AnalysisRunState(WINDOW, time_basis="slowlog_timestamp")


def node_log(line: str, at: datetime = TRIGGER) -> NodeLogEntry:
    return NodeLogEntry(
        timestamp=at,
        node="es-master-1",
        node_role="master",
        level="WARN",
        detected_level="WARN",
        logger="o.e.c.c.Coordinator",
        filename="es.log",
        host="10.0.0.1",
        line=line,
    )


class TestMasterLogs:
    def test_같은_내용은_한_번만_담는다(self):
        run_state = state()
        entry = node_log("follower check failed")

        run_state.record_master_logs([entry, entry])

        assert len(run_state.master_logs) == 1

    def test_SSH_줄에서_레벨과_로거를_뽑는다(self):
        """세 칸을 비워 두면 리포트가 사건별로 묶을 때 쓰는 키가 모든 줄에
        대해 같은 값이 되어 수백 줄이 헤더 한 줄로 붕괴한다."""
        run_state = state()

        run_state.record_master_text(
            "[2026-09-17T03:00:01,123][WARN ][o.e.c.c.Coordinator      ] node left"
        )

        event = next(iter(run_state.master_logs.values()))
        assert event.level == "WARN"
        assert event.logger == "o.e.c.c.Coordinator"
        assert event.timestamp == datetime(2026, 9, 17, 3, 0, 1, tzinfo=KST)

    def test_시각을_못_뽑은_줄도_버리지_않는다(self):
        """값이 없다는 것과 줄이 없다는 것은 다르다."""
        run_state = state()

        run_state.record_master_text("\tat org.elasticsearch.Foo.bar(Foo.java:42)")

        assert len(run_state.master_logs) == 1


class TestHealth:
    def test_같은_상태가_이어지면_접는다(self):
        """호출마다 한 줄을 쌓으면 같은 green이 열 줄 늘어서고, 그 목록은
        "상태 변화를 시간순으로"라는 리포트의 요구를 오히려 가린다."""
        run_state = state()
        payload = {
            "status": "green",
            "unassigned_shards": 0,
            "active_shards": 10,
            "number_of_nodes": 3,
        }

        run_state.record_health(payload, now=TRIGGER)
        run_state.record_health(payload, now=TRIGGER + timedelta(seconds=30))

        assert len(run_state.health) == 1
        assert run_state.health[0].until == TRIGGER + timedelta(seconds=30)

    def test_상태가_바뀌면_줄을_늘린다(self):
        run_state = state()

        run_state.record_health({"status": "green", "unassigned_shards": 0}, now=TRIGGER)
        run_state.record_health({"status": "yellow", "unassigned_shards": 2}, now=TRIGGER)

        assert [point.status for point in run_state.health] == ["green", "yellow"]


class TestGaps:
    def test_gap을_남기고_그대로_돌려준다(self):
        run_state = state()

        assert run_state.mark_gap("SSH 실패") == "SSH 실패"
        assert run_state.gaps == ["SSH 실패"]

    def test_gap은_degraded를_세우지_않는다(self):
        """보조 조사가 실패해도 주 분석 결과는 온전하다. 리포트를 버릴 이유가
        없다."""
        run_state = state()

        run_state.mark_gap("노드 로그 수집 실패")

        assert run_state.degraded is False


class TestObservations:
    def test_관측값을_모은다(self):
        run_state = state()
        entries = [
            SlowlogEntry(timestamp=TRIGGER, node="es-data-01", took="37.1s"),
            SlowlogEntry(timestamp=TRIGGER, node="es-data-01", took="1.2s"),
        ]

        run_state.record_log_observations(entries)

        observations = run_state.to_observations()
        assert observations.timeline[0].counts == {"slowlog": 2}
        assert observations.timeline[0].took_max == "37.1s"
        assert len(observations.candidates) == 2

    def test_후보_id는_C1부터_순서대로_붙는다(self):
        run_state = state()

        run_state.record_candidates(
            [
                SlowlogEntry(timestamp=TRIGGER, took="5s"),
                SlowlogEntry(timestamp=TRIGGER, took="37.1s"),
            ]
        )

        assert [c.candidate_id for c in run_state.to_observations().candidates] == [
            "C1",
            "C2",
        ]

    def test_같은_후보는_번호를_다시_받지_않는다(self):
        """모델이 이미 본 id가 가리키는 것이 중간에 바뀌면 안 된다."""
        run_state = state()
        entry = SlowlogEntry(timestamp=TRIGGER, took="37.1s")

        run_state.record_candidates([entry])
        run_state.record_candidates([entry])

        assert len(run_state.candidates) == 1

    def test_실패한_분도_타임라인에_남는다(self):
        """행 자체가 없으면 "실패해서 못 봤다"가 "아무 일도 없었다"로 읽힌다."""
        run_state = state()

        run_state.record_timeline([SlowlogEntry(timestamp=TRIGGER)], failed=True)

        assert run_state.to_observations().timeline[0].failed is True

    def test_이미_성공한_분은_실패로_덮지_않는다(self):
        run_state = state()
        run_state.record_timeline([SlowlogEntry(timestamp=TRIGGER)])

        run_state.record_timeline([SlowlogEntry(timestamp=TRIGGER)], failed=True)

        assert run_state.to_observations().timeline[0].failed is False

    def test_마스터_이벤트는_시각순이고_시각_없는_줄이_뒤로_간다(self):
        run_state = state()
        run_state.record_master_logs([node_log("later", TRIGGER + timedelta(minutes=1))])
        run_state.record_master_logs([node_log("earlier", TRIGGER)])
        run_state.record_master_text("timestamp 없는 줄")

        observations = run_state.to_observations()

        assert [event.line for event in observations.master_events] == [
            "earlier",
            "later",
            "timestamp 없는 줄",
        ]
        assert observations.master_log_total == 3

    def test_시각_기준을_그대로_싣는다(self):
        assert state().to_observations().time_basis == "slowlog_timestamp"

    def test_분석한_구간을_기록한다(self):
        observations = state().to_observations()

        assert observations.requested == ((WINDOW.start, WINDOW.end),)


class TestPromptSummary:
    def test_분_단위_관측값을_요약한다(self):
        run_state = state()
        run_state.record_log_observations(
            [SlowlogEntry(timestamp=TRIGGER, took="37.1s")]
        )

        summary = run_state.summary_for_prompt()

        assert "03:00" in summary
        assert "slowlog=1" in summary

    def test_관측값이_없으면_없다고_말한다(self):
        assert state().summary_for_prompt() == "(관측값 없음)"
