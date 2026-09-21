"""IncidentState 저장소와 산출물 저장소.

첫 구현은 in-memory이고 그것으로 충분하다. 포트를 둔 이유는 교체 가능성이
실재하기 때문이고, 그래서 이 테스트는 **포트가 약속한 성질**만 본다 — dict를
어떻게 들고 있는지는 보지 않는다.
"""

from datetime import datetime

import pytest

from cluster_doctor.application.exception import IncidentNotFoundError
from cluster_doctor.domain.model.diagnosis_report import (
    NodeMetricRow,
    Observations,
    TimelineRow,
)
from cluster_doctor.domain.model.evidence import Evidence, EvidenceSource
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.log_analysis_report import LogAnalysisReport
from cluster_doctor.infrastructure.outbound.memory.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.infrastructure.outbound.memory.in_memory_incident_state_repository import (
    InMemoryIncidentStateRepository,
)
from tests.domain.model.test_time_range_spans import KST, span


def at(minute: int) -> datetime:
    return datetime(2026, 9, 18, 14, minute, tzinfo=KST)


def evidence(evidence_id: str, minute: int) -> Evidence:
    return Evidence(
        evidence_id=evidence_id,
        event_time=at(minute),
        source=EvidenceSource.SLOWLOG,
        message=f"{evidence_id} 근거",
    )


class TestStateRepository:
    def test_만들고_꺼낸다(self):
        repository = InMemoryIncidentStateRepository()
        repository.create(IncidentState(incident_id="inc-1"))

        assert repository.get("inc-1").incident_id == "inc-1"

    def test_없는_Incident를_꺼내면_의미_있는_예외다(self):
        with pytest.raises(IncidentNotFoundError):
            InMemoryIncidentStateRepository().get("없음")

    def test_같은_id로_두_번_만들지_못한다(self):
        """같은 incident_id로 두 번 시작하는 것은 버그이고, 그것이 조용히 앞선
        상태를 지우면 진행 중이던 분석을 잃는다."""
        repository = InMemoryIncidentStateRepository()
        repository.create(IncidentState(incident_id="inc-1"))

        with pytest.raises(ValueError):
            repository.create(IncidentState(incident_id="inc-1"))

    def test_없는_Incident는_저장할_수_없다(self):
        with pytest.raises(IncidentNotFoundError):
            InMemoryIncidentStateRepository().save(IncidentState(incident_id="inc-1"))

    def test_꺼낸_객체를_고쳐도_저장소는_변하지_않는다(self):
        """같은 객체를 돌려주면 호출자가 저장소를 거치지 않고 상태를 바꿀 수
        있고, 그러면 save를 부르지 않은 변경이 조용히 반영된다 — Redis 구현으로
        바꾸는 날 그 코드가 전부 깨진다."""
        repository = InMemoryIncidentStateRepository()
        repository.create(IncidentState(incident_id="inc-1"))

        borrowed = repository.get("inc-1")
        borrowed.analyzed_windows.append(span(14, 0, 14, 10))

        assert repository.get("inc-1").analyzed_windows == []

    def test_저장하면_반영된다(self):
        repository = InMemoryIncidentStateRepository()
        repository.create(IncidentState(incident_id="inc-1"))
        state = repository.get("inc-1")
        state.analysis_call_count = 3

        repository.save(state)

        assert repository.get("inc-1").analysis_call_count == 3


class TestArtifactStore:
    def test_Evidence를_시간순으로_돌려준다(self):
        store = InMemoryArtifactStore()
        store.put_evidence("inc-1", evidence("E-2", 5))
        store.put_evidence("inc-1", evidence("E-1", 2))

        assert [item.evidence_id for item in store.list_evidence("inc-1")] == [
            "E-1",
            "E-2",
        ]

    def test_참조로_고른다(self):
        store = InMemoryArtifactStore()
        store.put_evidence("inc-1", evidence("E-1", 2))
        store.put_evidence("inc-1", evidence("E-2", 5))

        assert [i.evidence_id for i in store.get_evidence("inc-1", ("E-2",))] == ["E-2"]

    def test_없는_참조는_조용히_건너뛴다(self):
        """모델이 없는 id를 인용하는 것은 검증이 잡을 일이지, 조회가 죽을
        일이 아니다."""
        store = InMemoryArtifactStore()
        store.put_evidence("inc-1", evidence("E-1", 2))

        assert store.get_evidence("inc-1", ("E-999",)) == []

    def test_Incident마다_나뉜다(self):
        store = InMemoryArtifactStore()
        store.put_evidence("inc-1", evidence("E-1", 2))

        assert store.list_evidence("inc-2") == []

    def test_id는_Incident_안에서_이어진다(self):
        """datasource마다 1번부터 다시 시작하면 E-1이 여러 개가 된다."""
        store = InMemoryArtifactStore()

        first = store.next_evidence_id("inc-1")
        second = store.next_evidence_id("inc-1")

        assert first != second
        assert store.next_evidence_id("inc-2") != first

    def test_원문을_참조로_넣고_꺼낸다(self):
        store = InMemoryArtifactStore()

        ref = store.put_raw("inc-1", "[WARN ] follower check failed")

        assert store.get_raw(ref) == "[WARN ] follower check failed"

    def test_원문이_상한을_넘으면_자르고_그_사실을_적는다(self):
        """조용히 자르면 검증이 "인용이 원문에 없다"고 잘못 말한다."""
        store = InMemoryArtifactStore(max_raw_chars=10)

        ref = store.put_raw("inc-1", "가" * 100)

        assert "잘림" in store.get_raw(ref)

    def test_리포트를_참조로_넣고_꺼낸다(self):
        store = InMemoryArtifactStore()
        report = LogAnalysisReport(
            incident_id="inc-1", analyzed_from=at(0), analyzed_to=at(10), summary="s"
        )

        ref = store.put_report("inc-1", report)

        assert store.get_report(ref).summary == "s"

    def test_없는_리포트는_None이다(self):
        assert InMemoryArtifactStore().get_report("RPT-없음") is None


class TestObservationMerging:
    def test_분을_키로_덮어쓴다(self):
        """실패한 분을 다시 분석해 성공하면 failed 표시가 사라져야 한다."""
        store = InMemoryArtifactStore()
        store.merge_observations(
            "inc-1", Observations(timeline=(TimelineRow(minute=at(2), failed=True),))
        )

        store.merge_observations(
            "inc-1", Observations(timeline=(TimelineRow(minute=at(2), failed=False),))
        )

        timeline = store.get_observations("inc-1").timeline
        assert len(timeline) == 1
        assert timeline[0].failed is False

    def test_노드_지표는_max의_max다(self):
        store = InMemoryArtifactStore()
        store.merge_observations(
            "inc-1",
            Observations(nodes=(NodeMetricRow(node="es-data-01", jvm_heap_max=70, samples=1),)),
        )

        store.merge_observations(
            "inc-1",
            Observations(nodes=(NodeMetricRow(node="es-data-01", jvm_heap_max=92, samples=1),)),
        )

        row = store.get_observations("inc-1").nodes[0]
        assert row.jvm_heap_max == 92
        assert row.samples == 2

    def test_아무것도_안_넣었으면_빈_관측값이다(self):
        assert InMemoryArtifactStore().get_observations("inc-1").is_empty()

    def test_요청_구간은_이어_붙인다(self):
        """같은 구간을 다시 부른 것은 그 자체로 사실이다."""
        store = InMemoryArtifactStore()
        store.merge_observations("inc-1", Observations(requested=((at(0), at(10)),)))
        store.merge_observations("inc-1", Observations(requested=((at(10), at(20)),)))

        assert len(store.get_observations("inc-1").requested) == 2
