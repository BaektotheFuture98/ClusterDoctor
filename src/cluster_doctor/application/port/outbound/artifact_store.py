"""Evidence·원문·리포트를 참조로 주고받게 하는 포트.

이 포트가 없으면 Agent 사이를 오가는 것이 payload가 된다. 그러면 Supervisor
Context에 SSH 원문과 minute 중간 결과가 쌓이고, Context window가 아직 남아
있더라도 비용과 잡음이 함께 오른다.

그래서 규칙은 하나다 — **Agent 사이에는 참조만 흐르고, 실체는 여기 있다.**

관측값(``Observations``)도 여기 둔다. 그것은 Evidence가 아니라 코드가 센
숫자이고, Incident에 걸쳐 누적돼야 하며(한 Incident에 분석이 여러 번 돈다),
Supervisor는 읽을 일이 없다.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from cluster_doctor.domain.model.diagnosis_report import Observations
from cluster_doctor.domain.model.evidence import Evidence
from cluster_doctor.domain.model.log_analysis_report import LogAnalysisReport


@runtime_checkable
class ArtifactStore(Protocol):
    def put_raw(self, incident_id: str, text: str) -> str:
        """원문 한 덩어리를 넣고 참조를 돌려준다.

        Evidence의 ``raw_ref``가 가리키는 곳이다. 검증이 "이 주장이 실제 줄과
        맞는가"를 볼 때만 꺼내며, 평소에는 아무도 읽지 않는다.
        """
        ...

    def get_raw(self, raw_ref: str) -> str | None: ...

    def next_evidence_id(self, incident_id: str) -> str:
        """다음 Evidence에 붙일 id를 발급한다.

        번호를 저장소가 발급하는 이유는 ``SlowCandidate``의 ``C1``과 같다 —
        한 Incident 안에서 번호가 이어져야 하는데, 워크플로가 그 상태를 들고
        있으면 datasource마다 1번부터 다시 시작해 ``E1``이 여러 개가 된다.
        """
        ...

    def put_evidence(self, incident_id: str, evidence: Evidence) -> str:
        """Evidence를 넣고 ``evidence_id``를 돌려준다."""
        ...

    def get_evidence(self, incident_id: str, refs: tuple[str, ...]) -> list[Evidence]:
        """참조로 Evidence를 꺼낸다. 없는 참조는 조용히 건너뛴다.

        예외를 올리지 않는 이유: 모델이 없는 id를 인용하는 것은 검증이 잡을
        일이지, 조회가 죽을 일이 아니다.
        """
        ...

    def list_evidence(self, incident_id: str) -> list[Evidence]:
        """이 Incident에서 지금까지 모은 Evidence 전부, 시간순."""
        ...

    def put_report(self, incident_id: str, report: LogAnalysisReport) -> str: ...

    def get_report(self, report_ref: str) -> LogAnalysisReport | None: ...

    def merge_observations(self, incident_id: str, observations: Observations) -> None:
        """코드가 센 관측값을 누적한다. 지표마다 max의 max, 타임라인은 분을 키로.

        덮어쓰기가 아니라 병합인 이유: 분석은 한 Incident에서 여러 번
        불리고 구간이 겹칠 수 있다.
        """
        ...

    def get_observations(self, incident_id: str) -> Observations: ...
