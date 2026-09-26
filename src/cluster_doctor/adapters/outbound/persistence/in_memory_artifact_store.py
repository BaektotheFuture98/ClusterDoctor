"""Evidence·원문·리포트·관측값의 프로세스 내 보관.

참조 문자열에 incident_id를 **넣는다**(``E-<incident>-7``). 두 가지가 따라온다.
참조 하나만 보면 어느 Incident의 것인지 알 수 있어 로그를 읽기 쉽고, 다른
Incident의 참조를 넘겨받아도 조회 단계에서 걸러진다 — 10번 테스트가 요구하는
"이전 Incident의 Context가 이어지지 않는다"를 저장소 층에서 한 번 더 막는다.

원문에는 상한을 건다. SSH 한 번이 수천 줄을 가져올 수 있고, 그것을 그대로 들고
있으면 오래 도는 프로세스에서 메모리가 Incident 수만큼 늘어난다.
"""

from __future__ import annotations

import logging
import threading

from cluster_doctor.domain.diagnosis.observations import (
    Observations,
    merge_observations,
)
from cluster_doctor.domain.diagnosis.evidence import Evidence
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport

_logger = logging.getLogger(__name__)

# 원문 한 덩어리의 상한. 넘으면 앞부분만 남기고 잘린 사실을 본문에 적는다 —
# 조용히 자르면 검증이 "인용이 원문에 없다"고 잘못 말한다.
MAX_RAW_CHARS = 200_000


class InMemoryArtifactStore:
    def __init__(self, max_raw_chars: int = MAX_RAW_CHARS) -> None:
        self._raw: dict[str, str] = {}
        self._evidence: dict[str, list[Evidence]] = {}
        self._reports: dict[str, LogAnalysisReport] = {}
        self._observations: dict[str, Observations] = {}
        self._counters: dict[str, int] = {}
        self._max_raw_chars = max_raw_chars
        self._lock = threading.RLock()

    def _next(self, kind: str, incident_id: str) -> str:
        key = f"{kind}:{incident_id}"
        self._counters[key] = self._counters.get(key, 0) + 1
        return f"{kind}-{incident_id}-{self._counters[key]}"

    def put_raw(self, incident_id: str, text: str) -> str:
        with self._lock:
            ref = self._next("R", incident_id)
            if len(text) > self._max_raw_chars:
                dropped = len(text) - self._max_raw_chars
                text = text[: self._max_raw_chars] + f"\n... ({dropped}자 잘림)"
            self._raw[ref] = text
        return ref

    def get_raw(self, raw_ref: str) -> str | None:
        with self._lock:
            return self._raw.get(raw_ref)

    def put_evidence(self, incident_id: str, evidence: Evidence) -> str:
        with self._lock:
            self._evidence.setdefault(incident_id, []).append(evidence)
        return evidence.evidence_id

    def next_evidence_id(self, incident_id: str) -> str:
        """다음 Evidence에 붙일 id.

        id를 저장소가 발급하는 이유는 ``SlowCandidate``의 ``C1``과 같다 —
        한 Incident 안에서 번호가 이어져야 하고, 그 상태를 워크플로가 들고
        있으면 datasource마다 1번부터 다시 시작한다.
        """
        with self._lock:
            return self._next("E", incident_id)

    def get_evidence(self, incident_id: str, refs: tuple[str, ...]) -> list[Evidence]:
        wanted = set(refs)
        with self._lock:
            items = self._evidence.get(incident_id, [])
        return [item for item in items if item.evidence_id in wanted]

    def list_evidence(self, incident_id: str) -> list[Evidence]:
        with self._lock:
            items = list(self._evidence.get(incident_id, []))
        return sorted(items, key=lambda item: (item.event_time, item.evidence_id))

    def put_report(self, incident_id: str, report: LogAnalysisReport) -> str:
        with self._lock:
            ref = self._next("RPT", incident_id)
            self._reports[ref] = report
        return ref

    def get_report(self, report_ref: str) -> LogAnalysisReport | None:
        with self._lock:
            return self._reports.get(report_ref)

    def merge_observations(self, incident_id: str, observations: Observations) -> None:
        with self._lock:
            current = self._observations.get(incident_id)
            self._observations[incident_id] = (
                observations
                if current is None
                else merge_observations(current, observations)
            )

    def get_observations(self, incident_id: str) -> Observations:
        with self._lock:
            return self._observations.get(incident_id) or Observations()

    def discard(self, incident_id: str) -> None:
        """끝난 Incident의 산출물을 지운다. 리포트는 남긴다.

        리포트를 남기는 이유는 운영자가 나중에 참조로 다시 꺼낼 수 있어야
        하기 때문이다. 부피가 큰 것은 원문과 Evidence 쪽이다.
        """
        with self._lock:
            self._evidence.pop(incident_id, None)
            self._observations.pop(incident_id, None)
            prefix = f"R-{incident_id}-"
            for ref in [key for key in self._raw if key.startswith(prefix)]:
                self._raw.pop(ref, None)
            suffix = f":{incident_id}"
            for key in [k for k in self._counters if k.endswith(suffix)]:
                self._counters.pop(key, None)
