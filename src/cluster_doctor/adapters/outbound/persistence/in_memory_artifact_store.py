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
        """끝난 Incident의 프로세스 내 산출물을 모두 지운다.

        운영자가 읽는 최종 리포트는 이 메서드가 호출되기 전에 publisher가
        외부 저장소에 남긴다. 이 저장소의 리포트는 분석 중 참조하기 위한
        임시 객체이므로 완료 뒤까지 붙들면 장기 실행 프로세스에서 계속 누적된다.
        """
        with self._lock:
            self._evidence.pop(incident_id, None)
            self._observations.pop(incident_id, None)
            prefix = f"R-{incident_id}-"
            for ref in [key for key in self._raw if key.startswith(prefix)]:
                self._raw.pop(ref, None)
            report_count = self._counters.get(f"RPT:{incident_id}", 0)
            for sequence in range(1, report_count + 1):
                self._reports.pop(f"RPT-{incident_id}-{sequence}", None)
            suffix = f":{incident_id}"
            for key in [k for k in self._counters if k.endswith(suffix)]:
                self._counters.pop(key, None)
