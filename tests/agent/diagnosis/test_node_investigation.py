"""Master → Node Investigation.

요구사항 4·5가 여기 있다 — 문제 노드가 없으면 SSH에 붙지 않고, 있으면
ES 조회 → SSH 수집 → Node Triage 순서로 돈다.

Resolver와 Fetcher가 LLM이 아니라는 것도 함께 확인한다. 노드 주소는 추론할
것이 아니라 조회할 것이고, 모델이 IP를 지어내면 SSH가 엉뚱한 호스트에 붙는다.
"""

import json
from datetime import datetime

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.domain.diagnosis.evidence import Evidence, EvidenceSource, ProblemNodeCandidate
from cluster_doctor.domain.diagnosis.resolved_node import ResolvedNode
from cluster_doctor.agent.diagnosis.node_investigation import (
    find_problem_nodes,
    investigate_nodes,
)
from tests.contracts.test_time_range_spans import KST, span

WINDOW = span(14, 0, 14, 10)


def master_evidence(evidence_id: str = "E-1", node: str = "es-data-03") -> Evidence:
    return Evidence(
        evidence_id=evidence_id,
        event_time=datetime(2026, 9, 18, 14, 2, tzinfo=KST),
        source=EvidenceSource.MASTER_LOG,
        node_name=node,
        message=f"node-left[{node}] reason: followers check retry count exceeded",
        severity="WARN",
    )


class RecordingResolver:
    def __init__(self, resolved: ResolvedNode | None) -> None:
        self._resolved = resolved
        self.calls: list[str] = []

    def resolve(self, node_id: str):
        self.calls.append(node_id)
        return self._resolved


class RecordingFetcher:
    def __init__(self, text: str = "", error: Exception | None = None) -> None:
        self._text = text
        self._error = error
        self.calls: list[tuple] = []

    def fetch(self, ip, log_path, cluster_name, start_dt, end_dt, keyword="", max_lines=300):
        self.calls.append((ip, log_path, cluster_name))
        if self._error is not None:
            raise self._error
        return self._text


def llm_returning(payload: dict):
    def call(messages, max_tokens, response_format=None):
        return json.dumps(payload)

    return call


def llm_raising(exc: Exception):
    def call(messages, max_tokens, response_format=None):
        raise exc

    return call


REACHABLE = ResolvedNode(
    node_id="node-abc",
    node_name="es-data-03",
    host="10.0.0.5",
    log_path="/var/log/elasticsearch",
    cluster_name="es-prod",
)


class TestFindProblemNodes:
    def test_근거가_없으면_LLM을_부르지_않는다(self):
        """빈 목록을 보여 주고 "고르라"는 호출은 비용만 들고 답이 정해져 있다."""

        def must_not_be_called(*args, **kwargs):
            raise AssertionError("근거가 없는데 LLM을 불렀다")

        assert find_problem_nodes([], must_not_be_called) == []

    def test_근거를_인용한_후보만_남긴다(self):
        """근거 없는 지목에 SSH 비용을 쓰지 않는다."""
        call = llm_returning(
            {
                "candidates": [
                    {"node_id": "es-data-03", "reason": "이탈", "evidence_refs": ["E-1"]},
                    {"node_id": "es-data-09", "reason": "감", "evidence_refs": []},
                ]
            }
        )

        candidates = find_problem_nodes([master_evidence("E-1")], call)

        assert [item.node_id for item in candidates] == ["es-data-03"]

    def test_없는_근거를_인용한_후보는_버린다(self):
        call = llm_returning(
            {"candidates": [{"node_id": "es-data-03", "evidence_refs": ["E-999"]}]}
        )

        assert find_problem_nodes([master_evidence("E-1")], call) == []

    def test_상한을_넘겨_고르지_않는다(self):
        call = llm_returning(
            {
                "candidates": [
                    {"node_id": f"es-data-{i}", "evidence_refs": ["E-1"]}
                    for i in range(5)
                ]
            }
        )

        assert len(find_problem_nodes([master_evidence("E-1")], call, limit=2)) == 2

    def test_LLM이_실패하면_후보가_없다(self):
        """노드 조사는 보조 조사다. 후보 선별이 실패했다고 분석 전체를 죽이지
        않는다."""
        assert find_problem_nodes([master_evidence()], llm_raising(LlmApiError("429"))) == []

    def test_응답이_JSON이_아니면_후보가_없다(self):
        def call(messages, max_tokens, response_format=None):
            return "후보를 고르겠습니다"

        assert find_problem_nodes([master_evidence()], call) == []


class TestInvestigationIsConditional:
    def test_후보가_없으면_ES도_SSH도_부르지_않는다(self):
        """요구사항 4번."""
        resolver = RecordingResolver(REACHABLE)
        fetcher = RecordingFetcher("무언가")

        result = investigate_nodes(
            [],
            WINDOW,
            resolver=resolver,
            fetcher=fetcher,
            call_llm=llm_returning({}),
            new_evidence_id=lambda: "E-X",
            put_raw=lambda text: "R-X",
        )

        assert resolver.calls == []
        assert fetcher.calls == []
        assert result.evidence == []
        assert result.investigated == []


class TestInvestigationOrder:
    def test_ES_조회_후에_SSH로_붙는다(self):
        """요구사항 5번. 접속 주소는 조회에서 오고, 그 조회가 SSH보다 먼저다."""
        resolver = RecordingResolver(REACHABLE)
        fetcher = RecordingFetcher(
            "[2026-09-18T14:02:11,000][WARN ][o.e.m.j.JvmGcMonitorService] [gc][young] overhead"
        )
        triage_calls: list[str] = []

        def call(messages, max_tokens, response_format=None):
            triage_calls.append(messages[0]["content"])
            return json.dumps(
                {"selected": [{"record_id": 1, "event_type": "gc_pause", "reason": "r"}]}
                if "1분치" in messages[0]["content"] or "구간:" in messages[0]["content"]
                else {"keep": [{"record_id": 1, "selection_reason": "유지"}]}
            )

        result = investigate_nodes(
            [ProblemNodeCandidate(node_id="es-data-03", evidence_refs=("E-1",))],
            WINDOW,
            resolver=resolver,
            fetcher=fetcher,
            call_llm=call,
            new_evidence_id=lambda: "E-node-1",
            put_raw=lambda text: "R-node-1",
        )

        assert resolver.calls == ["es-data-03"]
        assert fetcher.calls == [("10.0.0.5", "/var/log/elasticsearch", "es-prod")]
        # Triage는 SSH 다음이다. 원문이 있어야 고를 것이 있다.
        assert triage_calls
        assert [item.node_name for item in result.evidence] == ["es-data-03"]
        assert result.evidence[0].source is EvidenceSource.NODE_LOG

    def test_노드를_찾지_못하면_SSH로_가지_않는다(self):
        resolver = RecordingResolver(None)
        fetcher = RecordingFetcher("무언가")

        result = investigate_nodes(
            [ProblemNodeCandidate(node_id="사라진노드", evidence_refs=("E-1",))],
            WINDOW,
            resolver=resolver,
            fetcher=fetcher,
            call_llm=llm_returning({}),
            new_evidence_id=lambda: "E-X",
            put_raw=lambda text: "R-X",
        )

        assert fetcher.calls == []
        assert any("찾지 못했다" in gap for gap in result.gaps)

    def test_접속_정보가_모자라면_붙지_않는다(self):
        incomplete = ResolvedNode(node_id="node-abc", node_name="es-data-03", host="")
        fetcher = RecordingFetcher("무언가")

        result = investigate_nodes(
            [ProblemNodeCandidate(node_id="es-data-03", evidence_refs=("E-1",))],
            WINDOW,
            resolver=RecordingResolver(incomplete),
            fetcher=fetcher,
            call_llm=llm_returning({}),
            new_evidence_id=lambda: "E-X",
            put_raw=lambda text: "R-X",
        )

        assert fetcher.calls == []
        assert result.gaps

    def test_SSH가_실패해도_gap으로만_남는다(self):
        """노드 하나에 붙지 못한 것이 분석 전체의 실패는 아니다."""
        result = investigate_nodes(
            [ProblemNodeCandidate(node_id="es-data-03", evidence_refs=("E-1",))],
            WINDOW,
            resolver=RecordingResolver(REACHABLE),
            fetcher=RecordingFetcher(error=OSError("연결 거부")),
            call_llm=llm_returning({}),
            new_evidence_id=lambda: "E-X",
            put_raw=lambda text: "R-X",
        )

        assert result.evidence == []
        assert any("SSH 수집 실패" in gap for gap in result.gaps)

    def test_로그가_비어_있으면_Triage를_돌리지_않는다(self):
        def must_not_be_called(*args, **kwargs):
            raise AssertionError("빈 로그로 Triage를 돌렸다")

        result = investigate_nodes(
            [ProblemNodeCandidate(node_id="es-data-03", evidence_refs=("E-1",))],
            WINDOW,
            resolver=RecordingResolver(REACHABLE),
            fetcher=RecordingFetcher("   \n  "),
            call_llm=must_not_be_called,
            new_evidence_id=lambda: "E-X",
            put_raw=lambda text: "R-X",
        )

        assert result.evidence == []
        assert result.investigated == []
