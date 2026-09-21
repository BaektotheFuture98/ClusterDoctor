"""Log Analysis SubAgent.

요구사항 7·8이 여기 있다 — 검증이 불일치를 찾으면 허용된 횟수 안에서 고치고,
그 횟수를 넘기면 무한 반복하지 않는다.

SubAgent는 예외를 밖으로 내보내지 않는다는 계약도 함께 확인한다. 예외로
올리면 Supervisor 사이클이 죽고 그때까지 모은 근거가 함께 사라진다.
"""

import json
import re
from datetime import datetime

from cluster_doctor.application.exception import LlmApiError
from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisRequest,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.domain.model.kafka.slowlog_entry import SlowlogEntry
from cluster_doctor.infrastructure.outbound.agent.diagnosis.agent import (
    DiagnosisAgentAdapter,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.schema import DraftReport
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.nodes import (
    MapOutput,
    ReduceOutput,
)
from cluster_doctor.infrastructure.outbound.memory.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from tests.domain.model.test_time_range_spans import KST, span

WINDOW = span(14, 0, 14, 10)
EVENT_AT = datetime(2026, 9, 18, 14, 2, tzinfo=KST)


def slowlog() -> SlowlogEntry:
    return SlowlogEntry(
        timestamp=EVENT_AT,
        index_name="logs-2026",
        node="es-data-03",
        took="37.1s",
        total_hits="12",
        total_shards=5,
        query='{"match_all":{}}',
    )


class GreenCluster:
    def health(self):
        return {"status": "green", "unassigned_shards": 0, "active_shards": 10,
                "number_of_nodes": 3}

    def explain_allocation(self):
        raise AssertionError("green인데 allocation explain을 불렀다")

    def node_info(self, node_id):
        return {}


class NoResolver:
    def resolve(self, node_id):
        return None


class NoFetcher:
    def fetch(self, *args, **kwargs):
        raise AssertionError("SSH를 부르면 안 된다")


class ScriptedLlm:
    """단계를 ``response_format``으로 구분해 답한다.

    Draft는 대본대로 돌려주되, 실제 Evidence id를 프롬프트에서 뽑아 쓴다 —
    저장소가 발급한 id를 테스트가 미리 알 수 없고, 알아야 한다면 그것이 이미
    설계의 냄새다.
    """

    def __init__(self, *drafts: dict) -> None:
        self._drafts = list(drafts)
        self.draft_calls = 0
        self.prompts: list[str] = []

    def __call__(self, messages, max_tokens, response_format=None):
        prompt = messages[-1]["content"]
        self.prompts.append(prompt)
        if response_format is MapOutput:
            visible = [
                int(line.split()[0].lstrip("#"))
                for line in prompt.splitlines()
                if line.startswith("#")
            ]
            return json.dumps(
                {"selected": [{"record_id": rid, "event_type": "slow_query"} for rid in visible]}
            )
        if response_format is ReduceOutput:
            ids = [int(m) for m in re.findall(r"^#(\d+)", prompt, flags=re.MULTILINE)]
            return json.dumps(
                {"keep": [{"record_id": rid, "selection_reason": "유지"} for rid in ids[:1]]}
            )
        if response_format is DraftReport:
            self.draft_calls += 1
            template = (
                self._drafts.pop(0) if self._drafts else {"summary": "빈 초안"}
            )
            return json.dumps(_resolve_refs(template, prompt))
        # 문제 노드 후보 선별. 마스터 로그가 없으므로 고를 것도 없다.
        return json.dumps({"candidates": []})


def _resolve_refs(template: dict, prompt: str) -> dict:
    """대본의 ``"<REF>"``를 프롬프트에 실린 실제 Evidence id로 바꾼다."""
    found = re.findall(r"\[(E-[^\]]+)\]", prompt)
    real = found[0] if found else "E-없음"
    return json.loads(json.dumps(template).replace("<REF>", real))


def build(llm, *, max_revisions=1, logs=None, store=None):
    store = store or InMemoryArtifactStore()
    agent = DiagnosisAgentAdapter(
        provider="gemini",
        model="x",
        api_key="y",
        fetch_logs=lambda window: list(logs if logs is not None else [slowlog()]),
        fetch_node_logs=lambda *args, **kwargs: [],
        cluster=GreenCluster(),
        node_resolver=NoResolver(),
        node_log_fetcher=NoFetcher(),
        store=store,
        max_revisions=max_revisions,
        call_llm=llm,
    )
    return agent, store


def request(incident_id="inc-1", state_ref=None) -> LogAnalysisRequest:
    return LogAnalysisRequest(
        incident_id=incident_id,
        cluster="es-prod",
        analysis_window=WINDOW,
        state_ref=state_ref,
        analysis_goal="테스트",
    )


GOOD_DRAFT = {
    "summary": "느린 쿼리가 몰렸다",
    "findings": [
        {"severity": "Warning", "title": "느린 쿼리", "evidence_refs": ["<REF>"]}
    ],
}
BAD_DRAFT = {
    "summary": "느린 쿼리가 몰렸다",
    "findings": [{"severity": "Warning", "title": "느린 쿼리", "evidence_refs": []}],
}


class TestBoundedRevision:
    def test_불일치를_찾으면_허용된_횟수_안에서_고친다(self):
        """요구사항 7번."""
        llm = ScriptedLlm(BAD_DRAFT, GOOD_DRAFT)
        agent, store = build(llm, max_revisions=1)

        response = agent.analyze(request())

        assert llm.draft_calls == 2
        assert response.verification_status is VerificationStatus.PASSED
        assert store.get_report(response.report_ref).revision_count == 1

    def test_처음부터_맞으면_수정하지_않는다(self):
        llm = ScriptedLlm(GOOD_DRAFT)
        agent, store = build(llm)

        response = agent.analyze(request())

        assert llm.draft_calls == 1
        assert store.get_report(response.report_ref).revision_count == 0

    def test_상한을_넘기면_반복하지_않는다(self):
        """요구사항 8번. 모델이 같은 지적을 이해하지 못하면 검증과 수정이
        서로를 부르며 끝나지 않는다."""
        llm = ScriptedLlm(BAD_DRAFT, BAD_DRAFT, BAD_DRAFT, BAD_DRAFT)
        agent, _ = build(llm, max_revisions=1)

        response = agent.analyze(request())

        assert llm.draft_calls == 2
        assert response.status is LogAnalysisStatus.VALIDATION_FAILED
        assert response.verification_status is VerificationStatus.MISMATCH

    def test_상한이_0이면_고치지_않고_기록만_한다(self):
        llm = ScriptedLlm(BAD_DRAFT)
        agent, store = build(llm, max_revisions=0)

        response = agent.analyze(request())

        assert llm.draft_calls == 1
        assert store.get_report(response.report_ref).verification_issues

    def test_남은_불일치를_리포트에_남긴다(self):
        """지적을 지우면 운영자가 검증을 통과한 리포트로 읽는다."""
        llm = ScriptedLlm(BAD_DRAFT, BAD_DRAFT)
        agent, store = build(llm, max_revisions=1)

        response = agent.analyze(request())

        assert store.get_report(response.report_ref).verification_issues

    def test_수정_호출이_실패하면_더_시도하지_않는다(self):
        class FailingRevision(ScriptedLlm):
            def __call__(self, messages, max_tokens, response_format=None):
                if response_format is DraftReport and self.draft_calls >= 1:
                    self.draft_calls += 1
                    raise LlmApiError("429")
                return super().__call__(messages, max_tokens, response_format)

        llm = FailingRevision(BAD_DRAFT)
        agent, _ = build(llm, max_revisions=2)

        response = agent.analyze(request())

        assert llm.draft_calls == 2
        assert response.verification_status is VerificationStatus.MISMATCH


class TestEvidenceIsWhatFlows:
    def test_응답에_원문이_아니라_참조가_실린다(self):
        llm = ScriptedLlm(GOOD_DRAFT)
        agent, store = build(llm)

        response = agent.analyze(request())

        assert response.evidence_refs
        assert all(ref.startswith("E-") for ref in response.evidence_refs)
        assert store.get_evidence("inc-1", response.evidence_refs)

    def test_관측값은_저장소에_누적된다(self):
        llm = ScriptedLlm(GOOD_DRAFT)
        agent, store = build(llm)

        agent.analyze(request())

        observations = store.get_observations("inc-1")
        assert observations.timeline
        assert observations.candidates

    def test_앞선_리포트는_요약만_프롬프트에_실린다(self):
        """전문을 실으면 호출마다 리포트가 하나씩 더 붙어 Context가 분석
        횟수만큼 불어난다."""
        store = InMemoryArtifactStore()
        first_agent, _ = build(ScriptedLlm(GOOD_DRAFT), store=store)
        first = first_agent.analyze(request())

        llm = ScriptedLlm(GOOD_DRAFT)
        second_agent, _ = build(llm, store=store)
        second_agent.analyze(request(state_ref=first.report_ref))

        analysis_prompt = next(p for p in llm.prompts if "Cross-source Analysis" in p)
        assert "앞선 분석 결과" in analysis_prompt
        assert "느린 쿼리가 몰렸다" in analysis_prompt


class TestNoExceptionEscapes:
    def test_조회가_실패해도_응답을_돌려준다(self):
        def broken(window):
            raise RuntimeError("ClickHouse 접속 실패")

        agent, _ = build(ScriptedLlm(GOOD_DRAFT), logs=[])
        agent._fetch_logs = broken

        response = agent.analyze(request())

        assert response.status is LogAnalysisStatus.FAILED
        assert response.unresolved_gaps == (WINDOW,)
        assert response.gaps

    def test_원인_분석이_실패해도_응답을_돌려준다(self):
        class NoDraft(ScriptedLlm):
            def __call__(self, messages, max_tokens, response_format=None):
                if response_format is DraftReport:
                    raise LlmApiError("429")
                return super().__call__(messages, max_tokens, response_format)

        agent, _ = build(NoDraft())

        response = agent.analyze(request())

        assert response.report_ref is not None
        assert any("원인 분석" in gap for gap in response.gaps)

    def test_근거가_없으면_원인_분석을_부르지_않는다(self):
        """빈 목록을 주고 "원인을 찾으라"는 호출은 비용만 들고, 그 답은 근거
        없는 산문이 된다."""
        llm = ScriptedLlm(GOOD_DRAFT)
        agent, _ = build(llm, logs=[])

        agent.analyze(request())

        assert llm.draft_calls == 0


class TestScopeExpansionRequest:
    def test_구간_밖이_필요하면_NEED_MORE_CONTEXT다(self):
        draft = {
            "summary": "시작 시점이 구간 밖이다",
            "findings": [{"severity": "Info", "title": "t", "evidence_refs": ["<REF>"]}],
            "needs_more_context": True,
            "suggested_windows": [
                {
                    "start_iso": "2026-09-18T13:50:00+09:00",
                    "end_iso": "2026-09-18T14:00:00+09:00",
                }
            ],
        }
        agent, _ = build(ScriptedLlm(draft))

        response = agent.analyze(request())

        assert response.status is LogAnalysisStatus.NEED_MORE_CONTEXT
        assert len(response.suggested_windows) == 1

    def test_제안_구간이_없으면_NEED_MORE_CONTEXT가_아니다(self):
        """"더 봐야 한다"고만 하고 어디를 봐야 하는지 말하지 않으면 Supervisor가
        할 수 있는 것이 없다."""
        draft = {
            "summary": "s",
            "findings": [{"severity": "Info", "title": "t", "evidence_refs": ["<REF>"]}],
            "needs_more_context": True,
        }
        agent, _ = build(ScriptedLlm(draft))

        response = agent.analyze(request())

        assert response.status is LogAnalysisStatus.COMPLETED


class TestSuspectPicksReachTheReport:
    """후보 목록이 프롬프트에 실리고, 모델이 고른 id가 리포트까지 간다.

    이 경로가 끊기면 "느린 요청 후보 중 모델이 고른 것" 섹션이 영영 비어 있고,
    그 사실은 어디에도 드러나지 않는다.
    """

    def test_후보_목록이_분석_프롬프트에_실린다(self):
        llm = ScriptedLlm(GOOD_DRAFT)
        agent, _ = build(llm)

        agent.analyze(request())

        analysis_prompt = next(p for p in llm.prompts if "Cross-source Analysis" in p)
        assert "느린 요청 후보" in analysis_prompt
        assert "[C1]" in analysis_prompt

    def test_모델이_고른_후보가_리포트에_남는다(self):
        draft = {
            "summary": "s",
            "findings": [{"severity": "Info", "title": "t", "evidence_refs": ["<REF>"]}],
            "suspect_picks": [{"candidate_id": "C1", "reason": "37.1초로 가장 느리다"}],
        }
        agent, store = build(ScriptedLlm(draft))

        response = agent.analyze(request())

        picks = store.get_report(response.report_ref).suspect_picks
        assert [p.candidate_id for p in picks] == ["C1"]
        assert picks[0].reason == "37.1초로 가장 느리다"

    def test_목록_밖의_후보를_고르면_검증이_잡는다(self):
        draft = {
            "summary": "s",
            "findings": [{"severity": "Info", "title": "t", "evidence_refs": ["<REF>"]}],
            "suspect_picks": [{"candidate_id": "C99", "reason": "감"}],
        }
        agent, store = build(ScriptedLlm(draft, draft), max_revisions=1)

        response = agent.analyze(request())

        assert response.verification_status is VerificationStatus.MISMATCH
        assert any("C99" in issue for issue in store.get_report(response.report_ref).verification_issues)
