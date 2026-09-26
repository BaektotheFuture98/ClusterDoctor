"""Diagnosis SubAgent의 도구 셋.

모델이 고를 수 있는 것은 셋뿐이고 전부 굵다. 기존에 돌던 결정적 Python을
LLM 도구로 쪼개 다시 쓰지 않는다는 뜻이다.
"""

from __future__ import annotations

import logging

from langchain_core.tools import tool

from cluster_doctor.domain.diagnosis.report import VerificationStatus
from cluster_doctor.domain.diagnosis.time_range import TimeRange, split_span
from cluster_doctor.domain.diagnosis.kst import parse_kst
from cluster_doctor.agent.diagnosis.collector import CollectedEvidence, EvidenceCollector
from cluster_doctor.agent.diagnosis.state import DiagnosisSeams, _Delegation

_logger = logging.getLogger(__name__)

# ``write_report``가 실제로 돌 수 있는 횟수. 리포트 **안쪽**의 수정 횟수는
# ``MAX_REPORT_REVISIONS``가 쥐고 있고 여기서 다시 정의하지 않는다. 이 상한은
# 성격이 다르다 — 모델이 초안이 마음에 들지 않는다고 처음부터 다시 쓰는 것을
# 막는다. 한 번은 다시 쓸 수 있게 둔 이유는 근거를 보고 물음을 고쳐 잡는 것이
# 실제로 더 나은 리포트를 내기 때문이다.
_MAX_REPORT_ATTEMPTS = 2


def _build_tools(seams: DiagnosisSeams, delegation: _Delegation) -> list:
    """모델이 고를 수 있는 것 전부. 셋뿐이고 전부 굵다."""

    @tool
    def collect_evidence() -> dict:
        """승인된 구간의 모든 데이터소스를 훑어 근거를 모은다.

        클러스터 상태, slowlog, query log, 노드 지표, 마스터 로그를 보고,
        마스터 로그가 특정 노드를 지목했을 때만 그 노드에 접속해 확인한다.
        어느 소스를 어떤 순서로 볼지, 노드에 들어갈지는 이 도구 안의 규칙이
        정한다.

        **위임당 한 번만 실제로 돌아간다.** 두 번째 호출은 이미 모은 결과의
        요약을 그대로 돌려준다 — 데이터소스 조회도 LLM 선별도 다시 하지 않는다.
        같은 구간을 다시 훑어도 결과는 같고, 비용만 배로 든다.

        근거 **본문은 돌려주지 않는다.** 개수와 관측 요약뿐이다.
        """
        if delegation.collected_once:
            _logger.info("[diagnosis] 근거 수집은 이미 끝났다 — 캐시된 요약을 돌려준다")
            return _evidence_digest(delegation, cached=True)

        try:
            collector = EvidenceCollector(
                incident_id=delegation.request.incident_id,
                store=seams.store,
                fetch_logs=seams.fetch_logs,
                fetch_node_logs=seams.fetch_node_logs,
                cluster=seams.cluster,
                node_resolver=seams.node_resolver,
                node_log_fetcher=seams.node_log_fetcher,
                call_llm=seams.call_llm,
                metric_thresholds=seams.metric_thresholds,
            )
            delegation.collected = collector.collect(
                delegation.window, delegation.run_state
            )
        except Exception as exc:  # noqa: BLE001
            # 수집기는 소스별 실패를 스스로 삼키므로 여기까지 오는 것은 조립
            # 자체가 틀어진 경우다. 그래도 루프를 죽이지 않는다.
            _logger.exception("[diagnosis] 근거 수집이 실패했다: %s", exc)
            delegation.run_state.mark_gap(f"근거 수집이 실패했다: {exc}")
            delegation.collected = CollectedEvidence()

        delegation.evidence = list(delegation.collected.evidence)
        return _evidence_digest(delegation, cached=False)

    @tool
    def write_report(focus: str) -> dict:
        """모은 근거로 원인을 분석하고 리포트를 쓰고 검증한다.

        초안 작성 → 일관성 검증 → (지적이 있으면) 정해진 횟수 안에서 수정까지
        한 번에 돈다. 검증 규칙과 수정 횟수 상한은 이 도구 안에 있고 바꿀 수
        없다 — 모델이 자기 리포트를 채점하면 거의 통과하기 때문이다.

        ``focus``는 "이 근거로 무엇을 묻고 싶은가"다. 비워도 되지만, 구체적일
        수록 원인 분석이 좁게 들어간다.

        근거를 모으기 전에는 부를 수 없고, 위임당 실행 횟수에 상한이 있다.

        리포트 **전문은 돌려주지 않는다.** 검증 상태와 참조뿐이다.
        """
        if not delegation.collected_once:
            return {
                "error": "근거를 먼저 모아야 한다. collect_evidence를 부르고 다시 와라.",
            }
        if not delegation.evidence:
            return {
                "error": (
                    "근거가 하나도 없어 리포트를 쓰지 않는다. 근거 없이 쓰는 원인은 "
                    "검증에서 어차피 걸린다. report_insufficient로 올려라."
                ),
            }
        if delegation.report_attempts >= _MAX_REPORT_ATTEMPTS:
            return {
                "error": (
                    f"리포트 작성 상한 {_MAX_REPORT_ATTEMPTS}회를 이미 썼다. "
                    "마지막 리포트가 결과로 나간다."
                ),
                "verification_status": _verification_of(delegation),
                "report_ref": delegation.report_ref,
            }

        delegation.report_attempts += 1
        # focus를 analysis_goal 자리에 넣는다. 원인 분석 프롬프트가 읽는 자리가
        # 거기이고, 구간은 request에 이미 고정돼 있어 focus로 바뀌지 않는다.
        request = delegation.request.model_copy(
            update={"analysis_goal": (focus or delegation.request.analysis_goal)}
        )
        try:
            draft = seams.report_writer.draft_report(
                request, delegation.evidence, delegation.run_state
            )
            report = draft.to_domain(
                incident_id=request.incident_id,
                window=delegation.window,
                evidence_refs=tuple(item.evidence_id for item in delegation.evidence),
            )
            report = seams.report_writer.verify_and_revise(
                report, delegation.evidence, delegation.run_state.candidate_ids()
            )
            report_ref = seams.store.put_report(request.incident_id, report)
        except Exception as exc:  # noqa: BLE001
            _logger.exception("[diagnosis] 리포트 작성이 실패했다: %s", exc)
            delegation.run_state.mark_gap(f"리포트 작성이 실패했다: {exc}")
            return {"error": f"리포트 작성이 실패했다: {exc}"}

        delegation.draft = draft
        delegation.report = report
        delegation.report_ref = report_ref
        # 초안이 "구간 밖을 봐야 한다"고 말했으면 그것을 여기서 올린다.
        # 그 판단을 내린 모델은 도구가 없는 구조화 호출 안에 있어 스스로
        # report_insufficient를 부를 수 없다 — 전달하지 않으면 요청이 조용히
        # 사라지고, 프롬프트가 쓰라고 시킨 필드가 아무 데도 닿지 않는다.
        wanted = [
            f"{item.start.isoformat()}/{item.end.isoformat()}"
            for item in draft.parsed_windows()
        ]
        payload = {
            "report_ref": report_ref,
            "verification_status": str(report.verification_status),
            "verification_issue_count": len(report.verification_issues),
            "revision_count": report.revision_count,
            "finding_count": len(report.findings),
            "root_cause_count": len(report.root_causes),
            "unresolved_question_count": len(report.unresolved_questions),
            "attempts_left": _MAX_REPORT_ATTEMPTS - delegation.report_attempts,
        }
        if draft.needs_more_context and wanted:
            payload["needs_more_context"] = True
            payload["draft_suggested_windows"] = wanted
            payload["next_step"] = (
                "초안이 이 구간 밖을 봐야 한다고 판단했다. "
                "동의하면 report_insufficient에 위 구간을 그대로 넘겨라."
            )
        return payload

    @tool
    def report_insufficient(reason: str, suggested_windows: list[str]) -> dict:
        """이 구간의 근거만으로는 답할 수 없다고 상위에 올린다.

        ``suggested_windows``는 ``"<시작 ISO>/<끝 ISO>"`` 형식 문자열의 목록이다
        (예: ``"2026-09-21T13:50:00+09:00/2026-09-21T14:00:00+09:00"``).

        **제안일 뿐이다.** 실제로 그 구간을 보게 될지는 상위 Agent가 예산과 이미
        분석한 구간을 보고 정한다. 이 도구를 부른다고 네 분석 범위가 넓어지지
        않는다.
        """
        delegation.insufficient_reason = (reason or "").strip()
        accepted, rejected = _parse_windows(suggested_windows)
        delegation.suggested = accepted
        if delegation.insufficient_reason:
            delegation.run_state.mark_gap(
                f"이 구간만으로는 부족하다: {delegation.insufficient_reason}"
            )
        return {
            "accepted_windows": [
                f"{w.start.isoformat()}/{w.end.isoformat()}" for w in accepted
            ],
            "rejected": rejected,
            "note": "상위 Agent가 예산과 중복을 보고 실제로 볼 구간을 정한다.",
        }

    return [collect_evidence, write_report, report_insufficient]


def _evidence_digest(delegation: _Delegation, *, cached: bool) -> dict:
    """모델이 볼 수집 결과. **본문 없이 개수와 관측 요약만.**

    SubAgent의 Context도 Context다. 여기서 근거 본문을 돌려주면 루프가 한 바퀴
    돌 때마다 그것이 메시지로 쌓이고, 원문을 Context에서 떼어 놓으려고 만든
    ArtifactStore 간접참조가 이 경계에서 무너진다.
    """
    collected = delegation.collected or CollectedEvidence()
    return {
        "evidence_count": len(delegation.evidence),
        "investigated_nodes": list(collected.investigated_nodes),
        "failed_minute_count": len(collected.failed_minutes),
        "observation_summary": delegation.run_state.summary_for_prompt(),
        "suspect_candidates": delegation.run_state.candidates_for_prompt(),
        "gaps": list(delegation.run_state.gaps),
        "cached": cached,
    }


def _parse_windows(raw: list[str]) -> tuple[list[TimeRange], list[str]]:
    """``"<start>/<end>"`` 문자열을 ``TimeRange``로. 읽지 못한 것은 버린다.

    10분을 넘는 제안은 거절하지 않고 쪼갠다 — ``schema.parsed_windows``와 같은
    이유다. 상한의 근거는 한 번의 조회가 부담하는 팬아웃 비용이지 "그 시간대를
    보면 안 된다"가 아니고, 거절하면 정당한 15분 요청이 통째로 사라진다.
    """
    accepted = []
    rejected: list[str] = []
    for item in raw or []:
        try:
            start_text, end_text = str(item).split("/", 1)
            # ``parse_kst``를 쓴다. 모델이 offset 없이 적어 보내는 것이 예사이고,
            # ``fromisoformat``으로 읽으면 그 값이 naive가 된다. naive 구간은
            # aware인 ``state.analyzed_windows``와 비교되지 못해 ``_apply_response``
            # 의 차집합에서 ``TypeError``로 터지고, 그 예외는 이미 리포트를
            # 저장한 뒤 ``repository.save`` 앞에서 나므로 리포트가 저장소에
            # 있는데도 운영자에게 가지 않는다.
            start = parse_kst(start_text.strip())
            end = parse_kst(end_text.strip())
            pieces = split_span(start, end)
            if not pieces:
                raise ValueError("시작이 끝보다 뒤이거나 같다")
            accepted.extend(pieces)
        except Exception as exc:  # noqa: BLE001
            rejected.append(f"{item!r}: {exc}")
    return accepted, rejected


def _verification_of(delegation: _Delegation) -> VerificationStatus:
    """리포트가 없으면 검증은 통과가 아니라 **돌지 않은** 것이다."""
    if delegation.report is None:
        return VerificationStatus.NOT_VERIFIED
    return delegation.report.verification_status
