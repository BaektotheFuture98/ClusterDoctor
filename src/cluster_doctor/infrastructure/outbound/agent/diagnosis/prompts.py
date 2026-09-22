"""Cross-source 분석과 Report Revision 프롬프트.

여기가 이 파이프라인에서 **처음으로 원인을 묻는 자리**다. Triage 단계는 선별만
했고, 그래서 지금 모델 앞에 있는 것은 원문이 아니라 줄어든 근거 목록이다.

근거는 ``[id]``로 인용하게 한다. 내용을 옮겨 적게 하면 틀린다 — 그것이 이
저장소가 두 번 당한 실패이고, Evidence에 id를 붙인 이유 전부다.

문자열 보간을 쓰지 않는다(f-string 아님). 본문의 중괄호가 서식으로 해석되면
프롬프트가 조용히 망가진다. 값이 들어갈 자리만 ``format``으로 채운다.
"""

from __future__ import annotations

from cluster_doctor.contracts.evidence import Evidence
from cluster_doctor.contracts.report import LogAnalysisReport
from cluster_doctor.agent.common.log_format import (
    format_evidence_line,
)

_ANALYSIS_HEADER = """너는 Elasticsearch 장애 분석 시스템 ClusterDoctor의
Log Analysis SubAgent다. 지금 단계는 Cross-source Analysis다.

아래 근거는 여러 데이터 소스(slowlog, 쿼리 로그, 노드 메트릭, 마스터 로그,
노드 로그)에서 선별되어 하나로 모인 것이다. 소스가 달라도 같은 형식이며,
각 줄 맨 앞의 [id]가 그 근거의 식별자다.

다음 순서로 판단한다.

1. Observation   - 근거가 실제로 말하는 것만 정리한다. 사실과 추론을 섞지 않는다.
2. Timeline      - 시각순으로 무엇이 먼저이고 무엇이 나중인가.
3. Hypothesis    - 이 전개를 설명하는 원인 후보.
4. Supporting    - 각 후보를 뒷받침하는 근거 [id].
5. Counter       - 각 후보와 맞지 않는 근거 [id]. 없으면 없다고 쓴다.
6. Conclusion    - 가장 잘 설명하는 것 하나. 근거가 얇으면 얇다고 쓴다.
"""

_ANALYSIS_RULES = """
규칙:

- 모든 주장에 evidence_refs를 단다. 근거를 댈 수 없는 주장은 쓰지 않는다.
- 반드시 주어진 [id] 중에서만 인용한다. 없는 id를 만들어 내지 않는다.
- timeline의 at은 인용한 근거의 시각을 **그대로** 쓴다. 반올림하거나 옮기지 않는다.
- 노드 이름은 근거에 실제로 등장한 것만 쓴다.
- 근거가 부족한 원인을 확정적으로 쓰지 않는다.
  "~이다"가 아니라 "~로 보인다", confidence는 Low로 둔다.
  확정할 수 없다는 것을 쓰는 것이 틀린 확신보다 낫다.
- 답할 수 없는 물음은 unresolved_questions에 남긴다. 지어내서 채우지 않는다.

이 분석 구간 **밖의** 시간을 봐야 답할 수 있는 것이 있으면
needs_more_context=true로 두고 suggested_windows에 필요한 범위를 쓴다.
구간 **안에서** 더 조사하면 되는 것은 여기 해당하지 않는다.

응답은 JSON 하나로만 한다.
"""


def build_analysis_prompt(
    *,
    cluster: str,
    window_label: str,
    analysis_goal: str,
    evidence: list[Evidence],
    observation_summary: str,
    candidates_for_prompt: str = "",
    prior_summary: str = "",
    gaps: tuple[str, ...] = (),
) -> str:
    """선별된 Evidence 전체를 놓고 원인을 묻는다."""
    sections = [
        _ANALYSIS_HEADER,
        f"클러스터: {cluster}",
        f"분석 구간: {window_label}",
    ]
    if analysis_goal:
        sections += ["", "이 구간을 분석하는 이유 (Supervisor가 정한 목표):", analysis_goal]
    if prior_summary:
        sections += [
            "",
            "같은 Incident의 앞선 분석 결과 (참고용, 이번 구간의 근거가 아니다):",
            prior_summary,
        ]
    sections += ["", "코드가 센 관측값 (모델이 옮겨 적지 않는다. 참고만 한다):", observation_summary]
    if candidates_for_prompt:
        sections += ["", candidates_for_prompt]
    if gaps:
        sections += [
            "",
            "이번 분석에서 확보하지 못한 것:",
            "\n".join(f"- {gap}" for gap in gaps),
        ]
    sections += [
        _ANALYSIS_RULES,
        "",
        f"--- 선별된 근거 {len(evidence)}건 ---",
        "\n".join(format_evidence_line(item) for item in evidence) or "(근거 없음)",
    ]
    return "\n".join(sections)


_REVISION_HEADER = """너는 방금 아래 리포트를 작성했다.
검증 단계가 근거와 리포트 사이의 불일치를 찾아냈다.

**지적된 것만 고친다.** 지적되지 않은 부분은 그대로 둔다.
새로운 원인을 만들어 내지 않는다.

고치는 방법은 둘 중 하나다.
- 근거를 제대로 인용하도록 고친다.
- 근거를 댈 수 없으면 그 주장을 **뺀다**. 빼는 것이 지어내는 것보다 낫다.
  확정적인 표현을 지적받았으면 표현을 낮춘다.
"""


def build_revision_prompt(
    *,
    report: LogAnalysisReport,
    issues: tuple[str, ...],
    evidence: list[Evidence],
) -> str:
    """검증이 잡은 불일치를 고쳐 다시 쓰게 한다."""
    findings = "\n".join(
        f"- [{item.severity or '미분류'}] {item.title} (근거: {', '.join(item.evidence_refs) or '없음'})"
        for item in report.findings
    )
    causes = "\n".join(
        f"- {item.statement} (confidence={item.confidence or '미기재'}, "
        f"근거: {', '.join(item.supporting_evidence_refs) or '없음'})"
        for item in report.root_causes
    )
    timeline = "\n".join(
        f"- {item.at:%Y-%m-%d %H:%M:%S} {item.description} "
        f"(근거: {', '.join(item.evidence_refs) or '없음'})"
        for item in report.timeline
    )
    return "\n".join(
        [
            _REVISION_HEADER,
            "",
            "--- 검증이 지적한 것 ---",
            "\n".join(f"{index}. {issue}" for index, issue in enumerate(issues, start=1)),
            "",
            "--- 현재 리포트 ---",
            f"요약: {report.summary or '(없음)'}",
            "",
            "타임라인:",
            timeline or "(없음)",
            "",
            "발견된 문제:",
            findings or "(없음)",
            "",
            "원인 후보:",
            causes or "(없음)",
            "",
            "미해결 물음:",
            "\n".join(f"- {item}" for item in report.unresolved_questions) or "(없음)",
            "",
            _ANALYSIS_RULES,
            "",
            f"--- 인용할 수 있는 근거 {len(evidence)}건 ---",
            "\n".join(format_evidence_line(item) for item in evidence) or "(근거 없음)",
        ]
    )
