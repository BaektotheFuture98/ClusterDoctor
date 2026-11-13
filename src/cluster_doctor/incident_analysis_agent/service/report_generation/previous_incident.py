"""세션의 이전 incident 보고서를 다음 incident 프롬프트에 싣는 요약.

이전 보고서는 이어서 쓰기 위한 맥락이지 이번 incident의 관측이 아니다. 근거
대조 대상에 넣지 않고, 프롬프트에는 "참고용"이라고 표시한 자리에만 싣는다.
"""

from __future__ import annotations

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport

# 원인·조치를 몇 개까지 싣는가. 전문을 실으면 incident마다 리포트가 하나씩 더 붙는다.
_MAX_ITEMS = 3


def previous_incident_digest(report: LogAnalysisReport) -> str:
    start = report.analyzed_from.astimezone(KST)
    end = report.analyzed_to.astimezone(KST)
    lines = [f"[{start:%H:%M}~{end:%H:%M}] {report.summary}"]
    lines += [f"- 원인: {cause.statement}" for cause in report.root_causes[:_MAX_ITEMS]]
    lines += [f"- 조치: {item.text}" for item in report.recommendations[:_MAX_ITEMS]]
    return "\n".join(lines)
