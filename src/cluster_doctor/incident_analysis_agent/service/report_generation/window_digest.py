"""위키 조사 에이전트에 넘기는 구간 리포트 요약.

사건 종합 초안에는 구간 진단 문장을 싣지 않지만(근거 없는 주장에 고정된다), 위키 에이전트는 구간 해석을
출발점으로 인과를 재구성하므로 싣는다. 대신 검증이 모순을 찾은 구간(MISMATCH)은 해석을 빼고 근거 id만 남긴다.
"""
import json
import logging

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, VerificationStatus

_logger = logging.getLogger(__name__)

# 관측 JSON(24,000자)·색인·패턴 페이지와 같은 대화에 실린다.
WINDOW_DIGEST_MAX_CHARS = 12000
# 해석을 싣지 않는 판정. 모순이 확인됐거나 대조 자체를 못 했다.
_WITHHELD = (VerificationStatus.MISMATCH, VerificationStatus.UNCHECKED)


def _entry(report: LogAnalysisReport) -> dict:
    entry = {
        "from": report.analyzed_from.astimezone(KST).isoformat(),
        "to": report.analyzed_to.astimezone(KST).isoformat(),
        "verification_status": report.verification_status.value,
    }
    if report.verification_status in _WITHHELD:
        entry["evidence_refs"] = list(report.evidence_refs)
        return entry
    entry["summary"] = report.summary
    entry["summary_evidence_refs"] = list(report.summary_evidence_refs)
    entry["root_causes"] = [
        {"statement": cause.statement, "supporting_evidence_refs": list(cause.supporting_evidence_refs)}
        for cause in report.root_causes]
    entry["recommendations"] = [
        {"text": action.text, "evidence_refs": list(action.evidence_refs)} for action in report.recommendations]
    return entry


def window_digest(window_reports: list[LogAnalysisReport]) -> str:
    """구간 순서대로 싣고, 상한을 넘으면 뒤쪽 구간부터 뺀다. 원인은 앞 구간에서 만들어지는 경우가 많다."""
    entries = [_entry(report) for report in window_reports]
    kept = len(entries)
    text = json.dumps({"windows": entries}, ensure_ascii=False)
    while kept and len(text) > WINDOW_DIGEST_MAX_CHARS:
        kept -= 1
        text = json.dumps({"windows": entries[:kept]}, ensure_ascii=False)
    if kept < len(entries):
        _logger.warning("구간 리포트 요약이 상한 %d자를 넘어 앞쪽 %d/%d개 구간만 위키 조사에 싣는다.",
                        WINDOW_DIGEST_MAX_CHARS, kept, len(entries))
    return text if kept else ""
