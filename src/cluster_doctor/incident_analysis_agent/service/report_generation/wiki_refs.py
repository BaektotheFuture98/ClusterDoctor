"""리포트의 ``wiki_ref``를 주어진 패턴 id로 제한한다."""

from collections.abc import Collection

from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport


def keep_known_wiki_refs(report: LogAnalysisReport, known_ids: Collection[str]) -> LogAnalysisReport:
    """주어진 패턴 id 목록에 있는 ``wiki_ref``만 남긴다.

    호출처는 위키 조사 에이전트가 열어 고른 패턴 id(``WikiFindings.known_ids``)를 넘긴다.
    모델이 지어낸 id가 위키 배지로 나가면 운영자는 존재하지 않는 패턴을 찾게 된다.

    원인의 ``wiki_ref``는 ``supporting_evidence_refs``가 비어 있으면 지운다. 위키는 지식 근거라
    관측 근거 없이 위키만으로 선 원인에 배지를 달지 않는다. 이 검사는 관측 근거가 있다는 것까지만
    보고, 그 근거가 패턴의 증상과 맞는지는 프롬프트와 교정 단계가 본다.
    """
    def known(ref: str) -> bool:
        return ref.strip() in known_ids

    recommendations = tuple(
        item.model_copy(update={"wiki_ref": item.wiki_ref.strip() if known(item.wiki_ref) else ""})
        if item.wiki_ref else item
        for item in report.recommendations
    )
    root_causes = tuple(
        item.model_copy(update={
            "wiki_ref": item.wiki_ref.strip() if known(item.wiki_ref) and item.supporting_evidence_refs else "",
        })
        if item.wiki_ref else item
        for item in report.root_causes
    )
    return report.model_copy(update={"recommendations": recommendations, "root_causes": root_causes})
