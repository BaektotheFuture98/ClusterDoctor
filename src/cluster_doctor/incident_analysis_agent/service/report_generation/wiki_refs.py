"""위키 색인에서 패턴 id를 읽고, 리포트의 ``wiki_ref``를 그 id로 제한한다."""

import re
from collections.abc import Collection

from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport

_INDEX_ROW_ID = re.compile(r"^\|\s*\[([^\]\s]+)\]\(", re.MULTILINE)


def index_pattern_ids(index: str) -> tuple[str, ...]:
    """색인 표 행 ``| [<id>](…) | …``에서 패턴 id를 등장 순서대로, 중복 없이 뽑는다."""
    return tuple(dict.fromkeys(_INDEX_ROW_ID.findall(index)))


def keep_known_wiki_refs(report: LogAnalysisReport, known_ids: Collection[str]) -> LogAnalysisReport:
    """위키 색인에 실제로 있는 패턴 id만 ``wiki_ref``로 남긴다.

    모델이 지어낸 id가 위키 배지로 나가면 운영자는 존재하지 않는 패턴을 찾게 된다.
    """
    def known(ref: str) -> bool:
        return ref.strip() in known_ids

    recommendations = tuple(
        item.model_copy(update={"wiki_ref": item.wiki_ref.strip() if known(item.wiki_ref) else ""})
        if item.wiki_ref else item
        for item in report.recommendations
    )
    return report.model_copy(update={"recommendations": recommendations})
