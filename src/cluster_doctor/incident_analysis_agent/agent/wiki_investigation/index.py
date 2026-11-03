"""위키 색인 표에서 패턴 id를 읽는다."""

import re

_INDEX_ROW_ID = re.compile(r"^\|\s*\[([^\]\s]+)\]\(", re.MULTILINE)


def index_pattern_ids(index: str) -> tuple[str, ...]:
    """색인 표 행 ``| [<id>](…) | …``에서 패턴 id를 등장 순서대로, 중복 없이 뽑는다."""
    return tuple(dict.fromkeys(_INDEX_ROW_ID.findall(index)))
