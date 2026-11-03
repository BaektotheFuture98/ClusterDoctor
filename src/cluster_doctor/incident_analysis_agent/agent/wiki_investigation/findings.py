"""위키 조사 결과. 에이전트가 실제로 연 패턴 페이지를 나른다."""

import logging
from dataclasses import dataclass

_logger = logging.getLogger(__name__)

# 고른 패턴 페이지를 프롬프트에 싣는 합계 상한. 구간 범위와 누적 관측값이 쓰는 예산을 침범하지 않는다.
PAGES_MAX_CHARS = 12000


def cut_at_line(text: str, limit: int) -> str:
    """``limit`` 이내에서 마지막 줄바꿈까지만 남긴다. 줄바꿈이 없으면 ``limit``에서 자른다."""
    if len(text) <= limit:
        return text
    head = text[:limit]
    newline = head.rfind("\n")
    return head[:newline] if newline > 0 else head


@dataclass(frozen=True)
class WikiPattern:
    """``reason``은 고른 이유를 로그에 남기기 위한 값이고 프롬프트에는 싣지 않는다."""

    pattern_id: str
    reason: str
    page: str

    @property
    def title(self) -> str:
        """페이지 본문의 첫 ``# `` 제목. 없으면 패턴 id."""
        for line in self.page.splitlines():
            if line.startswith("# "):
                return line[2:].strip() or self.pattern_id
        return self.pattern_id


@dataclass(frozen=True)
class WikiFindings:
    patterns: tuple[WikiPattern, ...] = ()

    @property
    def known_ids(self) -> tuple[str, ...]:
        return tuple(pattern.pattern_id for pattern in self.patterns)

    def as_prompt_block(self) -> str:
        """프롬프트에 싣는 본문. 합계가 상한을 넘으면 뒤쪽 패턴이 잘리고 경고한다."""
        if not self.patterns:
            return ""
        joined = "\n\n".join(f"[패턴 {pattern.pattern_id}]\n{pattern.page}" for pattern in self.patterns)
        cut = cut_at_line(joined, PAGES_MAX_CHARS)
        if len(cut) < len(joined):
            _logger.warning(
                "위키 패턴 페이지 합계(%d자)가 상한 %d자를 넘어 %d자까지만 싣는다. 뒤쪽 패턴은 일부만 보인다.",
                len(joined), PAGES_MAX_CHARS, len(cut))
        return cut
