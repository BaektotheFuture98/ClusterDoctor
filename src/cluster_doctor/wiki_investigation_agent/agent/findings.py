"""위키 조사 결과. 에이전트가 실제로 연 패턴 페이지, 클러스터 구성 문서, 재구성한 인과를 나른다."""

import logging
from dataclasses import dataclass

_logger = logging.getLogger(__name__)

# 고른 패턴 페이지를 프롬프트에 싣는 합계 상한. 구간 범위와 누적 관측값이 쓰는 예산을 침범하지 않는다.
PAGES_MAX_CHARS = 12000
# 재구성 초안을 프롬프트에 싣는 상한. 패턴·구성 문서와 합쳐도 사건 종합 관측 예산의 절반을 넘지 않게 둔다.
RECONSTRUCTIONS_MAX_CHARS = 4000

INFRA_SECTION_TITLE = "[인프라 구성]"


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
class ChainStep:
    """인과 사슬의 고리 하나. ``basis``는 evidence(이번 관측)·infra(구성 사실)·wiki(패턴 지식)다."""

    step: str
    basis: str
    refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Reconstruction:
    """위키 에이전트가 관측·구간 리포트·구성 문서·패턴을 대조해 세운 원인 후보 초안.

    관측 고리가 하나 이상 있어야 남는다. 사건 종합이 검토하는 초안이고 결론이 아니다.
    """

    statement: str
    chain: tuple[ChainStep, ...]
    wiki_ref: str = ""


def _ref_label(step: ChainStep) -> str:
    if step.basis == "evidence":
        return f"근거 {', '.join(step.refs)}"
    if step.basis == "infra":
        return "인프라 구성"
    return f"위키 {', '.join(step.refs)}" if step.refs else "위키"


@dataclass(frozen=True)
class WikiFindings:
    patterns: tuple[WikiPattern, ...] = ()
    infra: str = ""
    reconstructions: tuple[Reconstruction, ...] = ()

    @property
    def known_ids(self) -> tuple[str, ...]:
        return tuple(pattern.pattern_id for pattern in self.patterns)

    def _pages_block(self) -> str:
        if not self.patterns:
            return ""
        joined = "\n\n".join(f"[패턴 {pattern.pattern_id}]\n{pattern.page}" for pattern in self.patterns)
        cut = cut_at_line(joined, PAGES_MAX_CHARS)
        if len(cut) < len(joined):
            _logger.warning(
                "위키 패턴 페이지 합계(%d자)가 상한 %d자를 넘어 %d자까지만 싣는다. 뒤쪽 패턴은 일부만 보인다.",
                len(joined), PAGES_MAX_CHARS, len(cut))
        return cut

    def _reconstructions_block(self) -> str:
        if not self.reconstructions:
            return ""
        parts = []
        for index, item in enumerate(self.reconstructions, 1):
            head = f"[재구성 초안 {index}] {item.statement}" + (f" (패턴 {item.wiki_ref})" if item.wiki_ref else "")
            parts.append("\n".join([head, *(f"- {s.step} [{_ref_label(s)}]" for s in item.chain)]))
        joined = "\n\n".join(parts)
        cut = cut_at_line(joined, RECONSTRUCTIONS_MAX_CHARS)
        if len(cut) < len(joined):
            _logger.warning("위키 재구성 초안(%d자)이 상한 %d자를 넘어 %d자까지만 싣는다.",
                            len(joined), RECONSTRUCTIONS_MAX_CHARS, len(cut))
        return cut

    def as_prompt_block(self) -> str:
        """프롬프트에 싣는 본문. 구성 문서·재구성 초안·패턴 페이지 순서이고 각자 상한에서 잘린다."""
        infra = f"{INFRA_SECTION_TITLE}\n{self.infra}" if self.infra else ""
        return "\n\n".join(block for block in (infra, self._reconstructions_block(), self._pages_block()) if block)
