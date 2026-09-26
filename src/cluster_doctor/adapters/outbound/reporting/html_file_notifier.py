"""진단 리포트를 HTML 파일로 남기는 ReportPublisher 구현.

리포트는 LLM이 쓴 **평문**이다(프롬프트가 마크다운을 금지한다). 형식은
"1. 섹션 제목" + ``──`` 구분선 + ``•`` 불렛 + 두 칸 들여쓴 ``-`` 세부로 정해져
있으므로, 그 구조를 파싱해 제목·목록·본문으로 그린다.

세 가지를 지킨다.

1. **모든 텍스트를 이스케이프한다.** 리포트에는 ES 쿼리 원문과 로그 줄이
   그대로 인용되고, 그 안에는 외부 사용자가 보낸 검색어와 ``<``·``&``가
   들어온다. 이스케이프하지 않으면 페이지가 깨지거나 마크업이 주입된다.
2. **원문을 잃지 않는다.** 모델이 형식을 어기면 파싱이 어긋날 수 있으므로
   평문 전문을 페이지 끝에 항상 함께 싣는다. 파싱 결과가 리포트를 대체하는
   것이 아니라 읽기 쉽게 덧입히는 것이다.
3. **어떤 실패로도 진단을 잃지 않는다.** ``notify``에서 예외가 새면
   ``_run_agent``의 ``succeeded``가 False로 남아 리포트가 사라지고 재트리거도
   막힌다. 그래서 경로 문제뿐 아니라 렌더링·인코딩 실패까지 잡아 전문을
   로그로 떨어뜨린다 — 파일이 없어도 운영자가 읽을 길은 남는다.

외부 리소스를 한 개도 참조하지 않는다. 폐쇄망 서버에서 열리는 파일이므로
웹폰트·CDN을 쓰지 않고 CSS를 인라인으로 싣는다.
"""

from __future__ import annotations

import asyncio
import html
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cluster_doctor.domain.diagnosis.observations import (
    DiagnosisReport,
    Observations,
    observed_severity,
)
from cluster_doctor.application.ports.report_publisher import (
    ReportPublication,
    ReportPublisher,
)
from cluster_doctor.adapters.outbound.reporting.report_text import (
    SEVERITY_PREFIX,
    candidate_details,
    candidate_line,
    health_lines,
    master_log_lines,
    node_lines,
    overview_lines,
    render_text,
    scrub,
    timeline_line,
)

_logger = logging.getLogger(__name__)

_KST = timezone(timedelta(hours=9))

# "1. 요청 이해" / "3. 근본 원인" — 최종 리포트의 섹션 제목.
_SECTION_RE = re.compile(r"^(\d{1,2})\.\s+(\S.*)$")
# 프롬프트가 지시한 불렛은 "•"다. 모델이 "·"나 "*"를 쓰는 경우도 받는다.
_BULLET_RE = re.compile(r"^[•·*]\s*(\S.*)$")
# 두 칸 들여쓴 "- 세부 내용". 들여쓰기는 이미 벗겨진 상태로 검사한다.
_SUB_RE = re.compile(r"^[-–]\s+(\S.*)$")
# "섹션 제목 아래 ── 구분선" — 화면에서는 CSS가 그리므로 버린다.
_DIVIDER_RE = re.compile(r"^[─━—–\-=_]{3,}$")
# 줄 앞머리의 심각도 표기. 배지로 뽑아내 한눈에 보이게 한다.
_SEVERITY_RE = re.compile(r"^(Critical|Warning|Info)\s*[:\-]?\s*", re.IGNORECASE)
# 쿼리 원문·로그 줄처럼 그대로 보여야 하는 것. 등폭으로 그리고 줄바꿈을 살린다.
#
# **평문 폴백에서만 쓰인다.** 관측값 섹션은 mono=True로 명시 지정되므로
# (``_mono_attr``) 힌트 매칭을 타지 않는다. 그래서 예전에 관측값 줄을 위해
# 넣어 둔 "slowlog=" 같은 항목은 근거가 사라져 뺐다 — 남겨 두면 다음 사람이
# 관측값 렌더링이 이 목록에 의존한다고 읽는다.
_RAW_HINTS = ('{"', '":', "took=", "node=", "[SLOWLOG]", "[METRIC]")

_FILENAME_FORMAT = "report-%Y%m%d-%H%M%S"


class HtmlFileReportPublisher(ReportPublisher):
    """리포트를 ``output_dir`` 아래 HTML 파일 한 건으로 저장한다.

    디렉터리는 생성자가 아니라 첫 저장 시점에 만든다. 생성자에서 만들면
    ``build_trigger_service``를 부르는 것만으로 디렉터리가 생겨, 의존성 조립을
    검증하는 테스트가 작업 디렉터리에 흔적을 남긴다.
    """

    def __init__(self, output_dir: str | Path = "reports") -> None:
        self._output_dir = Path(output_dir)

    async def publish(
        self,
        report: DiagnosisReport,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> ReportPublication:
        # 인코딩 불가 문자는 _e()가 걸러낸다. 리포트가 객체가 되면서 문자열이
        # 수십 곳에서 나오므로 진입부에서 한 번 치환하는 것으로는 부족하다.
        # 폴백 로그만 여기서 따로 치환한다 — 그쪽은 _e를 타지 않는다.
        gaps = tuple(scrub(gap) for gap in gaps)

        # 파일 쓰기는 짧지만 이벤트 루프에서 하지 않는다. 같은 루프가 Kafka를
        # 계속 소비하고 있고, 리포트는 수십 KB까지 자란다.
        try:
            text_length = len(render_text(report))
            path = await asyncio.to_thread(
                self._write, report, gaps, analysis_failed
            )
        except Exception as exc:  # noqa: BLE001
            # OSError만 잡으면 보장이 깨진다. 렌더링·인코딩 실패도 여기로
            # 와야 한다 — 예를 들어 provider 응답에 짝 없는 서로게이트가
            # 섞이면 write_text가 UnicodeEncodeError(ValueError)를 내는데,
            # 그것이 새어 나가면 _run_agent의 succeeded가 False로 남아
            # 리포트가 사라지고 이 폴백조차 타지 못한다.
            _logger.error("리포트 HTML 저장 실패(%s) — 전문을 로그로 남긴다", exc)
            try:
                _logger.info("\n%s", scrub(render_text(report)))
            except Exception:  # noqa: BLE001
                # 렌더링 자체가 실패한 경우다. 그때도 이 폴백이 죽으면 안 된다.
                _logger.exception("리포트 평문 렌더링도 실패했다")
            return ReportPublication(text_length=locals().get("text_length", 0))

        _logger.info("리포트 저장: %s", path)
        return ReportPublication(text_length=text_length)

    def _write(
        self,
        report: DiagnosisReport,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> Path:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now(_KST)
        path = _unique_path(self._output_dir, now)
        path.write_text(
            render_report(
                report,
                generated_at=now,
                gaps=gaps,
                analysis_failed=analysis_failed,
            ),
            encoding="utf-8",
        )
        return path


def _unique_path(output_dir: Path, now: datetime) -> Path:
    """같은 초에 두 건이 저장되어도 앞의 것을 덮지 않게 한다.

    재트리거는 10초 간격이라 부딪히지 않지만, 인스턴스를 둘 띄우면 같은 초에
    저장될 수 있다. 파일명에 콜론을 쓰지 않는 것도 의도적이다 — Windows에서
    쓸 수 없는 문자다.
    """
    stem = now.strftime(_FILENAME_FORMAT)
    path = output_dir / f"{stem}.html"
    suffix = 2
    while path.exists():
        path = output_dir / f"{stem}-{suffix}.html"
        suffix += 1
    return path


# ────────────────────────── 평문 → 구조 ──────────────────────────


def _make_bullet(text: str) -> dict:
    """불렛 항목 하나를 만들고 앞머리 심각도 표기를 배지로 떼어낸다.

    ``•`` 줄과 불렛 없이 온 ``-`` 줄이 같은 처리를 받아야 한다. 한쪽만
    심각도를 떼면 같은 내용이 섹션 형식에 따라 다르게 그려진다.
    """
    severity = None
    severity_match = _SEVERITY_RE.match(text)
    if severity_match:
        severity = severity_match.group(1).capitalize()
        text = text[severity_match.end():].strip()
    return {"kind": "bullet", "text": text, "severity": severity, "subs": []}


class _Section:
    def __init__(self, number: str, title: str, index: int) -> None:
        self.number = number
        self.title = title
        self.anchor = f"sec-{index}"
        self.items: list[dict] = []


def _parse(message: str) -> list[_Section]:
    """리포트 평문을 섹션 목록으로 가른다.

    섹션 제목이 하나도 없으면 빈 리스트를 돌려준다. 그 경우 호출자가 전문을
    그대로 그린다 — 형식을 어긴 응답을 억지로 쪼개면 없는 구조를 만들어낸다.
    """
    sections: list[_Section] = []
    current: _Section | None = None
    # 섹션 제목을 한 번이라도 봤는지. 제목 앞에 나온 줄을 담는 "개요" 섹션과
    # "제목이 아예 없는 응답"을 구별하려면 이 표식이 필요하다. 없으면 후자도
    # 섹션 하나로 감싸져 전문 폴백이 영원히 죽는다.
    saw_section = False

    for raw_line in message.splitlines():
        line = raw_line.strip()
        if not line or _DIVIDER_RE.match(line):
            continue

        section_match = _SECTION_RE.match(line)
        if section_match:
            saw_section = True
            current = _Section(
                number=section_match.group(1),
                title=section_match.group(2).strip(),
                index=len(sections) + 1,
            )
            sections.append(current)
            continue

        if current is None:
            # 섹션 제목보다 앞서 나온 줄. 서두로 담을 곳이 없으므로 버리지 않고
            # 무제 섹션에 모은다.
            current = _Section(number="", title="개요", index=len(sections) + 1)
            sections.append(current)

        bullet_match = _BULLET_RE.match(line)
        if bullet_match:
            current.items.append(_make_bullet(bullet_match.group(1)))
            continue

        sub_match = _SUB_RE.match(line)
        if sub_match:
            if current.items and current.items[-1]["kind"] == "bullet":
                current.items[-1]["subs"].append(sub_match.group(1))
            else:
                # 불렛 없이 세부 줄이 바로 오는 경우가 있다. 리포트 형식이
                # 지시하는 섹션 1("인시던트 개요")이 그 형태다 —
                # "   - 유입 관찰: …"가 앞선 "•" 없이 나온다. 문단으로 떨구면
                # 대시가 본문에 그대로 남아 목록이 아니게 되므로 불렛으로
                # 승격시킨다.
                current.items.append(_make_bullet(sub_match.group(1)))
            continue

        is_raw = any(hint in line for hint in _RAW_HINTS)

        # 들여쓴 채 이어지는 줄은 앞 항목의 연속이다. 별도 문단으로 떼면 한
        # 문장이 불렛과 문단으로 쪼개져 읽는 순서가 무너진다(실제로 "근본 원인"
        # 섹션이 그렇게 깨졌다). 쿼리 원문처럼 보이는 줄은 예외로 두고 그대로
        # 등폭 블록에 담는다 — 그것은 연속이 아니라 인용이다.
        if raw_line[:1].isspace() and not is_raw and current.items:
            last = current.items[-1]
            if last["kind"] == "bullet":
                if last["subs"]:
                    last["subs"][-1] = f"{last['subs'][-1]} {line}"
                else:
                    last["text"] = f"{last['text']} {line}"
                continue

        current.items.append({"kind": "raw" if is_raw else "para", "text": line})

    if not saw_section:
        return []
    return sections


# ────────────────────────── 구조 → HTML ──────────────────────────


def _e(text: str) -> str:
    """이 모듈에서 텍스트가 HTML로 들어가는 유일한 통로.

    여기서 ``scrub``을 함께 건다. 리포트가 객체라 문자열이 수십 곳에서 나오므로
    진입부에서 한 번 치환하는 것으로는 부족하다. 통로가 하나이니 여기서 거는
    것이 가장 적게 틀린다.
    """
    return html.escape(scrub(text), quote=True)


def _mono_attr(text: str, mono: bool | None = None) -> str:
    """수치·로그 원문이 실린 줄은 등폭으로 그린다.

    ``pre.raw``는 불렛이 아닌 줄에만 적용된다. 그런데 분 단위 타임라인과
    노드 상태는 불렛으로 오고, 거기 실리는 것은 줄을 맞춰 읽어야 하는
    수치다 — 비례 폭으로 그리면 분마다의 jvm_heap이나 rejected를 위아래로
    비교할 수 없다. 근거로 인용된 로그 원문도 같다.

    ``mono``를 명시하면 그것이 이긴다. 관측값 섹션은 코드가 만드는 줄이라
    등폭이어야 한다는 것을 이미 알고 있고, 그것을 문자열 힌트 매칭이라는
    우연에 맡길 이유가 없다. 힌트 매칭은 평문 폴백 경로를 위해 남는다.
    """
    if mono is not None:
        return ' class="mono"' if mono else ""
    return ' class="mono"' if any(hint in text for hint in _RAW_HINTS) else ""


def _render_items(items: list[dict]) -> str:
    out: list[str] = []
    bullets: list[dict] = []

    def flush() -> None:
        if not bullets:
            return
        rows = []
        for bullet in bullets:
            badge = ""
            if bullet["severity"]:
                level = bullet["severity"].lower()
                badge = (
                    f'<span class="sev sev-{level}">{_e(bullet["severity"])}</span>'
                )
            subs = ""
            if bullet["subs"]:
                sub_rows = "".join(
                    f'<li{_mono_attr(sub)}>{_e(sub)}</li>' for sub in bullet["subs"]
                )
                subs = f'<ul class="subs">{sub_rows}</ul>'
            rows.append(
                f"<li>{badge}"
                f'<span{_mono_attr(bullet["text"], bullet.get("mono"))}>'
                f'{_e(bullet["text"])}</span>'
                f"{subs}</li>"
            )
        out.append(f'<ul class="bullets">{"".join(rows)}</ul>')
        bullets.clear()

    for item in items:
        if item["kind"] == "bullet":
            bullets.append(item)
            continue
        flush()
        if item["kind"] == "raw":
            out.append(f'<pre class="raw">{_e(item["text"])}</pre>')
        else:
            out.append(f"<p>{_e(item['text'])}</p>")
    flush()
    return "\n".join(out)


def _overview_block(obs: Observations) -> list[dict]:
    """개요 불렛. 코드 판정 심각도 줄만 배지를 받는다.

    배지를 붙이는 이유는 눈에 띄어야 해서다 — 모델이 severity를 채우지 않는
    일이 반복돼 노드 이탈이 분류 없이 나갔고, 그때 리포트에서 심각도를 말하는
    것은 이 줄뿐이다. 판정 근거는 줄 안에 이미 적혀 있다.
    """
    level, _reasons = observed_severity(obs)
    return [
        _bullet(line, severity=level or None)
        if line.startswith(SEVERITY_PREFIX)
        else _bullet(line)
        for line in overview_lines(obs)
    ]


def _bullet(
    text: str,
    *,
    severity: str | None = None,
    subs: list[str] | None = None,
    mono: bool | None = None,
) -> dict:
    """``_render_items``가 먹는 불렛 항목을 직접 만든다.

    ``_make_bullet``을 쓰지 않는 이유: 그 함수의 존재 이유는 평문에서
    ``"Critical: …"`` 앞머리를 정규식으로 떼어내는 것인데, 관측값과 구조화
    narrative에서는 ``severity``가 값으로 온다. 정규식을 다시 태우면 본문에
    우연히 들어간 "Info"가 배지로 승격될 수 있다.
    """
    return {
        "kind": "bullet",
        "text": text,
        "severity": severity,
        "subs": subs or [],
        "mono": mono,
    }


def _raw_block(lines: list[str]) -> list[dict]:
    """여러 줄을 등폭 블록 하나로 묶는다.

    줄마다 항목을 만들지 않는 이유: 타임라인·노드 표는 **줄을 맞춰 위아래로
    비교**하는 것이 용도다. 항목이 나뉘면 사이에 여백이 들어가 그 정렬이
    깨진다.
    """
    return [{"kind": "raw", "text": "\n".join(lines)}] if lines else []


def _sections_from_report(report: DiagnosisReport) -> list[_Section]:
    """``DiagnosisReport``를 기존 ``_Section`` 표현으로 옮긴다.

    렌더러를 새로 쓰지 않는 이유: ``_render_items``·``_e``·``_CSS``·TOC·배너가
    전부 그대로 쓸 수 있고, 새로 쓰면 다크모드·인쇄·``sev-*`` 배지가 전부
    회귀 대상이 된다. 어댑터 하나로 끝나는 일이다.

    관측값을 앞에, 판단을 뒤에 둔다. 프롬프트가 지시하던 *"2번과 3번은
    관찰값만 적는 섹션이다. 판단은 4번부터"* 를 구조로 고정하는 것이다 —
    지시는 어길 수 있지만 구조는 어길 수 없다.
    """
    obs = report.observations
    blocks: list[tuple[str, list[dict]]] = [
        ("인시던트 개요", _overview_block(obs)),
        (
            "분 단위 타임라인 (관측값)",
            _raw_block([timeline_line(row) for row in obs.timeline]),
        ),
        (
            "클러스터 상태 이력 (관측값)",
            _raw_block(health_lines(obs.health, obs.requested)),
        ),
        ("노드별 구간 최대값 (관측값)", _raw_block(node_lines(obs.nodes))),
    ]

    blocks.append(
        (
            "마스터 노드 로그 (관측값)",
            _raw_block(master_log_lines(obs.master_events, obs.master_log_total)),
        )
    )

    picks = {
        pick.candidate_id: pick.reason
        for pick in (report.narrative.suspect_picks if report.narrative else ())
    }
    candidates = [
        _bullet(
            candidate_line(candidate),
            subs=candidate_details(candidate, picks.get(candidate.candidate_id, "")),
            mono=True,
        )
        for candidate in obs.candidates
    ]
    blocks.append(("느린 요청 후보 (관측값 + 모델 선정)", candidates))

    narrative = report.narrative
    if narrative is not None:
        conclusion = [_bullet(narrative.headline)] if narrative.headline else []
        conclusion += [_bullet(line) for line in narrative.context]
        blocks.append(("결론", conclusion))

        blocks.append(
            (
                "발견된 문제점",
                [
                    _bullet(
                        finding.title,
                        severity=finding.severity or None,
                        subs=list(finding.evidence),
                    )
                    for finding in narrative.findings
                ],
            )
        )

        cause = [_bullet(narrative.root_cause)] if narrative.root_cause else []
        cause += [_bullet(f"근거: {item}") for item in narrative.supporting]
        cause += [_bullet(f"반박 근거: {item}") for item in narrative.contradicting]
        cause += [_bullet(f"확인하지 못한 것: {item}") for item in narrative.unverified]
        blocks.append(("근본 원인", cause))

        blocks.append(
            ("권장 조치", [_bullet(item) for item in narrative.recommendations])
        )

    sections: list[_Section] = []

    def add(title: str, items: list[dict]) -> None:
        if not items:
            return
        index = len(sections) + 1
        section = _Section(number=str(index), title=title, index=index)
        section.items = items
        sections.append(section)

    for title, items in blocks:
        add(title, items)

    # 구조화 출력이 실패했을 때의 자리. 모델이 평문은 남겼으므로 기존 정규식
    # 파서로 그린다 — 이 경로 때문에 _parse를 지우지 않는다.
    if narrative is None and report.narrative_text:
        parsed = _parse(report.narrative_text)
        if parsed:
            for section in parsed:
                add(section.title, section.items)
        else:
            # 섹션 제목조차 없는 응답. 구조를 지어내지 않고 전문을 싣는다.
            add("모델 리포트 (평문)", [{"kind": "raw", "text": report.narrative_text}])

    return sections


def render_report(
    report: DiagnosisReport,
    generated_at: datetime | None = None,
    gaps: tuple[str, ...] = (),
    analysis_failed: bool = False,
) -> str:
    """리포트를 완결된 HTML 문서 한 장으로 만든다.

    ``generated_at``은 테스트가 시각을 고정할 수 있게 열어 뒀다.

    ``gaps``와 ``analysis_failed``는 배너로 그린다. 모델이 쓴 본문에 섞지
    않는 이유는 두 가지다 — 본문과 시스템이 덧붙인 사실이 구별되어야 하고,
    모델이 프롬프트를 어겨 누락을 밝히지 않았더라도 이 배너는 반드시 남는다.
    """
    now = generated_at or datetime.now(_KST)
    sections = _sections_from_report(report)
    source = render_text(report)

    banners = []
    if analysis_failed:
        banners.append(
            '<div class="banner banner-fail">'
            "<b>이 진단은 분석에 실패했다.</b> "
            "아래 본문은 agent가 작성한 것이지만 분석 근거가 확보되지 않았으므로 "
            "결론을 신뢰할 수 없다. 같은 사고는 다음 slowlog가 도착할 때 다시 "
            "진단된다(재트리거는 걸리지 않는다)."
            "</div>"
        )
    if gaps:
        items = "".join(f"<li>{_e(gap)}</li>" for gap in gaps)
        banners.append(
            '<div class="banner">'
            "<b>수집하지 못한 근거가 있다.</b> "
            "리포트의 결론은 아래 항목 없이 도출된 것이다."
            f"<ul>{items}</ul>"
            "</div>"
        )
    if any(row.failed for row in report.observations.timeline):
        # 본문에 "분석 실패"라는 문자열이 있는지로 판정하지 않는다. 그것은
        # 모델이 그 말을 옮겨 적어 줘야만 성립하는 휴리스틱이다 — 코드는
        # 실패한 분의 row.failed로 그 사실을 정확히 안다.
        #
        # elif가 아니라 if다. row.failed가 참이면 state.mark_gap이 반드시
        # gaps에도 남기므로, elif로 두면 이 배너가 **한 번도 뜨지 않는다.**
        # 두 배너는 다른 말을 한다 — 위는 "무엇이 빠졌는가", 이쪽은
        # "어느 구간을 믿을 수 없는가"다.
        banners.append(
            '<div class="banner">'
            "<b>이 리포트에는 분석하지 못한 구간이 있다.</b> "
            "해당 구간은 LLM 호출이 실패해 내용이 비어 있으며, 리포트의 결론은 "
            "남은 구간만 근거로 한다."
            "</div>"
        )
    # 구조화 출력이 실패한 사실은 여기서 배너로 그리지 않는다. 그것은 gaps
    # 항목이고, 위 gaps 배너가 이미 그린다. 여기 따로 두면 "리포트가 온전한가"를
    # 두 곳에서 판정하게 되고, 두 판정은 언젠가 어긋난다.
    banner = "\n".join(banners)

    if sections:
        toc_rows = "".join(
            f'<li><a href="#{s.anchor}">'
            f'<span class="n">{_e(s.number) or "—"}</span>{_e(s.title)}</a></li>'
            for s in sections
        )
        toc = f'<nav class="toc"><ol>{toc_rows}</ol></nav>'
        body = "\n".join(
            f'<section id="{s.anchor}">'
            f'<h2><span class="n">{_e(s.number)}</span>{_e(s.title)}</h2>'
            f"{_render_items(s.items)}"
            "</section>"
            for s in sections
        )
    else:
        # 관측값도 판단도 하나도 없다. 여기까지 오는 것은 분석이 한 번도
        # 성공하지 않은 실행뿐이다.
        toc = ""
        body = (
            '<section><h2><span class="n"></span>리포트 전문</h2>'
            '<p class="hint">그릴 수 있는 관측값도 판단도 없다.</p>'
            f'<pre class="raw">{_e(source)}</pre></section>'
        )

    return _DOCUMENT.format(
        stamp=_e(now.strftime("%Y-%m-%d %H:%M:%S")),
        banner=banner,
        toc=toc,
        body=body,
        source=_e(source),
        css=_CSS,
    )


_CSS = """
:root{color-scheme:light dark;
--ground:#f4f6f8;--surface:#fff;--surface-2:#eceff3;--ink:#151a21;--ink-2:#58626f;
--ink-3:#7b8593;--line:#dde2e8;--line-strong:#c2cad4;--accent:#2f47b5;
--accent-soft:#e7ebfa;--accent-ink:#23378f;--ok:#1c7a49;--warn:#9a6b06;
--warn-soft:#f8eeda;--crit:#ab2419;--crit-soft:#fae7e4;
--sans:"Malgun Gothic","Apple SD Gothic Neo",system-ui,sans-serif;
--mono:Consolas,"D2Coding","Cascadia Mono",monospace}
@media (prefers-color-scheme:dark){:root{
--ground:#0f1217;--surface:#171b22;--surface-2:#1d222b;--ink:#e8ebf0;--ink-2:#9ba5b3;
--ink-3:#78828f;--line:#272d38;--line-strong:#3a4250;--accent:#8fa3ff;
--accent-soft:#1c2340;--accent-ink:#b3c0ff;--ok:#4cb87c;--warn:#dfa93e;
--warn-soft:#2c2313;--crit:#f0857b;--crit-soft:#2f1917}}
*{box-sizing:border-box}
body{margin:0;background:var(--ground);color:var(--ink);font-family:var(--sans);
font-size:15px;line-height:1.75;-webkit-text-size-adjust:100%}
.wrap{max-width:980px;margin-inline:auto;padding-inline:clamp(16px,4vw,40px);
padding-block:clamp(28px,5vw,52px) 88px}
header{display:flex;flex-direction:column;gap:8px;
border-bottom:1px solid var(--line);padding-bottom:22px}
.eyebrow{font-family:var(--mono);font-size:11.5px;letter-spacing:.11em;
text-transform:uppercase;color:var(--accent-ink)}
h1{margin:0;font-size:clamp(25px,4vw,36px);line-height:1.14;letter-spacing:-.02em;
text-wrap:balance}
.stamp{font-family:var(--mono);font-size:12.5px;color:var(--ink-2);
font-variant-numeric:tabular-nums}
.banner{margin-top:24px;padding:13px 16px;background:var(--warn-soft);
border:1px solid var(--warn);border-left-width:3px;border-radius:0 4px 4px 0;
font-size:13.5px}
.banner+.banner{margin-top:10px}
.banner-fail{background:var(--crit-soft);border-color:var(--crit)}
.banner ul{margin:8px 0 0;padding-left:18px;display:flex;flex-direction:column;gap:4px}
.banner li{font-family:var(--mono);font-size:12.5px}
.toc{margin-top:28px;background:var(--surface);border:1px solid var(--line);
border-radius:5px;padding:14px 18px}
.toc ol{margin:0;padding:0;list-style:none;display:grid;gap:2px 20px;
grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}
.toc a{color:var(--ink-2);text-decoration:none;font-size:13.5px;
display:flex;gap:9px;padding:3px 0}
.toc a:hover{color:var(--accent-ink)}
.toc .n{font-family:var(--mono);color:var(--ink-3);min-width:1.4em;
font-variant-numeric:tabular-nums}
section{margin-top:38px}
h2{margin:0 0 14px;font-size:19px;letter-spacing:-.01em;line-height:1.3;
display:flex;gap:11px;align-items:baseline;padding-bottom:9px;
border-bottom:1px solid var(--line);text-wrap:balance}
h2 .n{font-family:var(--mono);font-size:14px;color:var(--accent);
font-variant-numeric:tabular-nums}
p{margin:0 0 11px;max-width:74ch}
.hint{color:var(--ink-2);font-size:13.5px}
ul.bullets{margin:0 0 12px;padding:0;list-style:none;
display:flex;flex-direction:column;gap:11px}
ul.bullets>li{display:flex;flex-wrap:wrap;align-items:baseline;gap:8px;
padding-left:15px;position:relative;max-width:82ch}
ul.bullets>li::before{content:"";position:absolute;left:0;top:.62em;
width:5px;height:5px;border-radius:50%;background:var(--line-strong)}
ul.subs{list-style:none;margin:7px 0 0;padding:0 0 0 3px;flex:1 1 100%;
display:flex;flex-direction:column;gap:5px;
border-left:2px solid var(--line);padding-left:13px}
ul.subs>li{color:var(--ink-2);font-size:14px}
.mono{font-family:var(--mono);font-size:12.5px;
font-variant-numeric:tabular-nums;word-break:break-word}
ul.subs>li.mono{font-size:12px}
.sev{font-family:var(--mono);font-size:10.5px;font-weight:600;letter-spacing:.07em;
text-transform:uppercase;padding:2px 7px;border-radius:3px;border:1px solid;
flex:0 0 auto;position:relative;top:-1px}
.sev-critical{color:var(--crit);background:var(--crit-soft);border-color:var(--crit)}
.sev-warning{color:var(--warn);background:var(--warn-soft);border-color:var(--warn)}
.sev-info{color:var(--ink-2);background:var(--surface-2);border-color:var(--line-strong)}
pre.raw{margin:0 0 12px;padding:12px 14px;background:var(--surface-2);
border:1px solid var(--line);border-radius:4px;font-family:var(--mono);
font-size:12.5px;line-height:1.6;white-space:pre-wrap;overflow-wrap:break-word;
overflow-x:auto}
details.source{margin-top:44px;border-top:1px solid var(--line);padding-top:18px}
details.source summary{cursor:pointer;font-family:var(--mono);font-size:12px;
letter-spacing:.06em;text-transform:uppercase;color:var(--ink-3)}
details.source summary:hover{color:var(--ink)}
details.source pre{margin-top:14px;padding:16px;background:var(--surface-2);
border:1px solid var(--line);border-radius:4px;font-family:var(--mono);
font-size:12.5px;line-height:1.65;white-space:pre-wrap;overflow-wrap:break-word}
a:focus-visible,summary:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
@media print{body{background:#fff}.toc{break-inside:avoid}
section{break-inside:avoid}details.source{display:none}}
"""

_DOCUMENT = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClusterGuard 진단 리포트 {stamp}</title>
<style>{css}</style>
</head>
<body>
<div class="wrap">
<header>
  <p class="eyebrow">Elasticsearch 클러스터 진단</p>
  <h1>ClusterGuard 진단 리포트</h1>
  <p class="stamp">생성 {stamp} KST</p>
</header>
{banner}
{toc}
{body}
<details class="source">
  <summary>리포트 평문 (관측값 + 모델 판단)</summary>
  <pre>{source}</pre>
</details>
</div>
</body>
</html>
"""
