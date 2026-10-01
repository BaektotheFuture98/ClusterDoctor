"""Publish standalone operator reports with escaped source logs and a text fallback.

No external assets are required. Publication failures retain the plain-text
report in the application log. Layout and source escaping live in report_layout.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.file.report_publisher import (
    ReportPublisher,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
    scrub,
)

_logger = logging.getLogger(__name__)

_KST = timezone(timedelta(hours=9))

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
        report: IncidentAnalysisReport,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> ReportPublication:
        # HTML 문자열은 report_layout.esc에서 이스케이프·인코딩 정리한다.
        # 평문 로그 폴백에는 scrub을 별도로 적용한다.
        gaps = tuple(scrub(gap) for gap in gaps)

        # 파일 쓰기는 짧지만 이벤트 루프에서 하지 않는다. 같은 루프가 Kafka를
        # 계속 소비하고 있고, 리포트는 수십 KB까지 자란다.
        try:
            text_length = len(render_text(report))
            path = await asyncio.to_thread(self._write, report, gaps, analysis_failed)
        except Exception as exc:  # noqa: BLE001
            # 파일·렌더링·인코딩 오류가 publication 밖으로 새어 나가지 않게 하고
            # 확보한 진단을 잃지 않도록 평문 로그로 대체한다.
            _logger.error("리포트 HTML 저장 실패(%s) — 전문을 로그로 남긴다", exc)
            try:
                _logger.info("\n%s", scrub(render_text(report)))
            except Exception:
                # 렌더링 자체가 실패한 경우다. 그때도 이 폴백이 죽으면 안 된다.
                _logger.exception("리포트 평문 렌더링도 실패했다")
            return ReportPublication(text_length=locals().get("text_length", 0))

        _logger.info("리포트 저장: %s", path)
        return ReportPublication(text_length=text_length)

    def _write(
        self,
        report: IncidentAnalysisReport,
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


def render_report(
    report: IncidentAnalysisReport,
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
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import (
        render_layout,
    )

    return render_layout(
        report,
        generated_at or datetime.now(_KST),
        gaps=gaps,
        analysis_failed=analysis_failed,
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
section{margin-top:38px}
h2{margin:0 0 14px;font-size:19px;letter-spacing:-.01em;line-height:1.3;
display:flex;gap:11px;align-items:baseline;padding-bottom:9px;
border-bottom:1px solid var(--line);text-wrap:balance}
h2 .n{font-family:var(--mono);font-size:14px;color:var(--accent);
font-variant-numeric:tabular-nums}
p{margin:0 0 11px;max-width:74ch}
.hint{color:var(--ink-2);font-size:13.5px}
.sev{font-family:var(--mono);font-size:10.5px;font-weight:600;letter-spacing:.07em;
text-transform:uppercase;padding:2px 7px;border-radius:3px;border:1px solid;
flex:0 0 auto;position:relative;top:-1px}
.sev-critical{color:var(--crit);background:var(--crit-soft);border-color:var(--crit)}
.sev-warning{color:var(--warn);background:var(--warn-soft);border-color:var(--warn)}
.sev-info{color:var(--ink-2);background:var(--surface-2);border-color:var(--line-strong)}
.incident-timeline{position:relative;display:grid;gap:16px;padding-left:28px}
.incident-timeline::before{content:"";position:absolute;left:8px;top:8px;bottom:8px;
width:2px;background:var(--line-strong)}
.timeline-card{position:relative;background:var(--surface);border:1px solid var(--line);
border-left:3px solid var(--line-strong);border-radius:6px;padding:15px 17px 16px;
break-inside:avoid;page-break-inside:avoid}
.timeline-card::before{content:"";position:absolute;left:-26px;top:21px;width:10px;
height:10px;border-radius:50%;background:var(--surface);border:3px solid var(--line-strong)}
.timeline-card-critical{border-left-color:var(--crit)}
.timeline-card-critical::before{border-color:var(--crit)}
.timeline-card-warning{border-left-color:var(--warn)}
.timeline-card-warning::before{border-color:var(--warn)}
.timeline-card-head{display:flex;align-items:center;gap:10px;margin-bottom:5px}
.timeline-card-head time{font-family:var(--mono);font-size:13px;font-weight:600;
font-variant-numeric:tabular-nums;color:var(--ink-2)}
.timeline-card h3{margin:0 0 13px;font-size:16px;line-height:1.45;letter-spacing:-.01em}
.timeline-card dl{margin:0;display:grid;grid-template-columns:minmax(92px,auto) 1fr;
gap:9px 15px}
.timeline-card dt{font-size:12px;font-weight:700;color:var(--ink-2);white-space:nowrap}
.timeline-card dd{margin:0;min-width:0}
.timeline-card dd ul{margin:0;padding:0;list-style:none;display:grid;gap:7px}
.timeline-card dd li{display:flex;flex-wrap:wrap;gap:5px 8px;font-size:14px}
.timeline-card dd li::before{content:"";width:4px;height:4px;margin-top:.75em;
border-radius:50%;background:var(--line-strong);flex:0 0 auto}
.timeline-card dd li>span:first-of-type{flex:1 1 34ch;min-width:0}
.timeline-refs{font-family:var(--mono);font-size:10.5px;color:var(--ink-3)}
.timeline-observations,.timeline-raw{margin-top:14px;padding-top:11px;
border-top:1px solid var(--line)}
.timeline-observations summary,.timeline-raw summary{cursor:pointer;color:var(--ink-2);
font-family:var(--mono);font-size:11.5px}
.timeline-observations pre.raw,.timeline-raw pre.raw{margin-top:10px;margin-bottom:0}
.timeline-observations-label{margin:0 0 7px;color:var(--ink-2);font-size:11.5px;
font-family:var(--mono)}
.timeline-raw{margin-left:28px}
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
@media (max-width:640px){
.incident-timeline{padding-left:20px}.incident-timeline::before{left:5px}
.timeline-card{padding:13px 14px}.timeline-card::before{left:-21px}
.timeline-card dl{grid-template-columns:1fr;gap:4px}.timeline-card dd{margin-bottom:8px}
.timeline-raw{margin-left:20px}}
@media print{body{background:#fff}.toc{break-inside:avoid}
section{break-inside:auto}.timeline-card{break-inside:avoid;page-break-inside:avoid}
details.source{display:none}}
"""
