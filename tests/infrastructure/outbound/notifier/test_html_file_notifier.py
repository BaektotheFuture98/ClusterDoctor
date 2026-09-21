"""HtmlFileNotifier 검증.

리포트는 LLM이 쓴 평문이고 그 안에 외부 사용자의 검색어와 ES 쿼리 원문이
그대로 인용된다. 그래서 이 어댑터에서 가장 중요한 성질은 두 가지다 —
이스케이프가 새지 않는 것, 그리고 파싱이 어긋나도 원문을 잃지 않는 것.
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path


from cluster_doctor.domain.model.diagnosis_report import (
    DiagnosisReport,
    Observations,
    TimelineRow,
)
from cluster_doctor.infrastructure.outbound.notifier.html_file_notifier import (
    HtmlFileNotifier,
    _unique_path,
    render_report,
)

_KST = timezone(timedelta(hours=9))
_AT = datetime(2026, 9, 10, 2, 5, 56, tzinfo=_KST)


def _plain(text: str, observations: Observations | None = None) -> DiagnosisReport:
    """모델이 평문만 남긴 리포트.

    구조화 출력이 실패했을 때의 모양이고, 이 파일의 검증 대상인 정규식
    파서가 도는 것도 그때다. 관측값을 주지 않으면 판단 섹션만 남는다.
    """
    return DiagnosisReport(
        observations=observations or Observations(),
        narrative=None,
        narrative_text=text,
    )

_REPORT = """1. 인시던트 개요
──────────────────────────────
• 유입 관찰: 02:03:58 ~ 02:04:44, 총 대기 70초, 대기 상한 미도달
• 분석 구간: 2026-09-10T02:02:00 ~ 2026-09-10T02:06:00

2. 발견된 문제점
──────────────────────────────
• Critical: es-data-02가 cpu=94%, search queue=920으로 포화
  - 같은 노드에서 search_rejected=37이 관찰됐다
  - 02:04 구간에만 나타났다
• Warning: wildcard 쿼리가 analyzed 필드를 대상으로 실행됐다
• Info: 클러스터 상태는 green을 유지했다

3. 문제 쿼리 후보
──────────────────────────────
{"query":{"wildcard":{"title":{"value":"*<보고서>*"}}},"size":1000}
"""


class TestFileOutput:
    async def test_지정_디렉터리에_파일을_만든다(self, tmp_path: Path):
        notifier = HtmlFileNotifier(output_dir=tmp_path / "reports")

        await notifier.notify(_plain(_REPORT))

        files = list((tmp_path / "reports").glob("report-*.html"))
        assert len(files) == 1
        assert files[0].read_text(encoding="utf-8").startswith("<!doctype html>")

    async def test_디렉터리가_없으면_만든다(self, tmp_path: Path):
        target = tmp_path / "a" / "b" / "reports"
        assert not target.exists()

        await HtmlFileNotifier(output_dir=target).notify(_plain(_REPORT))

        assert target.is_dir()

    def test_같은_초에_저장되면_접미사가_붙는다(self, tmp_path: Path):
        """시각을 고정해 검증한다. notify 두 번이 같은 초에 떨어지는지는
        벽시계에 달려 있어, 그것으로 검증하면 초 경계에서 깨진다."""
        first = _unique_path(tmp_path, _AT)
        first.write_text("이미 있는 파일", encoding="utf-8")

        second = _unique_path(tmp_path, _AT)

        assert first.name == "report-20260910-020556.html"
        assert second.name == "report-20260910-020556-2.html"

    async def test_두_건을_보내면_둘_다_남는다(self, tmp_path: Path):
        notifier = HtmlFileNotifier(output_dir=tmp_path)

        await notifier.notify(_plain("1. 첫 번째\n• 내용"))
        await notifier.notify(_plain("1. 두 번째\n• 내용"))

        bodies = [
            p.read_text(encoding="utf-8") for p in tmp_path.glob("report-*.html")
        ]
        assert len(bodies) == 2
        assert any("첫 번째" in b for b in bodies)
        assert any("두 번째" in b for b in bodies)

    async def test_파일명에_콜론을_쓰지_않는다(self, tmp_path: Path):
        """Windows에서 쓸 수 없는 문자다. 저장 자체가 실패한다."""
        await HtmlFileNotifier(output_dir=tmp_path).notify(_plain(_REPORT))

        assert ":" not in next(tmp_path.glob("report-*.html")).name

    async def test_저장_실패는_예외가_아니라_로그_폴백이다(
        self, tmp_path: Path, caplog
    ):
        """notify가 예외를 올리면 _run_agent의 succeeded가 False로 남아
        리포트가 사라지고 재트리거까지 막힌다. 진단은 이미 성공했으므로
        전문을 로그로 남기고 조용히 끝낸다."""
        blocked = tmp_path / "reports"
        blocked.write_text("여기 파일이 있어서 디렉터리를 만들 수 없다", encoding="utf-8")

        with caplog.at_level(logging.INFO):
            await HtmlFileNotifier(output_dir=blocked).notify(_plain(_REPORT))

        assert "리포트 HTML 저장 실패" in caplog.text
        # 전문이 로그에 남아야 한다 — 이것이 유일한 사본이다.
        assert "es-data-02" in caplog.text

    async def test_성공하면_저장_경로를_로그로_남긴다(self, tmp_path: Path, caplog):
        with caplog.at_level(logging.INFO):
            await HtmlFileNotifier(output_dir=tmp_path).notify(_plain(_REPORT))

        assert "리포트 저장" in caplog.text


class TestEscaping:
    def test_마크업이_주입되지_않는다(self):
        html_out = render_report(_plain('1. 개요\n• <script>alert("x")</script>'), _AT)

        assert "<script>alert" not in html_out
        assert "&lt;script&gt;" in html_out

    def test_쿼리_원문의_꺾쇠와_앰퍼샌드가_이스케이프된다(self):
        html_out = render_report(_plain('1. 쿼리\n{"q":"a<b & c>d"}'), _AT)

        assert "a&lt;b &amp; c&gt;d" in html_out

    def test_원문_블록도_이스케이프된다(self):
        """details 안의 원문은 사람이 읽는 사본이지만 같은 텍스트다."""
        html_out = render_report(_plain("1. 개요\n• <img src=x onerror=1>"), _AT)

        assert "<img src=x" not in html_out


class TestStructure:
    def test_섹션_제목이_h2와_목차로_들어간다(self):
        html_out = render_report(_plain(_REPORT), _AT)

        assert "발견된 문제점" in html_out
        assert 'id="sec-2"' in html_out
        assert 'href="#sec-2"' in html_out

    def test_불렛과_세부_항목이_중첩된다(self):
        html_out = render_report(_plain(_REPORT), _AT)

        assert 'class="bullets"' in html_out
        assert 'class="subs"' in html_out
        assert "search_rejected=37" in html_out

    def test_심각도가_배지로_분리된다(self):
        html_out = render_report(_plain(_REPORT), _AT)
        body = html_out.split("<details")[0]

        assert 'class="sev sev-critical">Critical<' in body
        assert 'class="sev sev-warning">Warning<' in body
        # 배지로 뽑은 뒤 본문에서는 접두어가 사라진다. 원문 블록(details)에는
        # 당연히 남아 있으므로 본문만 본다.
        assert "Critical: es-data-02" not in body
        assert "es-data-02" in body

    def test_구분선은_버린다(self):
        html_out = render_report(_plain(_REPORT), _AT)

        assert "──────" not in html_out.split("<details")[0]

    def test_여러_줄로_이어진_불렛은_한_항목으로_합친다(self):
        """모델은 긴 불렛을 들여쓴 다음 줄로 이어 쓴다. 떼어 놓으면 한 문장이
        불렛과 문단으로 쪼개져 읽는 순서가 무너진다."""
        html_out = render_report(
            _plain(
                "1. 근본 원인\n"
                "• 단일 계정의 wildcard 쿼리가 검색 큐를 포화시켰고, 그 결과\n"
                "  샤드 재배치가 유발됐다.\n"
            ),
            _AT,
        )
        body = html_out.split("<details")[0]

        assert "포화시켰고, 그 결과 샤드 재배치가 유발됐다." in body
        assert "<p>샤드 재배치가" not in body

    def test_들여쓴_세부_항목도_이어붙인다(self):
        html_out = render_report(
            _plain(
                "1. 개요\n• 상위 항목\n  - 세부 내용이 길어서\n    다음 줄로 이어진다\n"
            ),
            _AT,
        )
        body = html_out.split("<details")[0]

        assert "세부 내용이 길어서 다음 줄로 이어진다" in body

    def test_쿼리_원문은_연속으로_삼키지_않는다(self):
        """들여쓴 JSON은 앞 불렛의 연속이 아니라 인용이다."""
        html_out = render_report(
            _plain('1. 쿼리\n• 문제 쿼리다\n  {"query":{"match_all":{}}}\n'), _AT
        )
        body = html_out.split("<details")[0]

        assert 'class="raw"' in body
        assert "문제 쿼리다 {" not in body

    def test_쿼리_원문은_등폭_블록으로_그린다(self):
        html_out = render_report(_plain(_REPORT), _AT)

        assert 'class="raw"' in html_out

    def test_원문을_항상_함께_싣는다(self):
        """파싱이 어긋나도 리포트가 손실되지 않게 하는 안전장치다."""
        html_out = render_report(_plain(_REPORT), _AT)

        assert "리포트 평문" in html_out
        assert html_out.count("es-data-02") >= 2  # 본문 + 원문

    def test_섹션_형식이_아니면_구조를_만들어내지_않는다(self):
        """형식을 어긴 평문은 쪼개지 않고 통째로 싣는다.

        관측값이 하나도 없으므로 이 리포트에 남는 것은 모델 평문뿐이고,
        그것마저 섹션 형식이 아니면 구조를 지어내지 않는다.
        """
        html_out = render_report(_plain("특이사항 없음. 분석할 로그가 없었다."), _AT)

        assert "모델 리포트 (평문)" in html_out
        assert 'class="bullets"' not in html_out
        assert "특이사항 없음" in html_out

    def test_분석_실패_구간이_있으면_배너를_띄운다(self):
        """판정 근거가 본문 문자열이 아니라 관측값이다.

        본문에 "분석 실패"라는 말이 있는지로 판정하면, 모델이 그 말을 옮겨
        적어 줘야만 성립하고 반대로 본문이 그 말을 우연히 담으면 멀쩡한
        리포트에 배너가 붙는다.
        """
        failed_minute = TimelineRow(
            minute=datetime(2026, 9, 10, 2, 4, tzinfo=_KST),
            counts={"es_query_log": 12},
            failed=True,
        )
        html_out = render_report(
            _plain("1. 개요\n• 내용", Observations(timeline=(failed_minute,))),
            _AT,
        )

        assert 'class="banner"' in html_out
        assert "분석하지 못한 구간이 있다" in html_out

    def test_실패한_분이_있으면_gaps와_함께_배너가_둘_다_뜬다(self):
        """프로덕션에서 실제로 나오는 조합이다.

        ``row.failed``가 참이면 ``analyze_logs``의 ``_mark_gap``이 반드시
        gaps에도 남긴다. 그래서 이 배너를 ``elif``로 두면 **한 번도 뜨지
        않는다** — 위 테스트는 gaps를 비운 채로 통과시켜 도달 불가능한 경로에
        거짓 확신을 주고 있었다.

        두 배너는 다른 말을 한다. 위는 "무엇이 빠졌는가", 아래는 "어느 구간을
        믿을 수 없는가"다.
        """
        failed_minute = TimelineRow(
            minute=datetime(2026, 9, 10, 2, 4, tzinfo=_KST),
            counts={"es_query_log": 12},
            failed=True,
        )
        html_out = render_report(
            _plain("1. 개요\n• 내용", Observations(timeline=(failed_minute,))),
            _AT,
            gaps=("02:00 ~ 02:10 구간 중 1개 분의 분석이 실패했다(전체 3개 분).",),
        )

        assert html_out.count('class="banner"') == 2
        assert "수집하지 못한 근거가 있다" in html_out
        assert "분석하지 못한 구간이 있다" in html_out

    def test_본문에_분석_실패라는_말이_있어도_배너를_띄우지_않는다(self):
        """관측값이 온전하면 본문의 표현이 배너를 만들지 못한다."""
        ok_minute = TimelineRow(
            minute=datetime(2026, 9, 10, 2, 4, tzinfo=_KST),
            counts={"es_query_log": 12},
        )
        html_out = render_report(
            _plain(
                "1. 개요\n• 지난 진단에서는 [분석 실패]가 있었다",
                Observations(timeline=(ok_minute,)),
            ),
            _AT,
        )

        assert 'class="banner"' not in html_out

    def test_정상_리포트에는_배너가_없다(self):
        assert 'class="banner"' not in render_report(_plain(_REPORT), _AT)

    def test_완결된_HTML_문서다(self):
        html_out = render_report(_plain(_REPORT), _AT)

        assert html_out.startswith("<!doctype html>")
        assert html_out.rstrip().endswith("</html>")
        assert '<meta charset="utf-8">' in html_out
        # 폐쇄망에서 열리는 파일이므로 외부 리소스를 참조하지 않는다.
        assert "http://" not in html_out and "https://" not in html_out

    def test_생성_시각을_KST로_찍는다(self):
        html_out = render_report(_plain(_REPORT), _AT)

        assert "2026-09-10 02:05:56" in html_out
        assert "KST" in html_out


class TestSubItemsWithoutABullet:
    """리포트 형식의 섹션 1은 불렛 없이 "- 세부"로 시작한다.

    SYSTEM_PROMPT가 지시하는 형식이 그렇다. 불렛이 있을 때만 세부로 붙이면
    그 줄들이 문단으로 떨어져 대시가 본문에 그대로 남는다.
    """

    def test_불렛_없이_시작하는_세부_줄도_목록이_된다(self):
        html_out = render_report(
            _plain(
                "1. 인시던트 개요\n"
                "   - 유입 관찰: 02:03:58 ~ 02:04:44\n"
                "   - 분석 구간: 02:02:00 ~ 02:06:00\n"
            ),
            _AT,
        )
        body = html_out.split("<details")[0]

        assert 'class="bullets"' in body
        assert "<p>- " not in body
        assert "유입 관찰" in body

    def test_승격된_불렛도_심각도_배지를_뗀다(self):
        html_out = render_report(_plain("1. 문제점\n   - Critical: 노드 포화\n"), _AT)
        body = html_out.split("<details")[0]

        assert 'class="sev sev-critical">Critical<' in body
        assert "Critical: 노드 포화" not in body

    def test_불렛이_있으면_기존처럼_중첩된다(self):
        html_out = render_report(
            _plain("1. 문제점\n• 상위 항목\n  - 하위 세부\n"), _AT
        )
        body = html_out.split("<details")[0]

        assert 'class="subs"' in body


class TestFailureGuarantee:
    async def test_인코딩할_수_없는_문자는_치환하고_저장한다(self, tmp_path, caplog):
        """짝 없는 서로게이트가 섞여도 리포트를 잃지 않는다.

        이 문자를 그대로 두면 두 경로가 함께 무너진다 — write_text가
        UnicodeEncodeError를 내고, 폴백으로 전문을 로그에 남기려 해도 파일
        핸들러가 같은 이유로 실패한다. 그래서 입력 시점에 치환한다.
        """
        report = "1. 개요\n• 문제 노드: \ud800 포화"

        with caplog.at_level(logging.INFO):
            await HtmlFileNotifier(output_dir=tmp_path).notify(_plain(report))

        files = list(tmp_path.glob("report-*.html"))
        assert len(files) == 1
        body = files[0].read_text(encoding="utf-8")
        assert "문제 노드" in body
        assert "포화" in body
        assert "리포트 HTML 저장 실패" not in caplog.text

    def test_치환은_정상_문자를_건드리지_않는다(self):
        html_out = render_report(_plain(_REPORT), _AT)

        assert "es-data-02" in html_out
        assert "•" not in html_out.split("<details")[0]  # 불렛은 구조로 바뀐다

    async def test_그_외_저장_실패는_여전히_전문을_로그로_남긴다(
        self, tmp_path, caplog
    ):
        blocked = tmp_path / "reports"
        blocked.write_text("여기 파일이 있어 디렉터리를 만들 수 없다", encoding="utf-8")

        with caplog.at_level(logging.INFO):
            await HtmlFileNotifier(output_dir=blocked).notify(_plain(_REPORT))

        assert "리포트 HTML 저장 실패" in caplog.text
        assert "es-data-02" in caplog.text


class TestGapAndFailureBanners:
    """누락과 실패를 배너로 그린다.

    본문에 섞지 않는 이유는 두 가지다 — 모델이 쓴 것과 시스템이 덧붙인 것이
    구별되어야 하고, 모델이 프롬프트를 어겨 누락을 밝히지 않았더라도 이
    배너는 반드시 남는다.
    """

    def test_분석_실패는_붉은_배너로_경고한다(self):
        html_out = render_report(_plain(_REPORT), _AT, analysis_failed=True)

        assert "banner-fail" in html_out
        assert "결론을 신뢰할 수 없다" in html_out
        assert "재트리거는 걸리지 않는다" in html_out

    def test_누락된_근거를_목록으로_밝힌다(self):
        html_out = render_report(
            _plain(_REPORT),
            _AT,
            gaps=("es-data-02 노드 로그 SSH 수집 실패", "분석 호출 상한 도달"),
        )

        assert "수집하지 못한 근거가 있다" in html_out
        assert "es-data-02 노드 로그 SSH 수집 실패" in html_out
        assert "분석 호출 상한 도달" in html_out

    def test_누락_항목도_이스케이프된다(self):
        html_out = render_report(_plain(_REPORT), _AT, gaps=("<script>x</script>",))

        assert "<script>x" not in html_out
        assert "&lt;script&gt;" in html_out

    def test_둘_다_있으면_배너가_둘_다_나온다(self):
        html_out = render_report(
            _plain(_REPORT), _AT, gaps=("노드 로그 실패",), analysis_failed=True
        )

        assert "banner-fail" in html_out
        assert "수집하지 못한 근거가 있다" in html_out

    def test_아무_문제가_없으면_배너가_없다(self):
        assert 'class="banner' not in render_report(_plain(_REPORT), _AT)

    async def test_notify가_두_값을_리포트까지_전달한다(self, tmp_path):
        await HtmlFileNotifier(output_dir=tmp_path).notify(
            _plain(_REPORT),
            gaps=("es-data-02 노드 로그 SSH 수집 실패",),
            analysis_failed=True,
        )

        body = next(tmp_path.glob("report-*.html")).read_text(encoding="utf-8")
        assert "banner-fail" in body
        assert "es-data-02 노드 로그 SSH 수집 실패" in body


class TestObservationsAreNotFabricated:
    """빈 칸이 그럴듯한 값으로 채워지지 않는지.

    이 저장소가 두 번 당한 실패다 — es_query_log 264건이 slowlog 건수로 실렸고,
    모델이 severity를 채우지 않자 기본값 "Info"가 25초 지연과 노드 19대
    타임아웃에 붙었다. 둘 다 "비었다"가 "값이 있다"로 보이는 형태였다.
    """

    def test_분류하지_않은_문제에는_배지를_붙이지_않는다(self):
        from cluster_doctor.domain.model.diagnosis_report import Finding, Narrative

        report = _plain("")
        report = DiagnosisReport(
            observations=Observations(),
            narrative=Narrative(
                findings=(
                    Finding(severity="", title="분류하지 않은 문제"),
                    Finding(severity="Critical", title="분류한 문제"),
                )
            ),
        )
        html_out = render_report(report, _AT)

        assert 'class="sev sev-critical">Critical<' in html_out
        # 빈 severity가 빈 배지로 새어 나오면 안 된다.
        assert 'class="sev sev-">' not in html_out

    def test_상태_이력이_분석_구간_밖이면_그_사실을_밝힌다(self):
        """cluster_health는 ES 실시간 API라 과거 상태를 모른다.

        과거 사고를 분석하면 이 섹션의 시각은 사고 시각이 아니라 진단을 돌린
        시각이다. 실측에서 9/10 15:27 사고 리포트에 "11:40 green"이 실렸고,
        그대로 두면 운영자는 사고 당시가 green이었다고 읽는다.
        """
        from cluster_doctor.domain.model.elasticsearch.health_point import HealthPoint

        window = (
            datetime(2026, 9, 10, 15, 20, tzinfo=_KST),
            datetime(2026, 9, 10, 15, 30, tzinfo=_KST),
        )
        now = datetime(2026, 9, 14, 11, 40, tzinfo=_KST)
        report = DiagnosisReport(
            observations=Observations(
                requested=(window,),
                health=(HealthPoint(at=now, until=now, status="green"),),
            )
        )
        html_out = render_report(report, now)

        assert "과거 클러스터 상태를 보관하지 않는다" in html_out
        assert "분석 구간이 아니다" not in html_out  # 문구가 바뀌면 알아채게

    def test_상태_이력이_분석_구간_안이면_주의를_붙이지_않는다(self):
        from cluster_doctor.domain.model.elasticsearch.health_point import HealthPoint

        window = (
            datetime(2026, 9, 10, 15, 20, tzinfo=_KST),
            datetime(2026, 9, 10, 15, 30, tzinfo=_KST),
        )
        inside = datetime(2026, 9, 10, 15, 25, tzinfo=_KST)
        report = DiagnosisReport(
            observations=Observations(
                requested=(window,),
                health=(HealthPoint(at=inside, until=inside, status="yellow"),),
            )
        )

        assert "과거 클러스터 상태를 보관하지 않는다" not in render_report(report, _AT)
