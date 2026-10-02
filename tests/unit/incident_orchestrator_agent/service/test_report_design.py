from dataclasses import replace
from datetime import datetime, timedelta
from html.parser import HTMLParser
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import TimelineCard
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection import report_content
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp, minute_stamp
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html import report_layout
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_orchestrator_agent.model.incident_report import Narrative
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report, T0


def example():return report('PASSED')


def test_report_is_standalone_with_visible_escaped_ssh_raw():
    html=render_report(example())
    assert '/es/log' in html and 'WARN &lt;stack&gt;&amp;' in html
    assert '<script' not in html and '<link ' not in html and 'href="#evidence' not in html
    assert '<details' not in html and 'INTERNAL_ID' not in html


def test_plain_text_keeps_source_and_raw():
    text=render_text(example())
    assert '/es/log' in text and 'WARN <stack>&' in text
    assert text.index('핵심 요약') < text.index('타임라인') < text.index('SSH 노드 로그')


def test_parsed_and_fallback_ssh_times_differ():
    r=example(); e=r.evidence[0]
    html=render_report(replace(r,evidence=(e.model_copy(update={'time_origin':'parsed'}),)))
    section=html.split('id="ssh-logs"',1)[1].split('</section>',1)[0]
    assert '2026-10-01 09:00:00' in section and '시각 미확인' not in section
    assert '시각 미확인' in render_report(r).split('id="ssh-logs"',1)[1]


def test_ssh_full_stack_and_fractional_time_are_preserved():
    r=example();raw='\n'.join(f'frame-{n} <tag>&' for n in range(30))
    e=r.evidence[0].model_copy(update={'message':raw,'event_time':T0+timedelta(microseconds=123456),'time_origin':'parsed'})
    html=render_report(replace(r,evidence=(e,)))
    assert 'frame-29 &lt;tag&gt;&amp;' in html and '.123456' in html
    assert 'frame-29 <tag>&' in render_text(replace(r,evidence=(e,)))


def test_timeline_is_bounded_and_contains_absolute_maximum():
    r=example()
    evidence=tuple(Evidence(evidence_id=f'E{n}',event_time=T0+timedelta(seconds=n+1),source=EvidenceSource.NODE_LOG,severity='Critical',message=f'failure{n}') for n in range(30))
    html=render_report(replace(r,evidence=evidence))
    section=html.split('id="timeline"',1)[1].split('</section>',1)[0]
    assert section.count('<article class="timeline-event')<=8
    assert '최대 실행시간 1.96s' in section
    assert 'E29' not in html


def test_verified_headline_and_source_text_are_escaped():
    r=replace(example(),narrative=Narrative(headline='<script>alert(1)</script> & claim'))
    html=render_report(r)
    assert '&lt;script&gt;alert(1)&lt;/script&gt; &amp; claim' in html and '<script>' not in html


def test_unique_section_ids():
    class Ids(HTMLParser):
        def __init__(self):super().__init__();self.ids=[]
        def handle_starttag(self,tag,attrs):
            a=dict(attrs)
            if 'id' in a:self.ids.append(a['id'])
    p=Ids();p.feed(render_report(example()))
    assert len(p.ids)==len(set(p.ids))


def test_print_css_keeps_svg_raw_and_headers():
    html=render_report(example())
    assert '@media print' in html and 'svg{display:block!important}' in html
    assert 'thead{display:table-header-group}' in html and 'break-inside:avoid' in html
    assert 'white-space:pre-wrap' in html and 'overflow-wrap:anywhere' in html


def test_timeline_markup_matches_dot_and_line_css(monkeypatch):
    cards=(
        TimelineCard(start=T0,end=T0+timedelta(minutes=2),severity='Critical',representative_event='crit'),
        TimelineCard(start=T0+timedelta(minutes=5),end=T0+timedelta(minutes=5),severity='Warning',representative_event='warn'),
        TimelineCard(start=T0+timedelta(minutes=9),end=T0+timedelta(minutes=9),severity='Info',representative_event='info'),
    )
    monkeypatch.setattr(report_layout,'report_timeline',lambda _report:cards)
    section=render_report(example()).split('id="timeline"',1)[1].split('</section>',1)[0]
    assert '<div class="incident-timeline">' in section
    articles=section.split('<article')[1:]
    assert len(articles)==3
    assert 'class="timeline-event timeline-event-critical"' in articles[0]
    assert 'class="timeline-event timeline-event-warning"' in articles[1]
    assert 'class="timeline-event"' in articles[2] and 'timeline-event-' not in articles[2]
    for card,article in zip(cards,articles):
        assert f'<div class="event-time"><time datetime="{card.start.isoformat()}">{minute_stamp(card.start)}</time>' in article
    assert f'<time datetime="{cards[0].end.isoformat()}">마지막 관측 {minute_stamp(cards[0].end)}</time>' in articles[0]
    assert '마지막 관측' not in articles[1]+articles[2]
    assert '<br>마지막 관측' not in section


def test_timeline_times_show_minute_precision_in_html_and_text(monkeypatch):
    start=datetime.fromisoformat('2026-10-02T09:28:53.123456+09:00')
    card=TimelineCard(start=start,end=start,severity='Info',representative_event='evt')
    monkeypatch.setattr(report_layout,'report_timeline',lambda _report:(card,))
    monkeypatch.setattr(report_content,'report_timeline',lambda _report:(card,))
    section=render_report(example()).split('id="timeline"',1)[1].split('</section>',1)[0]
    assert '<div class="event-time"><time datetime="2026-10-02T09:28:53.123456+09:00">2026-10-02 09:28 KST</time>' in section
    visible=section.replace('2026-10-02T09:28:53.123456+09:00','')
    assert '09:28:53' not in visible and '.123456' not in visible
    text=render_text(example())
    assert '2026-10-02 09:28 KST · evt' in text
    assert '09:28:53' not in text.split('주요 타임라인',1)[1].split('evt',1)[0]
