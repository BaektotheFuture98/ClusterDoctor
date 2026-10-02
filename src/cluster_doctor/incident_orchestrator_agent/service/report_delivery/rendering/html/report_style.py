"""Single stylesheet for the HTML incident report."""

REPORT_CSS = """
:root{color-scheme:light dark;
--page:#F5F6F8;--surface:#FFFFFF;--surface-2:#F8F9FB;
--ink:#151A21;--ink-2:#59636F;--ink-3:#7A8491;
--line:#E1E5EA;--line-strong:#C8CFD8;
--accent:#2F47B5;--accent-soft:#E9EDFB;--accent-ink:#253A91;
--crit:#B42318;--crit-soft:#FEE4E2;--warn:#B54708;--warn-soft:#FEF0C7;--info:#667085;
--sans:"Pretendard","Apple SD Gothic Neo","Malgun Gothic",system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
--mono:"D2Coding","Cascadia Mono","SFMono-Regular",Consolas,"Liberation Mono",monospace}
@media (prefers-color-scheme:dark){:root{
--page:#0F1217;--surface:#171B22;--surface-2:#1D222B;
--ink:#E8EBF0;--ink-2:#9BA5B3;--ink-3:#78828F;
--line:#272D38;--line-strong:#3A4250;
--accent:#8FA3FF;--accent-soft:#1C2340;--accent-ink:#B3C0FF;
--crit:#F0857B;--crit-soft:#2F1917;--warn:#DFA93E;--warn-soft:#2C2313;--info:#9BA5B3}}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);font-family:var(--sans);
font-size:14px;line-height:1.7;word-break:keep-all;-webkit-text-size-adjust:100%}
.wrap{max-width:1120px;margin-inline:auto;padding-inline:clamp(16px,4vw,32px);
padding-block:32px 64px}
header{display:flex;flex-direction:column;gap:8px;border-bottom:1px solid var(--line);
padding-bottom:24px}
header .hint{margin:0}
h1{margin:0;font-size:clamp(26px,4vw,30px);font-weight:700;line-height:1.25;
letter-spacing:-.01em;text-wrap:balance}
.stamp{margin:0;font-family:var(--mono);font-size:12px;color:var(--ink-2);
font-variant-numeric:tabular-nums}
.stamp,.headline,h1,.hint,li,p{overflow-wrap:anywhere}
a{color:var(--accent-ink);text-underline-offset:3px}
section{margin-top:48px}
h2{margin:0 0 16px;font-size:19px;font-weight:700;line-height:1.3;padding-bottom:8px;
border-bottom:1px solid var(--line);text-wrap:balance}
h3{font-weight:600}
p{margin:0 0 12px;max-width:72ch}
.hint{color:var(--ink-2);font-size:13px}
section[id]{scroll-margin-top:56px}
.incident-timeline{position:relative;padding:0}
.incident-timeline::before{content:"";position:absolute;left:175px;top:24px;bottom:24px;
width:2px;background:var(--line-strong)}
.timeline-event{position:relative;display:grid;grid-template-columns:150px minmax(0,1fr);
gap:48px;padding:24px 0}
.timeline-event::before{content:"";position:absolute;left:170px;top:32px;width:10px;
height:10px;border-radius:50%;background:var(--page);border:3px solid var(--line-strong)}
.timeline-event-critical::before{border-color:var(--crit)}
.timeline-event-warning::before{border-color:var(--warn)}
.event-time{padding-top:3px;font-family:var(--mono);font-size:12px;line-height:1.8;
color:var(--ink-2)}
.event-time time{display:block}
.event-time time:first-child{font-weight:600;color:var(--ink)}
.event-body{min-width:0;padding-bottom:24px;border-bottom:1px solid var(--line)}
.event-meta{display:flex;align-items:center;flex-wrap:wrap;gap:4px 12px;margin:0 0 4px}
.event-node{font-size:12px;color:var(--ink-2)}
.event-body h3{margin:0 0 8px;font-size:16px;line-height:1.5}
.event-observation{margin:0 0 8px}
.analysis-note{margin:0 0 8px;padding:2px 0 2px 16px;border-left:2px solid var(--accent);
font-size:13px}
.event-refs{display:flex;flex-wrap:wrap;gap:4px 12px;margin:0;font-family:var(--mono);
font-size:12px}
summary:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
@media (max-width:700px){
.incident-timeline{padding-left:20px}.incident-timeline::before{left:4px}
.timeline-event{display:block;padding:20px 0}.timeline-event::before{left:-20px;top:28px}
.event-time{margin-bottom:8px}.event-time time{display:inline}}
@media print{
:root{color-scheme:light;--page:#fff;--surface:#fff;--surface-2:#f4f4f4;
--ink:#111;--ink-2:#444;--ink-3:#555;--line:#ddd;--line-strong:#bbb;
--accent:#2F47B5;--accent-soft:#E9EDFB;--accent-ink:#253A91;
--crit:#B42318;--crit-soft:#FEE4E2;--warn:#B54708;--warn-soft:#FEF0C7;--info:#667085}
body{background:#fff;color:#111}
.wrap{max-width:none;padding:0}
.timeline-event{break-inside:avoid}
details::details-content{content-visibility:visible;display:block}
.incident-timeline::before,.timeline-event::before{display:none}
.timeline-event{display:block;padding:16px 0}
.event-time{margin-bottom:8px}}
"""
