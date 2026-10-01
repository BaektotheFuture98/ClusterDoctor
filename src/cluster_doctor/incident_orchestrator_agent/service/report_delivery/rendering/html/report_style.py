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
font-size:14px;line-height:1.7;-webkit-text-size-adjust:100%}
.wrap{max-width:1120px;margin-inline:auto;padding-inline:clamp(16px,4vw,32px);
padding-block:32px 64px}
header{display:flex;flex-direction:column;gap:8px;border-bottom:1px solid var(--line);
padding-bottom:24px}
header .hint{margin:0}
.eyebrow{margin:0;font-size:12px;font-weight:600;letter-spacing:.08em;
text-transform:uppercase;color:var(--accent-ink)}
h1{margin:0;font-size:clamp(26px,4vw,30px);font-weight:700;line-height:1.25;
letter-spacing:-.01em;text-wrap:balance}
.stamp{margin:0;font-family:var(--mono);font-size:12px;color:var(--ink-2);
font-variant-numeric:tabular-nums}
.stamp,.headline,h1,.hint,li{overflow-wrap:anywhere}
a{color:var(--accent-ink);text-underline-offset:3px}
section{margin-top:48px}
h2{margin:0 0 16px;font-size:19px;font-weight:700;line-height:1.3;padding-bottom:8px;
border-bottom:1px solid var(--line);text-wrap:balance}
h3{font-weight:600}
p{margin:0 0 12px;max-width:72ch}
.hint{color:var(--ink-2);font-size:13px}
.section-label{margin:0 0 8px;font-size:12px;color:var(--ink-2)}
.banner{margin-top:24px;padding:12px 16px;background:var(--warn-soft);
border:1px solid var(--warn);border-left-width:3px;border-radius:6px;font-size:13px}
.banner ul{margin:8px 0 0;padding-left:20px;display:flex;flex-direction:column;gap:4px}
.report-nav{display:flex;flex-wrap:wrap;gap:12px 24px;padding:16px 0;
border-bottom:1px solid var(--line)}
.report-nav a{font-size:13px;text-decoration:none}
.incident-summary{margin-top:0;padding:24px 0;border-bottom:1px solid var(--line)}
.incident-summary .headline{margin:0;border:0;padding:0;font-size:clamp(22px,3vw,26px);
line-height:1.4}
.summary-status{display:flex;flex-wrap:wrap;gap:8px 24px;align-items:center;margin:16px 0;
font-size:13px;color:var(--ink-2)}
.cause-summary{margin-top:12px}
.next-actions{margin-top:24px}
.next-actions h3{font-size:15px;margin:0 0 8px}
.next-actions ul{margin:0;padding-left:24px}
.sev{font-size:12px;font-weight:600;white-space:nowrap}
.sev::before{content:"";display:inline-block;width:8px;height:8px;margin-right:6px;
border-radius:50%;background:currentColor}
.sev-critical{color:var(--crit)}
.sev-warning{color:var(--warn)}
.sev-info{color:var(--info)}
.incident-timeline{position:relative;padding:0}
.incident-timeline::before{content:"";position:absolute;left:175px;top:24px;bottom:24px;
width:2px;background:var(--line-strong)}
.timeline-card{position:relative;display:grid;grid-template-columns:150px minmax(0,1fr);
gap:48px;padding:24px 0}
.timeline-card::before{content:"";position:absolute;left:170px;top:32px;width:10px;
height:10px;border-radius:50%;background:var(--page);border:3px solid var(--line-strong)}
.timeline-card-critical::before{border-color:var(--crit)}
.timeline-card-warning::before{border-color:var(--warn)}
.event-time{padding-top:3px;font-family:var(--mono);font-size:12px;line-height:1.8;
color:var(--ink-2)}
.event-time time{display:block;font-weight:600;color:var(--ink)}
.event-end{display:block}
.event-body{min-width:0;padding-bottom:24px;border-bottom:1px solid var(--line)}
.event-heading{display:flex;align-items:center;flex-wrap:wrap;gap:8px 12px}
.event-heading h3{flex-basis:100%;margin:0 0 4px;font-size:18px;line-height:1.4}
.node-badge{font-size:12px;color:var(--ink-2);background:var(--surface);
padding:2px 8px;border:1px solid var(--line);border-radius:4px}
.event-observation{margin-top:16px}
.event-observation ul,.event-interpretation ul{margin:8px 0;padding-left:20px}
.event-observation li,.event-interpretation li{font-size:14px;line-height:1.7}
.event-evidence{margin:16px 0}
.event-interpretation{margin-top:16px;padding:2px 0 2px 16px;
border-left:2px solid var(--accent)}
.hypothesis-label{margin:12px 0 0;font-size:12px;color:var(--ink-2)}
.evidence-block{margin:16px 0 24px;scroll-margin-top:24px}
.compact-evidence{margin:8px 0;padding:12px 16px;background:var(--surface);
border:1px solid var(--line);border-radius:6px}
.evidence-title{margin:0 0 4px;font-family:var(--mono);font-size:12px;font-weight:600}
.evidence-meta{font-size:12px;color:var(--ink-2);overflow-wrap:anywhere}
.source-location{margin:4px 0;font-family:var(--mono);font-size:12px;color:var(--ink-2);
overflow-wrap:anywhere}
.raw-label{margin:8px 0 4px;font-size:12px;color:var(--ink-2)}
pre.raw{margin:0 0 12px;padding:12px 16px;background:var(--surface-2);
border:1px solid var(--line);border-left:2px solid var(--line-strong);border-radius:4px;
font-family:var(--mono);font-size:12px;line-height:1.6;white-space:pre-wrap;
overflow-wrap:anywhere;word-break:break-word}
pre.raw code{font:inherit}
.compact-evidence pre.raw{margin:8px 0}
.log-remainder{margin-top:-4px}
.log-remainder summary,.detail-group summary,.source-details summary,
.timeline-observations summary,details.source summary{cursor:pointer;font-size:12px;
color:var(--ink-2);padding:8px 0}
.source-details .evidence-meta{padding:8px 0}
.detail-group{border-bottom:1px solid var(--line)}
.detail-group summary{font-size:13px}
.cause-assessment{padding:16px 0;border-bottom:1px solid var(--line)}
.cause-assessment h3{margin:0 0 8px;font-size:16px}
.cause-assessment p{overflow-wrap:anywhere}
details.source{margin-top:48px;border-top:1px solid var(--line);padding-top:16px}
summary:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
@media (max-width:700px){
.incident-timeline{padding-left:20px}.incident-timeline::before{left:4px}
.timeline-card{display:block;padding:20px 0}.timeline-card::before{left:-20px;top:28px}
.event-time{margin-bottom:8px}.event-end{display:inline}
.event-heading h3{font-size:16px}.compact-evidence{padding:8px 12px}
.summary-status{gap:8px 16px}.report-nav{gap:8px 16px}}
@media print{
body{background:#fff;color:#111}
.wrap{max-width:none;padding:0}
.report-nav,details.source{display:none}
.incident-summary,.cause-assessment,.timeline-card,.evidence-block,.banner{break-inside:avoid}
.evidence-title{break-after:avoid}
details::details-content{content-visibility:visible;display:block}
details>summary{display:none}
.incident-timeline::before,.timeline-card::before{display:none}
.timeline-card{display:block;padding:16px 0}
.event-time{margin-bottom:8px}
pre.raw{background:#f4f4f4;color:#111}}
"""
