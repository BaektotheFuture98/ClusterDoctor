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
.header-top{display:flex;align-items:center;flex-wrap:wrap;gap:8px 12px}
.demo-badge{padding:1px 8px;border:1px solid var(--warn);border-radius:4px;
background:var(--warn-soft);color:var(--warn);font-size:11px;font-weight:700;
letter-spacing:.08em}
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
.alert-card{margin-top:16px;padding:16px 24px;background:var(--warn-soft);
border:1px solid var(--warn);border-left-width:3px;border-radius:8px;font-size:13px}
.alert-card h2{margin:0 0 8px;padding:0;border:0;font-size:16px}
.alert-card h3{margin:16px 0 0;font-size:14px}
.alert-card p{margin:0 0 8px}
.alert-card ul{margin:8px 0 0;padding-left:20px;display:flex;flex-direction:column;gap:4px}
.alert-card details summary{cursor:pointer;font-weight:600}
.report-nav{position:sticky;top:0;z-index:1;display:flex;flex-wrap:wrap;gap:12px 24px;
padding:12px 0;background:var(--page);border-bottom:1px solid var(--line)}
.report-nav a{font-size:13px;text-decoration:none}
section[id]{scroll-margin-top:56px}
.incident-summary{margin-top:24px;padding:24px;background:var(--surface);
border:1px solid var(--line);border-radius:8px}
.incident-summary .headline{margin:0 0 24px;border:0;padding:0 0 0 16px;
border-left:2px solid var(--accent);font-size:clamp(22px,3vw,26px);line-height:1.4}
.model-tag{display:inline-block;margin-left:4px;padding:0 6px;border-radius:4px;
background:var(--accent-soft);color:var(--accent-ink);font-size:11px;font-weight:600}
.summary-field{margin-top:24px}
.summary-field p{margin:0 0 4px}
.field-label{margin:0 0 4px;font-size:12px;font-weight:600;color:var(--ink-2)}
h3.field-label{line-height:1.7}
.key-observations{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 32px;
margin:0}
.key-observations div{display:flex;justify-content:space-between;gap:16px;
padding-bottom:8px;border-bottom:1px solid var(--line)}
.key-observations dt{color:var(--ink-2)}
.key-observations dd{margin:0;font-family:var(--mono);font-size:13px;font-weight:600;
text-align:right;overflow-wrap:anywhere}
.cause-summary p:not(.field-label){padding-left:16px;border-left:2px solid var(--accent)}
.confidence-value{font-weight:700;letter-spacing:.04em}
.sev{font-size:12px;font-weight:600;white-space:nowrap}
.sev::before{content:"";display:inline-block;width:8px;height:8px;margin-right:6px;
border-radius:50%;background:currentColor}
.sev-critical{color:var(--crit)}
.sev-warning{color:var(--warn)}
.sev-info{color:var(--info)}
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
.evidence-list{border-top:1px solid var(--line)}
.evidence-item{border-bottom:1px solid var(--line);scroll-margin-top:56px}
.evidence-item>summary{display:flex;flex-direction:column;gap:2px;padding:12px 0}
.evidence-head{display:flex;align-items:baseline;flex-wrap:wrap;gap:4px 12px;
font-size:12px;color:var(--ink-2)}
.evidence-id{font-family:var(--mono);font-weight:600;color:var(--accent-ink)}
.evidence-head time{font-family:var(--mono);font-variant-numeric:tabular-nums}
.evidence-message{font-size:13px;overflow-wrap:anywhere}
.evidence-fields{margin:0 0 12px;padding:0 0 0 16px;font-size:12px;
border-left:2px solid var(--line-strong)}
.evidence-fields div{display:grid;grid-template-columns:120px minmax(0,1fr);gap:4px 16px;
padding:2px 0}
.evidence-fields dt{color:var(--ink-2)}
.evidence-fields dd{margin:0;font-family:var(--mono);overflow-wrap:anywhere}
.raw-block{margin:0 0 16px 16px}
.truncated{margin-left:8px;padding:0 6px;border:1px solid var(--warn);border-radius:4px;
color:var(--warn);font-size:11px;font-weight:600}
pre.raw{margin:0 0 12px;padding:12px 16px;background:var(--surface-2);
border:1px solid var(--line);border-left:2px solid var(--line-strong);border-radius:4px;
font-family:var(--mono);font-size:12px;line-height:1.6;white-space:pre-wrap;
overflow-wrap:anywhere}
pre.raw code{font:inherit}
pre.raw-query{white-space:pre;overflow-wrap:normal;overflow-x:auto}
.evidence-item summary,.detail-group summary,.raw-block summary{cursor:pointer}
.raw-block summary,.detail-group summary{font-size:12px;color:var(--ink-2);padding:8px 0}
.detail-group{border-bottom:1px solid var(--line)}
.detail-group summary{font-size:13px}
.observation-detail{margin-top:24px;border-top:1px solid var(--line)}
.meta-list{margin:0 0 16px}
.meta-list div{display:grid;grid-template-columns:180px minmax(0,1fr);gap:4px 16px;
padding:8px 0;border-bottom:1px solid var(--line);font-size:13px}
.meta-list dt{color:var(--ink-2)}
.meta-list dd{margin:0;font-family:var(--mono);font-size:12px;color:var(--ink-2);
overflow-wrap:anywhere}
.mono{font-family:var(--mono)}
.cause-assessment{margin-top:16px;padding:24px;background:var(--surface);
border:1px solid var(--line);border-radius:8px}
.cause-assessment h3{margin:0 0 8px;font-size:16px;overflow-wrap:anywhere}
.cause-confidence{display:flex;align-items:center;gap:4px 8px;margin:0 0 16px}
.cause-confidence .field-label{margin:0}
.cause-evidence{margin-top:16px}
.cause-evidence h4{margin:0 0 4px;font-size:13px;font-weight:600;color:var(--ink-2)}
.evidence-lines,.finding-lines{margin:0;padding:0;list-style:none}
.evidence-line{display:grid;grid-template-columns:minmax(80px,max-content) minmax(0,1fr);
gap:4px 16px;padding:4px 0;border-top:1px solid var(--line);font-size:13px}
.evidence-line:first-child{border-top:0}
.evidence-line a{font-family:var(--mono);font-size:12px}
.evidence-line>span:only-child{grid-column:1 / -1}
.evidence-line span{overflow-wrap:anywhere}
.unverified{margin-top:32px}
.unverified h3{margin:0 0 8px;font-size:16px}
.marked-list{margin:0;padding:0;list-style:none}
.marked-list li{display:flex;gap:16px;padding:12px 0;border-bottom:1px solid var(--line)}
.marked-list li>span:last-child{min-width:0;overflow-wrap:anywhere}
.marker{flex:none;min-width:24px;font-family:var(--mono);font-weight:700;color:var(--accent-ink)}
.finding-line{padding:12px 0;border-bottom:1px solid var(--line)}
.finding-line p{margin:0}
.finding-head{display:flex;align-items:baseline;flex-wrap:wrap;gap:4px 12px}
.finding-title{font-weight:600;overflow-wrap:anywhere}
.finding-line .hint{margin-top:4px}
.query-table-scroll{overflow-x:auto;max-width:100%}
.query-ranking-table{min-width:640px;width:100%;border-collapse:collapse;font-size:13px}
.query-ranking-table th,.query-ranking-table td{padding:12px 10px;text-align:left;
border-bottom:1px solid var(--line);vertical-align:top;overflow-wrap:anywhere}
.query-ranking-table th{white-space:nowrap;background:var(--surface-2);font-size:12px}
.query-ranking-table tbody.picked{background:var(--accent-soft)}
.query-ranking-table tbody.picked tr:first-child td{border-bottom:0}
.pick-reason td{padding-top:0}
.query-warning{color:var(--warn);background:var(--warn-soft);padding:8px 16px;border-radius:4px}
summary:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
@media (max-width:700px){
.incident-timeline{padding-left:20px}.incident-timeline::before{left:4px}
.timeline-event{display:block;padding:20px 0}.timeline-event::before{left:-20px;top:28px}
.event-time{margin-bottom:8px}.event-time time{display:inline}
.evidence-fields div,.meta-list div{display:block}
.raw-block{margin-left:0}
.cause-assessment{padding:16px}.evidence-line{display:block}
.key-observations{grid-template-columns:minmax(0,1fr)}.incident-summary{padding:16px}
.report-nav{gap:8px 16px}}
@media print{
body{background:#fff;color:#111}
.wrap{max-width:none;padding:0}
.report-nav{display:none}
.incident-summary,.cause-assessment,.finding-line,.marked-list li,.timeline-event,.evidence-item>summary,.alert-card{break-inside:avoid}
.evidence-item>summary{break-after:avoid}
details::details-content{content-visibility:visible;display:block}
.incident-timeline::before,.timeline-event::before{display:none}
.timeline-event{display:block;padding:16px 0}
.event-time{margin-bottom:8px}
pre.raw{background:#f4f4f4;color:#111}
pre.raw-query{white-space:pre-wrap;overflow-wrap:anywhere;overflow:visible}
.query-ranking-table{min-width:0}.query-table-scroll{overflow:visible}
.query-ranking-table th,.query-ranking-table td{padding:4px 8px;font-size:11px}}
"""
