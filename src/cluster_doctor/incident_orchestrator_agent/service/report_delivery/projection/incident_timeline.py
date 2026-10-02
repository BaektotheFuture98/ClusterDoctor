"""관측값과 검증된 근거를 운영자용 인시던트 카드로 투영한다.

표현 계층은 이 모듈이 만든 카드만 그린다. HTML과 평문이 각자 임계값과
병합 규칙을 구현하면 같은 사고를 서로 다른 심각도로 보여 주게 되므로, 판정은
부작용 없는 이 함수 한 벌에만 둔다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.observations import (
    MasterEvent,
    Observations,
    TimelineRow,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    TimelineAnnotation,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    EvidenceCitation,
    citations,
    cite,
)

_ONE_MINUTE = timedelta(minutes=1)
_SEVERITY_RANK = {"": 0, "Info": 1, "Warning": 2, "Critical": 3}
_ACTION_RE = re.compile(r"action \[(.+?)\],")
_LOGGER_RE = re.compile(r"\blogger=([^\s]+)")


@dataclass(frozen=True)
class TimelineItem:
    text: str
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class TimelineCard:
    start: datetime
    end: datetime
    severity: str
    representative_event: str
    impacts: tuple[TimelineItem, ...] = ()
    causes: tuple[TimelineItem, ...] = ()
    interpretations: tuple[TimelineItem, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    raw_rows: tuple[TimelineRow, ...] = ()
    citations: tuple[str, ...] = ()
    evidence_citations: tuple[EvidenceCitation, ...] = ()


@dataclass(frozen=True)
class _Signal:
    start: datetime
    end: datetime
    severity: str
    category: str
    item: TimelineItem


def project_timeline(
    observations: Observations,
    evidence: tuple[Evidence, ...] = (),
    annotations: tuple[TimelineAnnotation, ...] = (),
    *,
    verification_status: str = "NOT_VERIFIED",
) -> tuple[TimelineCard, ...]:
    """특징적인 사건을 시작 시각별로 투영한다. 반복 신호의 구간은 유지한다."""
    rows = tuple(sorted(observations.timeline, key=lambda row: row.minute))
    evidence = tuple(sorted((item for item in evidence if item.time_origin == "parsed"), key=lambda item: item.event_time))

    signals: list[_Signal] = []
    signals += _slowlog_signals(rows, evidence)
    signals += _rejected_signals(rows, evidence)
    signals += _failed_signals(rows)
    signals += _latency_peak_signals(rows, evidence)
    signals += _volume_spike_signals(rows, evidence)
    signals += _cause_signals(observations, rows, evidence)
    signals += _annotation_signals(
        annotations, evidence, verification_status=verification_status
    )
    signals = _with_followup_events(signals, evidence)

    if not signals:
        if not rows:
            return ()
        cards = (
            TimelineCard(
                start=rows[0].minute,
                end=rows[-1].minute,
                severity="Info",
                representative_event="선별된 이상 신호 없음",
                impacts=(TimelineItem("선별 규칙에 해당하는 이상·피크 없음"),),
                raw_rows=rows,
            ),
        )
    else:
        cards = _merge_cards(signals, rows)

    by_id = {item.evidence_id: item for item in evidence}
    return tuple(
        replace(
            card,
            citations=_citation_lines(card.evidence_refs, by_id),
            evidence_citations=citations(card.evidence_refs, evidence),
        )
        for card in cards
    )


def _minute(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def _with_followup_events(
    signals: list[_Signal], evidence: tuple[Evidence, ...]
) -> list[_Signal]:
    """Show later observations separately, without labeling a recovery."""
    result = []
    by_id = {item.evidence_id: item for item in evidence}
    for signal in signals:
        text, marker, recovery = signal.item.text.partition(" · 다음 관측:")
        if not marker or signal.end <= signal.start:
            result.append(signal)
            continue
        recovery_refs = tuple(
            ref
            for ref in signal.item.evidence_refs
            if ref in by_id and _minute(by_id[ref].event_time) == signal.end
        )
        result.append(
            replace(
                signal,
                end=max(signal.start, signal.end - _ONE_MINUTE),
                item=TimelineItem(
                    text,
                    tuple(
                        ref
                        for ref in signal.item.evidence_refs
                        if ref not in recovery_refs
                    ),
                ),
            )
        )
        recovery = re.sub(r"\b\d{2}:\d{2}\s+", "", recovery.strip())
        result.append(
            _Signal(
                start=signal.end,
                end=signal.end,
                severity="Info",
                category="impact",
                item=TimelineItem("다음 관측: " + recovery, recovery_refs),
            )
        )
    return result


def _groups(rows: tuple[TimelineRow, ...], predicate) -> list[tuple[TimelineRow, ...]]:
    grouped: list[list[TimelineRow]] = []
    for row in rows:
        if not predicate(row):
            continue
        if grouped and row.minute - grouped[-1][-1].minute == _ONE_MINUTE:
            grouped[-1].append(row)
        else:
            grouped.append([row])
    return [tuple(group) for group in grouped]


def _refs_between(
    evidence: tuple[Evidence, ...],
    start: datetime,
    end: datetime,
    *,
    sources: tuple[EvidenceSource, ...] = (),
    event_type: str = "",
) -> tuple[str, ...]:
    refs = []
    for item in evidence:
        if sources and item.source not in sources:
            continue
        if event_type and (item.event_type or "") != event_type:
            continue
        if start <= item.event_time <= end:
            refs.append(item.evidence_id)
    return tuple(dict.fromkeys(refs))


def _next_row(rows: tuple[TimelineRow, ...], moment: datetime) -> TimelineRow | None:
    wanted = moment + _ONE_MINUTE
    return next((row for row in rows if row.minute == wanted), None)


def _previous_row(
    rows: tuple[TimelineRow, ...], moment: datetime
) -> TimelineRow | None:
    wanted = moment - _ONE_MINUTE
    return next((row for row in rows if row.minute == wanted), None)


def _slowlog_signals(
    rows: tuple[TimelineRow, ...], evidence: tuple[Evidence, ...]
) -> list[_Signal]:
    signals: list[_Signal] = []
    for group in _groups(rows, lambda row: row.counts.get("slowlog", 0) > 0):
        counts = [row.counts.get("slowlog", 0) for row in group]
        took = max(
            (row for row in group if row.took_max_ms is not None),
            key=lambda row: row.took_max_ms or 0,
            default=None,
        )
        if len(group) == 1:
            text = f"slowlog {counts[0]}건"
        else:
            text = f"slowlog {counts[0]}→{counts[-1]}건 (최고 {max(counts)}건)"
        if took is not None and took.took_max:
            text += f", took 최고 {took.took_max}"

        end = group[-1].minute
        following = _next_row(rows, end)
        if (
            following is not None
            and not following.failed
            and following.counts.get("slowlog", 0) == 0
        ):
            end = following.minute
            text += " · 다음 관측: 다음 분 slowlog 0건"
        elif following is None or following.failed:
            text += " · 마지막 관측"

        signals.append(
            _Signal(
                start=group[0].minute,
                end=end,
                severity="Info",
                category="impact",
                item=TimelineItem(
                    text,
                    _refs_between(
                        evidence,
                        group[0].minute,
                        group[-1].minute,
                        sources=(EvidenceSource.SLOWLOG,),
                    ),
                ),
            )
        )
    return signals


def _rejected_signals(
    rows: tuple[TimelineRow, ...], evidence: tuple[Evidence, ...]
) -> list[_Signal]:
    signals: list[_Signal] = []
    for group in _groups(
        rows,
        lambda row: row.search_rejected_max > 0 or row.write_rejected_max > 0,
    ):
        previous = _previous_row(rows, group[0].minute)
        if previous is not None and (
            previous.failed or previous.counts.get("node_metric", 0) <= 0
        ):
            previous = None
        search_start = (
            previous.search_rejected_max
            if previous is not None
            else group[0].search_rejected_max
        )
        write_start = (
            previous.write_rejected_max
            if previous is not None
            else group[0].write_rejected_max
        )
        search_end = group[-1].search_rejected_max
        write_end = group[-1].write_rejected_max
        text = (
            f"search rejected 누적값 {search_start}→{search_end} "
            f"(최고 {max(row.search_rejected_max for row in group)}), "
            f"write rejected 누적값 {write_start}→{write_end} "
            f"(최고 {max(row.write_rejected_max for row in group)}) — "
            "누적 카운터이며 실제 증가량으로 단정할 수 없음"
        )
        end = group[-1].minute
        following = _next_row(rows, end)
        if (
            following is not None
            and not following.failed
            and following.counts.get("node_metric", 0) > 0
            and following.search_rejected_max == 0
            and following.write_rejected_max == 0
        ):
            end = following.minute
            text += " · 다음 관측: 다음 분 누적 rejected 값 0"
        elif (
            following is None
            or following.failed
            or following.counts.get("node_metric", 0) == 0
        ):
            text += " · 마지막 관측"

        signals.append(
            _Signal(
                start=group[0].minute,
                end=end,
                severity="Info",
                category="impact",
                item=TimelineItem(
                    text,
                    _refs_between(
                        evidence,
                        group[0].minute,
                        group[-1].minute,
                        sources=(EvidenceSource.NODE_METRIC,),
                        event_type="node_metric_rejected",
                    ),
                ),
            )
        )
    return signals


def _failed_signals(rows: tuple[TimelineRow, ...]) -> list[_Signal]:
    signals: list[_Signal] = []
    for group in _groups(rows, lambda row: row.failed):
        end = group[-1].minute
        following = _next_row(rows, end)
        text = f"분석 실패 {len(group)}분"
        if following is not None and not following.failed:
            end = following.minute
            text += " · 다음 관측: 다음 분 분석 성공"
        elif following is None:
            text += " · 마지막 관측"
        signals.append(
            _Signal(
                start=group[0].minute,
                end=end,
                severity="Warning",
                category="impact",
                item=TimelineItem(text),
            )
        )
    return signals


def _latency_peak_signals(
    rows: tuple[TimelineRow, ...], evidence: tuple[Evidence, ...]
) -> list[_Signal]:
    signals: list[_Signal] = []

    took_rows = tuple(
        row for row in rows if not row.failed and row.took_max_ms is not None
    )
    if took_rows:
        values = [row.took_max_ms for row in took_rows if row.took_max_ms is not None]
        baseline = median(values)
        threshold = baseline * 2
        warning_groups = _groups(
            rows,
            lambda row: (
                not row.failed
                and row.took_max_ms is not None
                and bool(baseline)
                and row.took_max_ms >= threshold
            ),
        )
        if warning_groups:
            for group in warning_groups:
                peak = max(group, key=lambda row: row.took_max_ms or 0)
                peak_value = peak.took_max_ms or 0
                start = group[0].minute
                peak_end = group[-1].minute
                text = (
                    f"slowlog 지연 피크 {peak.took_max or f'{peak_value}ms'} "
                    f"(급증 구간 {start:%H:%M}–{peak_end:%H:%M}, "
                    f"피크 시각 {peak.minute:%H:%M}, "
                    f"비어 있지 않은 분 중앙값 {_format_ms(baseline)})"
                )
                end = peak_end
                following = _next_row(rows, peak_end)
                if (
                    following is not None
                    and not following.failed
                    and following.took_max_ms is not None
                    and following.took_max_ms < threshold
                ):
                    end = following.minute
                    text += (
                        " · 다음 관측: 다음 분 "
                        f"{following.took_max or _format_ms(following.took_max_ms)}"
                    )
                elif (
                    following is None
                    or following.failed
                    or following.took_max_ms is None
                ):
                    text += " · 마지막 관측"
                signals.append(
                    _Signal(
                        start=start,
                        end=end,
                        severity="Warning",
                        category="impact",
                        item=TimelineItem(
                            text,
                            _refs_between(
                                evidence,
                                start,
                                peak_end,
                                sources=(EvidenceSource.SLOWLOG,),
                            ),
                        ),
                    )
                )
        else:
            peak = max(took_rows, key=lambda row: row.took_max_ms or 0)
            text = (
                f"slowlog 지연 피크 {peak.took_max or f'{peak.took_max_ms}ms'} "
                f"(비어 있지 않은 분 중앙값 {_format_ms(baseline)})"
            )
            signals.append(
                _Signal(
                    start=peak.minute,
                    end=peak.minute,
                    severity="Info",
                    category="impact",
                    item=TimelineItem(
                        text,
                        _refs_between(
                            evidence,
                            peak.minute,
                            peak.minute,
                            sources=(EvidenceSource.SLOWLOG,),
                        ),
                    ),
                )
            )

    runtime_rows = tuple(
        row for row in rows if not row.failed and row.runtime_max is not None
    )
    if runtime_rows:
        values = [
            row.runtime_max for row in runtime_rows if row.runtime_max is not None
        ]
        baseline = median(values)
        threshold = baseline * 2
        warning_groups = _groups(
            rows,
            lambda row: (
                not row.failed
                and row.runtime_max is not None
                and bool(baseline)
                and row.runtime_max >= threshold
            ),
        )
        if warning_groups:
            for group in warning_groups:
                peak = max(group, key=lambda row: row.runtime_max or Decimal(0))
                peak_value = peak.runtime_max or Decimal(0)
                start = group[0].minute
                peak_end = group[-1].minute
                text = (
                    f"query runtime 지연 피크 {_format_seconds(peak_value)} "
                    f"(급증 구간 {start:%H:%M}–{peak_end:%H:%M}, "
                    f"피크 시각 {peak.minute:%H:%M}, "
                    f"비어 있지 않은 분 중앙값 {_format_seconds(baseline)})"
                )
                end = peak_end
                following = _next_row(rows, peak_end)
                if (
                    following is not None
                    and not following.failed
                    and following.runtime_max is not None
                    and following.runtime_max < threshold
                ):
                    end = following.minute
                    text += (
                        " · 다음 관측: 다음 분 "
                        f"{_format_seconds(following.runtime_max)}"
                    )
                elif (
                    following is None
                    or following.failed
                    or following.runtime_max is None
                ):
                    text += " · 마지막 관측"
                signals.append(
                    _Signal(
                        start=start,
                        end=end,
                        severity="Warning",
                        category="impact",
                        item=TimelineItem(
                            text,
                            _refs_between(
                                evidence,
                                start,
                                peak_end,
                                sources=(EvidenceSource.QUERY_LOG,),
                            ),
                        ),
                    )
                )
        else:
            peak = max(runtime_rows, key=lambda row: row.runtime_max or Decimal(0))
            peak_value = peak.runtime_max or Decimal(0)
            text = (
                f"query runtime 지연 피크 {_format_seconds(peak_value)} "
                f"(비어 있지 않은 분 중앙값 {_format_seconds(baseline)})"
            )
            signals.append(
                _Signal(
                    start=peak.minute,
                    end=peak.minute,
                    severity="Info",
                    category="impact",
                    item=TimelineItem(
                        text,
                        _refs_between(
                            evidence,
                            peak.minute,
                            peak.minute,
                            sources=(EvidenceSource.QUERY_LOG,),
                        ),
                    ),
                )
            )
    return signals


def _format_ms(value: float) -> str:
    if value >= 1_000:
        return f"{value / 1_000:g}s"
    return f"{value:g}ms"


def _format_seconds(value: Decimal) -> str:
    return f"{value.normalize()}s"


def _volume_spike_signals(
    rows: tuple[TimelineRow, ...], evidence: tuple[Evidence, ...]
) -> list[_Signal]:
    sources = sorted(
        {
            source
            for row in rows
            for source, count in row.counts.items()
            if count > 0 and source != "node_metric"
        }
    )
    signals: list[_Signal] = []
    for source in sources:
        nonempty = [
            row.counts.get(source, 0) for row in rows if row.counts.get(source, 0) > 0
        ]
        if not nonempty:
            continue
        baseline = median(nonempty)

        def is_spike(
            row: TimelineRow, source: str = source, baseline: float = baseline
        ) -> bool:
            count = row.counts.get(source, 0)
            return count >= baseline * 2 and count - baseline >= 10

        for group in _groups(rows, is_spike):
            counts = [row.counts.get(source, 0) for row in group]
            text = (
                f"{source} 로그량 급증 {counts[0]}→{counts[-1]}건 "
                f"(최고 {max(counts)}건, 비어 있지 않은 분 중앙값 {baseline:g}건)"
            )
            end = group[-1].minute
            following = _next_row(rows, end)
            if (
                following is not None
                and not following.failed
                and not is_spike(following)
            ):
                end = following.minute
                text += f" · 다음 관측: 다음 분 {following.counts.get(source, 0)}건"
            elif following is None or following.failed:
                text += " · 마지막 관측"
            source_enum = _source_enum(source)
            refs = (
                _refs_between(
                    evidence,
                    group[0].minute,
                    group[-1].minute,
                    sources=(source_enum,),
                )
                if source_enum is not None
                else ()
            )
            signals.append(
                _Signal(
                    start=group[0].minute,
                    end=end,
                    severity="Info",
                    category="impact",
                    item=TimelineItem(text, refs),
                )
            )
    return signals


def _source_enum(source: str) -> EvidenceSource | None:
    try:
        return EvidenceSource(source)
    except ValueError:
        return None


def _cause_signals(
    observations: Observations,
    rows: tuple[TimelineRow, ...],
    evidence: tuple[Evidence, ...],
) -> list[_Signal]:
    signals = _master_signals(observations, rows, evidence)
    last_minute = rows[-1].minute if rows else None

    for item in evidence:
        if item.source in (
            EvidenceSource.SLOWLOG,
            EvidenceSource.QUERY_LOG,
            EvidenceSource.MASTER_LOG,
            EvidenceSource.CLUSTER_STATE,
            EvidenceSource.NODE_LOG,
        ):
            continue

        severity = ""
        if item.source is EvidenceSource.NODE_METRIC:
            kind = item.event_type or ""
            if kind == "node_metric_rejected" or "rejected" in kind:
                severity = "Info"
            elif kind in ("node_metric_heap", "node_metric_queue") or any(
                marker in kind for marker in ("heap", "queue")
            ):
                severity = "Warning"
            else:
                continue
        else:
            continue

        moment = item.event_time
        text = item.message
        if severity in ("Warning", "Critical") and last_minute == moment:
            text += " · 마지막 관측"
        signals.append(
            _Signal(
                start=moment,
                end=moment,
                severity=severity,
                category="cause",
                item=TimelineItem(text, (item.evidence_id,)),
            )
        )

    signals += _node_log_signals(rows, evidence)
    signals += _health_signals(observations, evidence)
    return signals


def _generic_severity(value: str | None) -> str:
    normalized = (value or "").strip().upper()
    if normalized == "CRITICAL":
        return "Critical"
    if normalized in ("ERROR", "WARN", "WARNING"):
        return "Warning"
    return "Info"


def _node_log_signals(
    rows: tuple[TimelineRow, ...], evidence: tuple[Evidence, ...]
) -> list[_Signal]:
    grouped: dict[tuple[str, str, str], list[Evidence]] = {}
    for item in evidence:
        if item.source is not EvidenceSource.NODE_LOG:
            continue
        key = (
            item.event_type or "node_log",
            item.node_name or item.node_id or "",
            _generic_severity(item.severity),
        )
        grouped.setdefault(key, []).append(item)

    signals: list[_Signal] = []
    last_minute = rows[-1].minute if rows else None
    for (event_type, node, severity), items in grouped.items():
        runs: list[list[Evidence]] = []
        for item in sorted(items, key=lambda item: item.event_time):
            if (
                runs
                and _minute(item.event_time) - _minute(runs[-1][-1].event_time)
                <= _ONE_MINUTE
            ):
                runs[-1].append(item)
            else:
                runs.append([item])

        for run in runs:
            start = run[0].event_time
            end = run[-1].event_time
            if len(run) == 1:
                text = run[0].message
            else:
                subject = f"{node} " if node else ""
                text = (
                    f"{subject}{event_type} {len(run)}건 — "
                    f"대표: {_excerpt(run[0].message)}"
                )
            if severity in ("Warning", "Critical") and last_minute == end:
                text += " · 마지막 관측"
            signals.append(
                _Signal(
                    start=start,
                    end=end,
                    severity=severity,
                    category="cause",
                    item=TimelineItem(
                        text,
                        tuple(item.evidence_id for item in run),
                    ),
                )
            )
    return signals


def _master_key_from_event(event: MasterEvent) -> str:
    match = _ACTION_RE.search(event.line)
    if match:
        return f"action:{match.group(1)}"
    if event.logger:
        return f"logger:{event.logger}"
    return f"line:{event.line}"


def _master_key_from_evidence(item: Evidence) -> str:
    match = _ACTION_RE.search(item.message)
    if match:
        return f"action:{match.group(1)}"
    match = _LOGGER_RE.search(item.message)
    if match:
        return f"logger:{match.group(1)}"
    if item.event_type:
        return f"event:{item.event_type}"
    return f"line:{item.message}"


def _master_signals(
    observations: Observations,
    rows: tuple[TimelineRow, ...],
    evidence: tuple[Evidence, ...],
) -> list[_Signal]:
    selected: dict[str, list[Evidence]] = {}
    for item in evidence:
        if item.source is EvidenceSource.MASTER_LOG:
            match=next((event for event in observations.master_events
                if event.timestamp==item.event_time
                and event.node==(item.node_name or item.node_id or '')
                and (event.line and event.line in item.message)),None)
            key=_master_key_from_event(match) if match else _master_key_from_evidence(item)
            selected.setdefault(key, []).append(item)

    # Collected master observations remain visible when LLM selection yields no evidence.
    for event in observations.master_events:
        if event.timestamp is not None and event.level.strip().upper() in ('ERROR','WARN','WARNING'):
            selected.setdefault(_master_key_from_event(event), [])
    signals: list[_Signal] = []
    for key, refs in selected.items():
        raw = [
            event
            for event in observations.master_events
            if event.timestamp is not None
            and _master_key_from_event(event) == key
            and event.level.strip().upper() in ("ERROR", "WARN", "WARNING")
        ]
        if not raw:
            raw = [
                MasterEvent(
                    timestamp=item.event_time,
                    node=item.node_name or item.node_id or "",
                    level=item.severity or "",
                    line=item.message,
                    rendered=item.message,
                )
                for item in refs
            ]

        groups: list[list[MasterEvent]] = []
        for event in sorted(
            raw, key=lambda event: event.timestamp or refs[0].event_time
        ):
            if event.timestamp is None:
                continue
            if (
                groups
                and _minute(event.timestamp) - _minute(groups[-1][-1].timestamp)
                <= _ONE_MINUTE
            ):
                groups[-1].append(event)
            else:
                groups.append([event])

        for group in groups:
            start = group[0].timestamp
            end = group[-1].timestamp
            levels = {event.level.strip().upper() for event in group}
            severity = "Warning" if "ERROR" in levels else "Info"
            representative = _excerpt(group[0].line or group[0].rendered)
            label = key.split(":", 1)[-1].split("/")[-1]
            text = f"{label} {len(group)}건 — 대표: {representative}"
            if severity == "Warning" and rows and end == rows[-1].minute:
                text += " · 마지막 관측"
            evidence_refs = tuple(
                item.evidence_id
                for item in refs
                if start <= item.event_time <= end
            ) or tuple(item.evidence_id for item in refs)
            signals.append(
                _Signal(
                    start=start,
                    end=end,
                    severity=severity,
                    category="cause",
                    item=TimelineItem(text, tuple(dict.fromkeys(evidence_refs))),
                )
            )
    return signals


def _health_signals(
    observations: Observations, evidence: tuple[Evidence, ...]
) -> list[_Signal]:
    if not observations.requested:
        return []
    window_start = min(start for start, _end in observations.requested)
    window_end = max(end for _start, end in observations.requested)
    points = tuple(sorted(observations.health, key=lambda point: point.at))
    signals: list[_Signal] = []
    for index, point in enumerate(points):
        status = point.status.lower()
        if status not in ("red", "yellow") or not (
            window_start <= point.at <= window_end
        ):
            continue
        severity = "Critical" if status == "red" else "Warning"
        text = (
            f"cluster health {status.upper()} "
            f"(unassigned={point.unassigned_shards}, active={point.active_shards})"
        )
        following_green = next(
            (
                next_point
                for next_point in points[index + 1 :]
                if next_point.status.lower() == "green"
            ),
            None,
        )
        if (
            following_green is not None
            and window_start <= following_green.at <= window_end
        ):
            end = following_green.at
            text += f" · 다음 관측: {_minute(following_green.at):%H:%M} GREEN"
        else:
            end = point.at
            text += " · 마지막 관측"
        refs = _refs_between(
            evidence,
            _minute(point.at),
            _minute(end),
            sources=(EvidenceSource.CLUSTER_STATE,),
        )
        signals.append(
            _Signal(
                start=point.at,
                end=end,
                severity=severity,
                category="cause",
                item=TimelineItem(text, refs),
            )
        )
    return signals


def _annotation_signals(
    annotations: tuple[TimelineAnnotation, ...],
    evidence: tuple[Evidence, ...],
    *,
    verification_status: str,
) -> list[_Signal]:
    if str(verification_status).upper() != "PASSED":
        return []
    known = {item.evidence_id: item for item in evidence}
    signals: list[_Signal] = []
    for annotation in annotations:
        cited = [known[ref] for ref in annotation.evidence_refs if ref in known]
        if not annotation.evidence_refs or not cited:
            continue
        if not any(
            item.time_origin == "parsed" and item.event_time == annotation.at
            for item in cited
        ):
            continue
        moment = annotation.at
        signals.append(
            _Signal(
                start=moment,
                end=moment,
                severity="",
                category="interpretation",
                item=TimelineItem(annotation.description, annotation.evidence_refs),
            )
        )
    return signals


# 카드 하나가 소스 하나당 인용할 근거 원문 수. 넘으면 "… 외 N건"으로 자른다.
_CITATIONS_PER_SOURCE_MAX = 3


def _citation_lines(
    evidence_refs: tuple[str, ...], by_id: dict[str, Evidence]
) -> tuple[str, ...]:
    """카드의 근거를 출처별로 묶어 사람이 읽을 인용 줄로 그린다.

    근거가 없는 출처는 나오지 않는다 — 없는 값을 지어내지 않는다는 원칙과 같다
    (``evidence.py``, ``report_text.py`` 등).
    """
    by_source: dict[EvidenceSource, list[Evidence]] = {}
    for ref in evidence_refs:
        item = by_id.get(ref)
        if item is None:
            continue
        by_source.setdefault(item.source, []).append(item)

    lines: list[str] = []
    for source in EvidenceSource:
        items = by_source.get(source)
        if not items:
            continue
        lines.append(f"{source} {len(items)}건")
        shown = items[:_CITATIONS_PER_SOURCE_MAX]
        lines.extend(cite(item) for item in shown)
        if len(items) > len(shown):
            lines.append(f"… 외 {len(items) - len(shown)}건")
    return tuple(lines)


def _merge_cards(
    signals: list[_Signal], rows: tuple[TimelineRow, ...]
) -> tuple[TimelineCard, ...]:
    ordered = sorted(
        signals,
        key=lambda signal: (
            signal.start,
            signal.end,
            -_SEVERITY_RANK[signal.severity],
            _category_rank(signal.category),
        ),
    )
    groups: list[list[_Signal]] = []
    for signal in ordered:
        if groups and signal.start == groups[-1][0].start:
            groups[-1].append(signal)
        else:
            groups.append([signal])

    cards: list[TimelineCard] = []
    for group in groups:
        start = min(signal.start for signal in group)
        end = max(signal.end for signal in group)
        severity = (
            max(
                (signal.severity for signal in group),
                key=lambda level: _SEVERITY_RANK[level],
                default="Info",
            )
            or "Info"
        )
        impacts = _items(group, "impact")
        causes = _items(group, "cause")
        interpretations = _items(group, "interpretation")
        refs = tuple(
            dict.fromkeys(ref for signal in group for ref in signal.item.evidence_refs)
        )
        representative = _representative(group)
        raw_rows = tuple(row for row in rows if start <= row.minute <= end)
        cards.append(
            TimelineCard(
                start=start,
                end=end,
                severity=severity,
                representative_event=representative,
                impacts=impacts,
                causes=causes,
                interpretations=interpretations,
                evidence_refs=refs,
                raw_rows=raw_rows,
            )
        )
    return tuple(cards)


def _category_rank(category: str) -> int:
    return {"impact": 0, "cause": 1, "interpretation": 2}[category]


def _items(group: list[_Signal], category: str) -> tuple[TimelineItem, ...]:
    seen: set[tuple[str, tuple[str, ...]]] = set()
    items: list[TimelineItem] = []
    for signal in sorted(group, key=lambda item: (item.start, item.end)):
        if signal.category != category:
            continue
        key = (signal.item.text, signal.item.evidence_refs)
        if key in seen:
            continue
        seen.add(key)
        items.append(signal.item)
    return tuple(items)


def _representative(group: list[_Signal]) -> str:
    chosen = min(
        group,
        key=lambda signal: (
            -_SEVERITY_RANK[signal.severity],
            _category_rank(signal.category),
            signal.start,
        ),
    )
    return _excerpt(chosen.item.text, limit=100)


def _excerpt(text: str, limit: int = 180) -> str:
    compact = " ".join((text or "").split())
    return compact if len(compact) <= limit else compact[: limit - 1] + "…"
