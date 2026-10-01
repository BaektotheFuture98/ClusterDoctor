"""Individual execution views shared by analysis and report delivery."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlsplit

from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry, record_json
if TYPE_CHECKING:
    from cluster_doctor.incident_analysis_agent.model.observations import SlowCandidate


def valid_runtime(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() and number >= 0 else None


def query_record_key(record: QueryLogEntry) -> str:
    # A fetched-record fingerprint for unambiguous attribution, never a query identity.
    canonical = json.dumps(json.loads(record_json(record)), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _excerpt(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[:limit] + '…'


def _conditions(value: object) -> list[str]:
    out: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {'size', 'sort', 'search_after', 'track_total_hits'}:
                out.append(f'{key}=' + _excerpt(json.dumps(child, ensure_ascii=False, separators=(',', ':'))))
            elif key == 'range' and isinstance(child, dict):
                for field, bounds in child.items():
                    out.append(f'{field}: ' + json.dumps(bounds, ensure_ascii=False, separators=(',', ':')))
            elif key == 'terms' and isinstance(child, dict):
                for field, values in child.items():
                    if isinstance(values, list):
                        out.append(f'{field}: {len(values)}개 값')
            elif key == 'query_string' and isinstance(child, dict):
                expression = str(child.get('query', ''))
                ranges = re.findall(r'([\w.]+):\[(.*?) TO (.*?)\]', expression)
                terms = re.findall(r'([\w.]+):\((.*?)\)', expression)
                out.extend(f'{field}: {start} ~ {end}' for field, start, end in ranges)
                out.extend(f'{field}: {len(items.split(" OR "))}개 조건' for field, items in terms)
                if not ranges and not terms and expression:
                    out.append('query=' + _excerpt(expression))
            else:
                out.extend(_conditions(child))
    elif isinstance(value, list):
        for child in value:
            out.extend(_conditions(child))
    return out


@dataclass(frozen=True)
class QueryRequestView:
    record: QueryLogEntry
    record_key: str
    ordinal: int
    execution_seconds: Decimal | None
    target_host: str | None
    index_name: str | None
    conditions: tuple[str, ...]


def _view(record: QueryLogEntry, ordinal: int) -> QueryRequestView:
    head, _, body = record.url.strip().partition('\n')
    endpoint = re.sub(r'^(?:GET|POST|PUT|DELETE|HEAD|PATCH)\s+', '', head, flags=re.I)
    target, index = None, None
    try:
        parsed = urlsplit(endpoint)
        if parsed.scheme in {'http', 'https'} and parsed.hostname:
            target = parsed.hostname
            first = unquote(parsed.path.strip('/').split('/')[0])
            index = first if first and not first.startswith('_') else None
    except ValueError:
        pass
    conditions: list[str] = []
    if body:
        try:
            conditions = _conditions(json.loads(body))
        except (ValueError, TypeError):
            conditions = ['원문: ' + _excerpt(record.url)]
    if not conditions:
        conditions = ['원문: ' + _excerpt(record.url)] if record.url else []
    return QueryRequestView(record, query_record_key(record), ordinal,
        valid_runtime(record.run_time), target, index, tuple(dict.fromkeys(conditions)))


def rank_query_requests(requests: tuple[QueryLogEntry, ...]) -> tuple[QueryRequestView, ...]:
    rows = [_view(record, i) for i, record in enumerate(requests)]
    return tuple(sorted(rows, key=lambda r: (
        r.execution_seconds is None,
        -r.execution_seconds if r.execution_seconds is not None else Decimal(0),
        r.record.timestamp, r.ordinal)))


def matching_candidate(record: QueryLogEntry, requests: tuple[QueryLogEntry, ...],
                       candidates: tuple[SlowCandidate, ...]) -> SlowCandidate | None:
    key = query_record_key(record)
    if sum(query_record_key(r) == key for r in requests) != 1:
        return None
    matches = [c for c in candidates if c.source == QueryLogEntry.source and c.query_record_key == key]
    return matches[0] if len(matches) == 1 else None
