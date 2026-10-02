"""es_query_log의 ``url`` 컬럼을 수집 시점에 필요한 값으로 줄인다.

``url``은 요청 줄과 DSL 본문 전체라 키워드가 수백 개 실린다. DTO에는 대상
호스트·인덱스·조건 요약만 남기고 원문은 보관하지 않는다.
"""
from __future__ import annotations

import json
import re
from urllib.parse import unquote, urlsplit


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


def request_fields(url: str) -> dict[str, object]:
    """``QueryLogEntry``의 target_host·index_name·conditions 값을 만든다."""
    head, _, body = url.strip().partition('\n')
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
            conditions = ['원문: ' + _excerpt(url)]
    if not conditions:
        conditions = ['원문: ' + _excerpt(url)] if url else []
    return {
        'target_host': target,
        'index_name': index,
        'conditions': tuple(dict.fromkeys(conditions)),
    }
