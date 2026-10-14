"""코드가 판정한 지연 범위와 노드 응답 지연의 문장. HTML과 텍스트 renderer가 공유한다."""
from collections.abc import Iterable
from typing import NamedTuple
from cluster_doctor.incident_analysis_agent.service.observation.correlation import CrossLinks, NodeCard


def _shape(shape: tuple[str, int, int]) -> str:
    cmd, days, indexes = shape
    return f'{cmd}·{days}일·인덱스 {indexes}개'


def late_nodes(links: CrossLinks) -> tuple[NodeCard, ...]:
    # 노드 카드는 node-left 같은 줄에서도 만들어진다. 그런 노드는 늦게 응답한 것이 아니므로
    # 요청을 보낸 시각(sent_at)이 있는 대기가 하나라도 있는 노드만 센다.
    return tuple(card for card in links.nodes if any(wait.sent_at for wait in card.waits))


_SHAPE_LIMIT = 3


class VerdictText(NamedTuple):
    headline: str
    detail: str = ''


def _shape_list(entries: Iterable[str], total: int) -> str:
    # 일부만 보여 주면서 그것이 전부인 것처럼 읽히지 않게, 잘린 개수를 함께 적는다.
    text = ', '.join(list(entries)[:_SHAPE_LIMIT])
    return f'{text} 외 {total - _SHAPE_LIMIT}개 형태' if total > _SHAPE_LIMIT else text


def verdict_line(links: CrossLinks) -> VerdictText:
    verdict = links.verdict
    slow = [f'{_shape(s)} {n}건' for s, n in zip(verdict.slow_shapes, verdict.slow_counts)]
    if verdict.scope == 'no_query_log':
        # 로그를 못 읽은 것과 읽었는데 비어 있는 것은 다르다. 어느 쪽도 "느린 요청 없음"으로 읽히면 안 된다.
        if verdict.query_log_failed:
            return VerdictText('쿼리 실행 로그를 수집하지 못해 지연 범위를 판단할 수 없습니다.')
        return VerdictText('수집된 쿼리 실행 로그가 없습니다.')
    if verdict.scope == 'no_slow':
        return VerdictText(f'{links.slow_threshold}초 이상 실행된 요청이 없습니다.')
    if verdict.scope == 'cluster_wide':
        widened = [_shape(s) for s in verdict.widened_shapes]
        return VerdictText('느린 시각에는 여러 쿼리 형태의 요청이 함께 느려졌습니다. 클러스터 전반의 지연일 수 있습니다.',
                           f'함께 느려진 형태: {_shape_list(widened, len(widened))}')
    detail = f'주요 형태: {_shape_list(slow, len(slow))}'
    if verdict.scope == 'specific':
        if len(verdict.widened_shapes) == 1:
            detail += f' · {_shape(verdict.widened_shapes[0])}는 같은 시각 {links.slow_threshold}초 미만 요청도 함께 느려졌습니다'
        return VerdictText('느린 요청은 일부 쿼리 형태에서만 나왔고, 같은 시각 다른 요청은 느려지지 않았습니다.', detail)
    return VerdictText('느린 요청은 일부 쿼리 형태에서 나왔지만, 다른 요청도 느려졌는지는 비교할 요청이 부족해 판단할 수 없습니다.', detail)


def node_line(links: CrossLinks) -> str:
    late = late_nodes(links)
    if not late:
        return ''
    names = ', '.join(card.node for card in late[:4])
    return f'분석 구간에 노드 {len(late)}대({names})가 마스터 요청에 늦게 응답했습니다.'
