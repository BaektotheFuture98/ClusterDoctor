"""코드가 판정한 지연 범위와 노드 응답 지연의 문장. HTML과 텍스트 renderer가 공유한다."""
from cluster_doctor.incident_analysis_agent.service.observation.correlation import CrossLinks, NodeCard


def _shape(shape: tuple[str, int, int]) -> str:
    cmd, days, indexes = shape
    return f'{cmd} · {days}일 · 인덱스 {indexes}개'


def late_nodes(links: CrossLinks) -> tuple[NodeCard, ...]:
    # 노드 카드는 node-left 같은 줄에서도 만들어진다. 그런 노드는 늦게 응답한 것이 아니므로
    # 요청을 보낸 시각(sent_at)이 있는 대기가 하나라도 있는 노드만 센다.
    return tuple(card for card in links.nodes if any(wait.sent_at for wait in card.waits))


def verdict_line(links: CrossLinks) -> str:
    verdict = links.verdict
    shapes = ', '.join(_shape(s) for s in verdict.slow_shapes[:3])
    if verdict.scope == 'no_query_log':
        # 로그를 못 읽은 것과 읽었는데 비어 있는 것은 다르다. 어느 쪽도 "느린 요청 없음"으로 읽히면 안 된다.
        if verdict.query_log_failed:
            return '쿼리 실행 로그를 수집하지 못해 지연 범위를 판단할 수 없습니다.'
        return '수집된 쿼리 실행 로그가 없습니다.'
    if verdict.scope == 'no_slow':
        return f'{links.slow_threshold}초 이상 실행된 요청이 없습니다.'
    if verdict.scope == 'specific':
        return (f'느린 요청은 {shapes} 형태에 한정됐습니다. 같은 시각 다른 요청의 실행시간 중앙값은 '
                f'{verdict.slow_minute_median}s로, 느린 요청이 없던 분({verdict.reference_median}s)과 비슷합니다.')
    if verdict.scope == 'cluster_wide':
        return (f'같은 시각 다른 요청도 함께 느려졌습니다. 다른 요청의 실행시간 중앙값이 '
                f'{verdict.reference_median}s에서 {verdict.slow_minute_median}s로 늘었습니다.')
    return f'느린 요청은 {shapes} 형태에서 나왔지만, 비교할 요청이 부족해 다른 요청도 느려졌는지는 판단할 수 없습니다.'


def node_line(links: CrossLinks) -> str:
    late = late_nodes(links)
    if not late:
        return ''
    names = ', '.join(card.node for card in late[:4])
    return f'분석 구간에 노드 {len(late)}대({names})가 마스터 요청에 늦게 응답했습니다.'
