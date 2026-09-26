"""Map / Reduce 프롬프트.

두 단계 모두 **원인을 묻지 않는다.** 이 워크플로가 하는 일은 대량 로그를 줄여
후속 분석이 읽을 수 있는 크기로 만드는 것뿐이고, 원인 판단은 Evidence가 다
모인 뒤 Cross-source 단계에서 한다. 여기서 원인을 확정하면 그 판단이 한
datasource만 보고 내려진 것이 되며, 뒤 단계는 이미 내려진 결론을 확인하는
절차로 퇴화한다.

문자열 보간을 쓰지 않는다(f-string 아님). 본문에 JSON 예시의 중괄호가 있어
서식으로 해석되면 프롬프트가 조용히 망가진다.
"""

from __future__ import annotations

from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.minute_analysis.spec import AnalysisSpec
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.minute_analysis.state import (
    MinuteBucket,
    MinuteResult,
    RawRecord,
)

_MAP_HEADER = """너는 Elasticsearch 장애 분석 파이프라인의 로그 선별 단계다.

지금 하는 일은 **선별**이다. 원인을 추론하지 않는다. 결론을 쓰지 않는다.
아래 1분치 로그에서 후속 장애 분석에 의미가 있을 줄의 번호만 고른다.
"""

_MAP_RULES = """
규칙:
- 반드시 주어진 #번호 중에서만 고른다. 없는 번호를 만들어 내지 않는다.
- 로그 내용을 옮겨 적지 않는다. 번호와 짧은 이유만 쓴다.
- 의미 있는 줄이 없으면 빈 배열을 돌려준다. 억지로 채우지 않는다.
- event_type은 소문자 스네이크로 짧게 쓴다. 예: gc_pause, shard_failed, node_left,
  slow_query, circuit_breaker, rejection.
- reason은 "왜 이 줄이 후속 분석에 필요한가"를 한 문장으로 쓴다.

응답은 JSON 하나로만 한다.
"""


def build_map_prompt(spec: AnalysisSpec, bucket: MinuteBucket) -> str:
    """한 분의 레코드에서 후보를 고르게 하는 프롬프트."""
    lines = "\n".join(record.as_prompt_line() for record in bucket.records)
    return "\n".join(
        [
            _MAP_HEADER,
            f"데이터 소스: {spec.label}",
            f"구간: {bucket.minute:%Y-%m-%d %H:%M} (1분)",
            "",
            "이 소스에서 의미 있는 것:",
            spec.what_matters,
            "",
            "이 소스에서 버려야 하는 것:",
            spec.what_is_noise,
            _MAP_RULES,
            "",
            f"--- 로그 {len(bucket.records)}줄 ---",
            lines,
        ]
    )


_REDUCE_HEADER = """너는 Elasticsearch 장애 분석 파이프라인의 로그 선별 단계다.

분 단위로 고른 후보들이 아래에 모여 있다. 이제 **전체 구간을 함께 보고**
정말로 남길 것만 고른다.

지금 하는 일은 여전히 선별이다. 근본 원인을 확정하지 않는다.
어떤 줄이 원인이고 어떤 줄이 결과인지 판단하지 않는다.
그 판단은 모든 데이터 소스의 근거가 모인 뒤 다음 단계가 한다.
"""

_REDUCE_RULES = """
버릴 것:
- 구간 내내 같은 주기로 반복되어 특정 시점을 지목하지 못하는 이벤트.
- 평상시에도 나오는 정상 동작 기록.
- 다른 후보와 같은 사건을 가리키는 중복. 대표 한 줄만 남긴다.

남길 것:
- 그 시각에만 나타난 것. 시작·전환·급변을 표시하는 것.
- 심각도가 높은 것. 실패·거절·타임아웃·이탈.
- 다른 데이터 소스와 시각을 맞춰 볼 가치가 있는 것.

규칙:
- 반드시 주어진 #번호 중에서만 고른다.
- 상한을 넘겨 고르지 않는다.
- selection_reason에는 "왜 이것을 남기는가"만 쓴다. 원인 추정을 쓰지 않는다.

응답은 JSON 하나로만 한다.
"""


def build_reduce_prompt(
    spec: AnalysisSpec,
    results: list[MinuteResult],
    records: dict[int, RawRecord],
    *,
    limit: int,
) -> str:
    """분별 후보 전체에서 남길 것을 고르게 하는 프롬프트."""
    blocks: list[str] = []
    for result in sorted(results, key=lambda item: item.minute):
        if result.failed:
            blocks.append(f"--- {result.minute:%H:%M} [분석 실패] ---")
            continue
        if not result.selected:
            continue
        rows = []
        for item in result.selected:
            record = records.get(item.record_id)
            if record is None:
                continue
            rows.append(
                f"#{record.record_id} [{record.event_time:%H:%M:%S}]"
                f"{' ' + item.event_type if item.event_type else ''} {record.line}"
            )
        if rows:
            blocks.append(f"--- {result.minute:%H:%M} ---\n" + "\n".join(rows))

    body = "\n\n".join(blocks) or "(후보 없음)"
    return "\n".join(
        [
            _REDUCE_HEADER,
            f"데이터 소스: {spec.label}",
            f"최대 {limit}개까지 남길 수 있다.",
            "",
            "이 소스에서 의미 있는 것:",
            spec.what_matters,
            "",
            "이 소스에서 버려야 하는 것:",
            spec.what_is_noise,
            _REDUCE_RULES,
            "",
            "--- 분별 후보 ---",
            body,
        ]
    )
