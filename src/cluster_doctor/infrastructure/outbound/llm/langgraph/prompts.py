"""그래프 노드별 프롬프트.

여기 있는 한국어 문자열이 이 서비스의 산출물 품질을 결정한다. 최종 리포트의
7개 섹션 구성은 스펙에서 온 것이므로 바꾸기 전에 스펙을 먼저 확인할 것.
"""

from functools import singledispatch

from cluster_doctor.domain.model.log_entry import (
    LogEntry,
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.domain.model.time_range import TimeRange

_SOURCE_DESC = {
    "slowlog":      "ES 슬로우 쿼리 로그. JSON 형태의 원본 데이터 포함.",
    "es_query_log": "packetbeat가 수집한 ES 실시간 쿼리 실행 기록.",
    "node_metric":  "ES 노드 리소스 메트릭.",
}

# 한 쿼리가 키워드 200개 넘게 싣고 오는 경우가 있다. 그대로 그리면 한 줄이
# 2,000자를 넘고(실측 2,029자), 같은 유저가 agg와 count로 같은 목록을 두 번
# 보내면 4,000자가 연달아 들어간다. 분당 입력 토큰 한도를 넘긴 주범이었다.
#
# 진단에 필요한 것은 "이 유저가 어떤 주제를 검색했나"이지 전량이 아니다.
# 앞 몇 개면 주제가 드러나고, 전체 개수는 따로 알려 주면 "키워드 215개짜리
# 쿼리"라는 사실도 근거로 쓸 수 있다.
_MAX_KEYWORDS_SHOWN = 5


def _format_keywords(keywords: tuple[str, ...]) -> str:
    if len(keywords) <= _MAX_KEYWORDS_SHOWN:
        return f"keyword={list(keywords)}"
    shown = list(keywords[:_MAX_KEYWORDS_SHOWN])
    return f"keyword={shown} 외 {len(keywords) - _MAX_KEYWORDS_SHOWN}개"


@singledispatch
def format_log_line(entry) -> str:
    """한 항목을 프롬프트 한 줄로 그린다. 소스마다 그리는 법이 다르다.

    표현을 도메인 모델이 아니라 여기 두는 이유: 어떤 값을 어떻게 보여줄지는
    프롬프트의 관심사다. 모델은 값을 값인 채로 들고 있기만 하면 된다.

    등록되지 않은 타입은 조용히 넘기지 않고 터뜨린다 — 새 소스를 추가하며
    렌더러를 잊으면 프롬프트에 빈 줄이 들어가고, 그 사실이 아무 데도 남지
    않는다.
    """
    raise TypeError(f"프롬프트 줄로 그릴 수 없는 항목입니다: {type(entry).__name__}")


@format_log_line.register
def _format_slowlog(entry: SlowlogEntry) -> str:
    return (
        f"  {entry.timestamp} [SLOWLOG] node={entry.node or '-'} "
        f"comp={entry.index_name or '-'} "
        f"took={entry.took}, {entry.total_hits}, shards={entry.total_shards}, "
        f"id={entry.opaque_id}, query={entry.query}"
    )


@format_log_line.register
def _format_query_log(entry: QueryLogEntry) -> str:
    line = (
        f"  {entry.timestamp} [{'SUCCESS' if entry.success else 'FAIL'}] "
        f"node={entry.host or '-'} comp={entry.service or '-'} "
        f"[{entry.cmd}] project={entry.project} env={entry.env} "
        f"cluster={entry.cluster} runtime={entry.run_time}s "
        f"{_format_keywords(entry.keywords)}"
    )
    if entry.company or entry.user:
        line += f" company={entry.company or '-'} user={entry.user or '-'}"
    return line


@format_log_line.register
def _format_node_metric(entry: NodeMetricEntry) -> str:
    return (
        f"  {entry.timestamp} [METRIC] "
        f"node={entry.node_name} ({entry.node_ip}) comp=- "
        f"cpu={entry.os_cpu_percent}% mem={entry.os_mem_used_percent}% "
        f"proc_cpu={entry.process_cpu_percent}% "
        f"jvm_heap={entry.jvm_heap_used_percent}% "
        f"search(active={entry.search_active},queue={entry.search_queue},"
        f"rejected={entry.search_rejected}) "
        f"write(active={entry.write_active},queue={entry.write_queue},"
        f"rejected={entry.write_rejected})"
    )


def build_minute_prompt(minute_logs: list[LogEntry], minute_label: str) -> str:
    """한 구간을 압축하라고 시키는 프롬프트.

    로그를 소스별로 묶되 **샘플링하지 않는다**. 1분치 전량을 보여주는 것이
    이 그래프의 존재 이유다.

    응답 *형식*은 지시하지 않는다. 이 호출에는 ``response_format=MinuteOutput``이
    걸려 있어 스키마가 형식을 강제하기 때문이다. 예전에는 여기서 "요약:/근거:
    형식으로 답하라"고 시켰는데, 모순된 형식 계약 두 개가 동시에 전달되는
    상태였다 — 스키마 쪽이 이기므로 그 지시는 효과 없이 구간마다 토큰만
    축냈다. 형식 대신 각 필드가 무엇을 담아야 하는지만 말한다.
    """
    grouped: dict[str, list[LogEntry]] = {}
    for log in minute_logs:
        grouped.setdefault(log.source, []).append(log)

    sections = []
    for source, entries in grouped.items():
        desc = _SOURCE_DESC.get(source, source)
        lines = [f"[{source}] ({desc}) - {len(entries)}건 (전량)"]
        lines.extend(format_log_line(e) for e in entries)
        sections.append("\n".join(lines))

    return f"""아래는 {minute_label} 한 구간의 로그 전량이다. 이 구간만 분석하라.

=== 로그 데이터 ===
{chr(10).join(sections)}

=== 지시 ===
• 이 구간에서 실제로 관찰된 것만 쓴다. 다른 시간대를 추측하지 않는다.
• 이상 징후가 없으면 "특이사항 없음"이라고 명확히 쓴다. 억지로 문제를 만들지 않는다.
• 노드명·수치·쿼리 내용을 구체적으로 인용한다. "부하가 높다"가 아니라
  "es-data-02가 cpu=94%, search queue=920"처럼 쓴다.
• 반드시 한국어로 쓴다.
• 사고 과정·추론 설명은 담지 않는다. 결론만 담는다.

=== 각 필드에 담을 것 ===
summary  이 구간에서 무슨 일이 있었는지. 한두 문장.
evidence 위 요약의 근거가 된 로그를 원문 그대로, 한 줄에 하나씩.
         종합 단계가 이것을 인용해 최종 리포트를 쓰므로, 요약으로 바꿔
         쓰지 말고 원문을 그대로 옮긴다. 특이사항이 없으면 비워 둔다."""


def build_synthesis_prompt(
    time_range: TimeRange, minute_sections: str, analyzed: int, failed: int
) -> str:
    """구간별 결과를 최종 리포트로 합성하라고 시키는 프롬프트.

    원본 로그 전량이 아니라 구간별 요약 + 근거 원문만 들어온다. 전량을 다시
    넣으면 단발 모드와 똑같은 절단이 재발한다.
    """
    coverage = f"분석된 구간 {analyzed}개"
    if failed:
        coverage += f", 분석 실패 {failed}개"

    return f"""분석 시간 범위: {time_range.start} ~ {time_range.end}
({coverage})

아래는 이 시간 범위를 1분 단위로 나눠 각각 분석한 결과다. 로그가 없던 구간은
목록에 없다. 이것들을 종합해 하나의 진단 리포트를 작성하라.

=== 구간별 분석 ===
{minute_sections}

=== 분석 원칙 ===
• slowlog에 기록된 쿼리는 임계치 초과일 뿐, 그 자체로 문제가 아님. 빈도·리소스 영향 등을 종합 판단.
• 실제 이상 징후가 없으면 "특이사항 없음"이라고 명확히 표현. 억지로 문제 만들지 않기.
• FAIL 로그는 일시적 오류인지 반복 장애인지 구분.
• 구간을 가로질러 반복되거나 번지는 패턴을 우선한다. 한 구간에만 나타난 것과
  여러 구간에 걸친 것을 구분해서 쓴다.
• 근거로 제시된 로그 원문의 쿼리·노드명·수치를 그대로 인용한다.
• "분석 실패"로 표시된 구간이 있으면 그 사실을 리포트에 밝힌다. 없는 것처럼 쓰지 않는다.

=== 알려진 문제 패턴 ===
1. 토큰 분석 이슈: 미분석 토큰 대상 검색이 부하 유발. wildcard/term 쿼리가 analyzed 필드에 사용된 징후.
2. 광범위 범위 검색: 날짜/범위 필터 없는 full scan성 쿼리.

=== 출력 규칙 ===
• 반드시 한국어로만 답한다.
• 사고 과정·추론·설명을 출력하지 않는다. 아래 형식의 최종 답변만 출력한다.

=== 응답 형식 ===
마크다운 금지. 아래 형식 준수:
• 섹션 제목 아래 ── 구분선
• • 불렛, 두 칸 들여쓰기 후 - 세부 내용
• 섹션 사이 빈 줄 두 줄

섹션 구성 (반드시 이 순서로):
1. 요청 이해
2. 발견된 문제점 (Critical/Warning/Info 분류)
3. 근본 원인
4. 소스 간 상관관계
5. 문제 쿼리 후보 (+ 사용자 정보 파악(user, company))
6. 권장 조치
7. 클러스터 건강 상태"""
