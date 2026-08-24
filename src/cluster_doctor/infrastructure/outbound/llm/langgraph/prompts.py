"""그래프 노드별 프롬프트.

단발 모드의 ``prompt_builder.build_prompt``와 나란히 있는 별개의 것이다.
합치지 않은 이유는 목적이 다르기 때문이다. 단발 프롬프트는 한 번에 최종
리포트를 받아내야 하고, 여기 둘은 "한 구간을 압축"과 "압축본들을 합성"으로
역할이 쪼개져 있다.

여기 있는 한국어 문자열이 이 서비스의 산출물 품질을 결정한다. 최종 리포트의
7개 섹션 구성은 스펙에서 온 것이므로 바꾸기 전에 스펙을 먼저 확인할 것.
"""

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange

_SOURCE_DESC = {
    "slowlog":      "ES 슬로우 쿼리 로그. JSON 형태의 원본 데이터 포함.",
    "es_query_log": "packetbeat가 수집한 ES 실시간 쿼리 실행 기록.",
    "node_metric":  "ES 노드 리소스 메트릭.",
}


def format_log_line(entry: LogEntry) -> str:
    """로그 한 줄. 단발 모드의 표기와 같은 모양을 유지한다."""
    return (
        f"  {entry.timestamp} [{entry.level}] node={entry.node or '-'} "
        f"comp={entry.component or '-'} {entry.message}"
    )


def build_minute_prompt(minute_logs: list[LogEntry], minute_label: str) -> str:
    """한 구간을 압축하라고 시키는 프롬프트.

    로그를 소스별로 묶되 **샘플링하지 않는다**. 1분치 전량을 보여주는 것이
    이 그래프의 존재 이유다.
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

=== 출력 규칙 ===
• 반드시 한국어로만 답한다.
• 사고 과정·추론·설명을 출력하지 않는다. 아래 형식의 최종 답변만 출력한다.

=== 응답 형식 ===
마크다운 금지. 아래 두 부분만 이 순서로 출력한다.

요약:
(3줄 이내. 이 구간에서 무슨 일이 있었는지.)

근거:
(위 요약의 근거가 된 로그를 원문 그대로. 한 줄에 하나씩.
 특이사항이 없으면 이 항목은 비워 둔다.)"""


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
5. 문제 쿼리 후보
6. 권장 조치
7. 클러스터 건강 상태"""
