"""진단 프롬프트 조립. LLM provider와 무관하다.

여기 있는 한국어 문자열은 이 서비스의 실제 산출물 품질을 결정한다.
분석 원칙·문제 패턴·응답 섹션 구성은 스펙에서 온 것이므로
바꾸기 전에 스펙을 먼저 확인할 것.
"""

from collections import defaultdict

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange

_SAMPLE_SIZE = 200

_SOURCE_DESC = {
    "slowlog":      "ES 슬로우 쿼리 로그. JSON 형태의 원본 데이터 포함.",
    "es_query_log": "packetbeat가 수집한 ES 실시간 쿼리 실행 기록.",
    "node_metric":  "ES 노드 리소스 메트릭.",
}


def build_prompt(time_range: TimeRange, logs: list[LogEntry]) -> str:
    grouped: dict[str, list[LogEntry]] = defaultdict(list)
    for log in logs:
        grouped[log.source].append(log)

    sections = []
    for source, entries in grouped.items():
        sampled = entries[:_SAMPLE_SIZE]
        desc    = _SOURCE_DESC.get(source, source)
        lines   = [f"[{source}] ({desc}) - 총 {len(entries)}건 중 {len(sampled)}건 샘플"]
        for e in sampled:
            lines.append(
                f"  {e.timestamp} [{e.level}] node={e.node or '-'} comp={e.component or '-'} {e.message}"
            )
        sections.append("\n".join(lines))

    log_section = "\n\n".join(sections) if sections else "(로그 없음)"

    return f"""분석 시간 범위: {time_range.start} ~ {time_range.end}

=== 로그 데이터 ===
{log_section}

=== 분석 원칙 ===
• slowlog에 기록된 쿼리는 임계치 초과일 뿐, 그 자체로 문제가 아님. 빈도·리소스 영향 등을 종합 판단.
• 실제 이상 징후가 없으면 "특이사항 없음"이라고 명확히 표현. 억지로 문제 만들지 않기.
• FAIL 로그는 일시적 오류인지 반복 장애인지 구분.

=== 알려진 문제 패턴 ===
1. 토큰 분석 이슈: 미분석 토큰 대상 검색이 부하 유발. wildcard/term 쿼리가 analyzed 필드에 사용된 징후.
2. 광범위 범위 검색: 날짜/범위 필터 없는 full scan성 쿼리.

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
