"""분 단위 분석 workflow의 프롬프트.

여기 있는 한국어 문자열이 이 서비스의 산출물 품질을 결정한다. 최종 리포트의
7개 섹션 구성은 스펙에서 온 것이므로 바꾸기 전에 스펙을 먼저 확인할 것.

로그 한 줄을 그리는 것은 ``common.log_format``의 몫이다 — 리포트 렌더러와
같은 함수를 써야 모델이 본 수치와 운영자가 읽는 수치가 갈리지 않는다.
"""

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.common.log_format import (
    format_log_line,
)

_SOURCE_DESC = {
    "slowlog":      "ES 슬로우 쿼리 로그. JSON 형태의 원본 데이터 포함.",
    "es_query_log": "packetbeat가 수집한 ES 실시간 쿼리 실행 기록.",
    "node_metric":  "ES 노드 리소스 메트릭.",
    "node_log":     "ES 노드가 파일에 남긴 로그(WARN/ERROR/GC/shard 이벤트 등).",
}



def build_minute_prompt(minute_logs: list[LogEntry], minute_label: str) -> str:
    """한 구간을 압축하라고 시키는 프롬프트.

    로그를 소스별로 묶되 **샘플링하지 않는다**. 1분치 전량을 보여주는 것이
    이 그래프의 존재 이유다.

    응답 *형식*은 지시하지 않는다. 이 호출에는 ``response_format=MinuteOutput``이
    걸려 있어 스키마가 형식을 강제하기 때문이다. 여기에 "요약:/근거: 형식으로
    답하라"를 더하면 모순된 형식 계약 두 개가 동시에 전달된다 — 스키마 쪽이
    이기므로 그 지시는 효과 없이 구간마다 토큰만 축낸다. 형식 대신 각 필드가
    무엇을 담아야 하는지만 말한다.
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
• os_mem(캐시포함)은 GET _nodes/stats의 os.mem.used_percent다. 페이지 캐시를
  포함한 OS 전체 메모리이고, ES는 남는 RAM을 파일시스템 캐시로 쓰므로
  95~99%가 정상이다. 이 값만 보고 메모리 문제라고 쓰지 않는다.
• jvm_heap 값 자체는 근거가 아니다. ES는 할당된 힙을 채워 쓰는 것이 정상이고,
  이 클러스터는 평시에도 90%대가 흔하다. "몇 % 이상"으로 판단하지 않는다.
  rejected가 0이 아니거나 GC 경고가 함께 있을 때만 메모리를 문제로 쓰고,
  그때도 관찰된 수치를 그대로 인용한다.
• 반드시 한국어로 쓴다.
• 사고 과정·추론 설명은 담지 않는다. 결론만 담는다.

=== 각 필드에 담을 것 ===
summary  이 구간에서 무슨 일이 있었는지. 관찰된 사실만 간결하게.
         원인을 추정하지 않는다 — 원인 추론은 종합 단계가 한다.
evidence 위 요약의 근거가 된 로그를 원문 그대로, 한 줄에 하나씩.
         종합 단계가 이것을 인용해 최종 리포트를 쓰므로, 요약으로 바꿔
         쓰지 말고 원문을 그대로 옮긴다.

         [METRIC] 줄은 **특이사항이 없어도 반드시 하나 싣는다** — 이 구간에서
         jvm_heap이 가장 높은 노드의 줄을 그대로 옮긴다. 정상 범위라는 것도
         종합 단계가 알아야 하는 사실이다. 빠뜨리면 "못 봤다"와 "봤는데
         정상이다"가 구별되지 않고, 최종 리포트에 수치가 하나도 남지 않는다.
         (실제로 그런 리포트가 나왔다 — 상태 섹션이 "green" 네 줄뿐이었다.)

         그 밖의 줄은 특이사항이 없으면 비워 둔다.

         이렇게 쓴다:
           2026-08-27 14:00:02 [SLOWLOG] node=node-a01 comp=app_index_v1 took=32.4s, 68 hits, shards=902, id=service=web,company=1,user=2, query={{"size":0}}
           2026-08-27 14:00:02 [METRIC] node=node-b02 (10.0.0.12) comp=- cpu=1% os_mem(캐시포함)=99% proc_cpu=0% jvm_heap=59% search(active=0,queue=0,rejected=0) write(active=0,queue=0,rejected=0)

         이렇게 쓰면 종합 단계가 인용할 수치가 사라진다:
           node-a01에서 32.4초 걸린 느린 쿼리가 있었다
           노드 메트릭은 정상 범위였다"""


def build_synthesis_prompt(
    time_range: TimeRange,
    minute_sections: str,
    analyzed: int,
    failed: int,
    master_logs: str = "",
) -> str:
    """구간별 결과를 최종 리포트로 합성하라고 시키는 프롬프트.

    원본 로그 전량이 아니라 구간별 요약 + 근거 원문만 들어온다. 전량을 다시
    넣으면 단발 모드와 똑같은 절단이 재발한다.
    master_logs가 있으면 slowlog 분석 결과와 시간 연계해 원인 추론에 활용한다.

    응답은 ``===추론===`` / ``===리포트===`` 두 블록으로 받는다(구획 CoT).
    3번(근본 원인)·4번(소스 간 상관관계)은 구간을 가로지르는 다단계 추론이
    필요한데, 이 프로젝트가 쓰는 모델(gemma-4-31b-it)은 thinking을 지원하지
    않아 추론을 출력으로 받는 수밖에 없다. 추론 블록은 ``nodes.py``의
    ``_strip_reasoning``이 잘라내므로 운영자에게는 리포트만 간다.

    호출을 추론용·리포트용 두 번으로 쪼개는 대안은 쓰지 않는다. 같은 입력을
    두 번 보내 입력 토큰이 2배가 되는데, 트리거 서비스가 큐 잔여 시 10초
    간격으로 최대 4회 연속 실행하므로(``_MAX_CONSECUTIVE_RETRIGGERS=3``) 그
    증가가 다시 곱해진다 — 429의 원인이 분당 입력 토큰 한도 초과다.

    분별 분석(``build_minute_prompt``)에는 걸지 않는다. 요약·인용 작업이라
    추론이 불필요하고, 팬아웃되므로 비용이 구간 수만큼 배로 늘어난다.
    """
    coverage = f"분석된 구간 {analyzed}개"
    if failed:
        coverage += f", 분석 실패 {failed}개"

    master_section = ""
    if master_logs:
        master_section = f"""
=== 마스터 노드 로그 (같은 시간대) ===
{master_logs}

위 로그는 같은 분석 구간의 마스터 노드 ES 로그다(WARN/ERROR/GC/shard 관련 줄만).
slowlog 분석 결과와 시간대를 연계하라. 마스터 이벤트(shard relocation, cluster
state change, allocation 실패 등)가 slowlog 급증 시각과 겹치면 인과관계를 설명한다.
마스터 로그에 특이사항이 없으면 그렇게 명시한다.
"""

    return f"""분석 시간 범위: {time_range.start} ~ {time_range.end}
({coverage})

아래는 이 시간 범위를 1분 단위로 나눠 각각 분석한 결과다. 로그가 없던 구간은
목록에 없다. 이것들을 종합해 하나의 진단 리포트를 작성하라.

=== 구간별 분석 ===
{minute_sections}{master_section}

=== 분석 원칙 ===
• slowlog에 기록된 쿼리는 임계치 초과일 뿐, 그 자체로 문제가 아님. 빈도·리소스 영향 등을 종합 판단.
• 실제 이상 징후가 없으면 "특이사항 없음"이라고 명확히 표현. 억지로 문제 만들지 않기.
• FAIL 로그는 일시적 오류인지 반복 장애인지 구분.
• 구간을 가로질러 반복되거나 번지는 패턴을 우선한다. 한 구간에만 나타난 것과
  여러 구간에 걸친 것을 구분해서 쓴다.
• 근거로 제시된 로그 원문의 쿼리·노드명·수치를 그대로 인용한다.
• os_mem(캐시포함)이 높은 것은 ES의 정상 동작이다(페이지 캐시 포함, 95~99%가 정상).
  구간별 분석이 이것을 문제로 적어 왔더라도 리포트에 문제점으로 옮기지 않는다.
• jvm_heap도 값만으로는 근거가 아니다. 이 클러스터는 평시에도 90%대가 흔하다.
  메모리를 문제로 쓰려면 rejected가 0이 아니거나 GC 경고가 있어야 하고,
  그 근거를 함께 제시한다. 없으면 수치는 적되 "정상 범위"로 쓴다.
• "분석 실패"로 표시된 구간이 있으면 그 사실을 리포트에 밝힌다. 없는 것처럼 쓰지 않는다.

=== 알려진 문제 패턴 ===
아래는 찾아볼 후보이지 결론이 아니다. 주장하려면 해당 쿼리 원문을 근거로 인용한다.
인용할 쿼리가 없으면 그 패턴은 쓰지 않는다.
1. 토큰 분석 이슈: 미분석 토큰 대상 검색이 부하 유발. wildcard/term 쿼리가 analyzed 필드에 사용된 징후.
2. 광범위 범위 검색: 날짜/범위 필터 없는 full scan성 쿼리.

=== 출력 규칙 ===
• 반드시 한국어로만 답한다.
• 아래 두 블록을 이 순서로, 구분자를 한 글자도 바꾸지 않고 출력한다.

===추론===
여기서 먼저 추론한다. 형식은 자유이고 15줄 이내로 압축한다.
  1) 구간을 가로질러 반복되거나 번지는 패턴. 한 구간에만 나타난 것과 구분한다.
  2) 원인 가설을 열거하고, 각 가설을 뒷받침하는 근거와 반박하는 근거를 함께 쓴다.
  3) 마스터 로그·메트릭과 slowlog의 시각이 겹치는 지점.
  4) 위를 종합한 가장 유력한 근본 원인.

===리포트===
여기서부터 최종 리포트만 쓴다. 추론 과정을 다시 설명하지 않는다.
마크다운 금지. 아래 형식 준수:
• 섹션 제목 아래 ── 구분선
• • 불렛, 두 칸 들여쓰기 후 - 세부 내용
• 섹션 사이 빈 줄 두 줄

섹션 구성 (반드시 이 순서로):
1. 요청 이해
2. 분 단위 타임라인
   구간별 분석에 주어진 "건수:" 줄과 [METRIC] 근거를 그대로 옮긴다.
   분마다 한 줄, 시간순. 아래 key=value 형식을 그대로 쓴다:
   HH:MM 건수=<소스별로 그대로> took_max=<값> runtime_max=<값> jvm_heap_max=<%> rejected=<search>/<write> 특이사항=<...>
   건수 칸은 소스마다 갈라 쓴다(slowlog=0 es_query_log=264 node_metric=57).
   한 칸으로 뭉치지 마라. 0인 소스도 0으로 쓴다.
   값이 없는 항목은 "-", 특이사항이 없으면 특이사항=없음. 판단어를 쓰지 않는다.
   건수는 위에 주어진 값을 쓴다. 로그 줄을 직접 세지 않는다.
   [분석 실패]로 표시된 분도 건수와 함께 그 사실을 적는다.
3. 발견된 문제점 (Critical/Warning/Info 분류)
   항목마다 근거(로그 원문 또는 수치와 시각)를 붙인다. 근거를 댈 수 없으면 쓰지 않는다.
4. 근본 원인
   - 근거 / 반박 근거(없으면 "없음") / 확인하지 못한 것(없으면 "없음")
5. 소스 간 상관관계
   상관을 주장하려면 양쪽의 시각을 함께 인용한다.
   시각이 겹치지 않으면 "시각이 겹치지 않음 — 상관관계 근거 없음"이라고 쓴다.
6. 문제 쿼리 후보 (+ 사용자 정보 파악(user, company))
   최종 리포트의 후보 목록은 코드가 id와 수치를 붙여 따로 싣는다. 여기서는
   이 구간에서 무엇이 눈에 띄었는지만 쓴다.
7. 권장 조치
8. 클러스터 건강 상태"""
