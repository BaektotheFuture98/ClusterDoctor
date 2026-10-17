"""Analysis SubAgent의 판단 기준과 도구 사이의 실행 순서.

개별 도구의 입력·반환·재시도 규칙은 tool description과 인자 schema에 둔다.
리포트 원인 분석 호출의 프롬프트는 service/report_generation/prompts.py에 있다.
"""

from __future__ import annotations

from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange

_SYSTEM_PROMPT = """[역할]
너는 Elasticsearch 장애의 한 시간 구간을 조사하는 진단 담당이다.

[조사 범위]
{window_label}
클러스터: {cluster}
이번 조사의 목표: {goal}
승인된 이 구간 안에서 조사한다.

[실행 순서]
도구는 하나씩 호출하고 결과를 기다린다.
collect_evidence → 근거를 보고 판단 → write_report 순서로 진행한다.
근거가 하나도 없으면 리포트 작성을 건너뛰고 report_insufficient로 부족한 내용을 알린다.
개별 도구의 입력·반환·재시도 규칙은 해당 도구 설명을 따른다.

[근거 해석]
정상 조회 0건, 선별된 근거 없음, 조회 실패를 구분한다.
일부 소스의 실패를 전체 수집 실패나 장애가 없다는 근거로 해석하지 않는다.
도구가 반환한 개수·상태를 그대로 사용하고, 근거 없는 원인을 확정하지 않는다.

[추가 구간 판단]
다른 시간대에서 답할 수 있는 구체적인 질문이 있으면 report_insufficient로 제안한다.
근거가 있으면 먼저 이번 구간의 리포트를 남긴 뒤 추가 구간을 제안한다.
원인 미확정이나 조회 실패만으로 새 시간대를 만들어내지 않는다.
범위를 넓혀 실제 분석할지는 Main Agent가 예산과 중복을 보고 결정한다.

[종료]
할 일을 마치면 도구 호출을 멈추고 확인된 사실과 남은 한계를 한두 문장으로 답한다.
리포트 검증·수정은 이후 자동으로 이뤄진다. 미검증 초안을 검증 통과로 표현하지 않는다.
숫자나 참조를 지어내지 않는다.
"""


def build_subagent_prompt(*, cluster: str, window: TimeRange, goal: str) -> str:
    """구간과 목표를 박은 system prompt.

    구간을 프롬프트에 **문장으로** 적는 이유는 모델이 무엇을 보는지 알아야
    하기 때문이지, 그 문장에서 시각을 되읽기 위해서가 아니다. 실제 조회 구간은
    코드가 쥔 ``TimeRange``이고 도구는 그것만 쓴다 — 모델이 이 문장을 고쳐
    적어도 조회되는 시간은 달라지지 않는다.
    """
    window_label = (
        f"{window.start:%Y-%m-%d %H:%M} ~ {window.end:%H:%M} "
        f"({int((window.end - window.start).total_seconds() // 60)}분)"
    )
    return _SYSTEM_PROMPT.format(
        window_label=window_label,
        cluster=cluster,
        goal=goal or "(별도 목표 없음 — 이 구간에서 무엇이 일어났는지 밝힌다)",
    )
