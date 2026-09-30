"""분석 리포트 한 건의 최종 형태.

리포트가 문자열 하나이면 모델이 평문을 쓰고 notifier가 정규식으로 훑어야 한다.
그 경로는 실측에서 두 번 틀렸다.

  1. 타임라인이 ``slowlog=264``라고 썼는데 그 구간의 실제 slowlog는 0건이고
     264는 ``es_query_log`` 건수였다. 코드는 소스별 건수를 정확히 세어
     넘겼지만(``TimelineRow.counts``) 프롬프트의 줄 형식에 소스 칸이 하나뿐이라
     모델이 비어 있지 않은 숫자를 그 칸에 넣었다.
  2. "문제 쿼리 후보"의 ``took``·``total_hits``가 전부 ``미확인``이었다. 그 값은
     ``SlowlogEntry``에만 있는데 해당 구간 slowlog가 0건이라 모델이
     ``es_query_log`` 항목을 고르고 칸을 채우지 못했다.

둘 다 뿌리가 같다 — **코드가 이미 아는 값을 모델이 옮겨 적게 시켰다.**
이 코드베이스는 같은 위험 때문에 ``TimelineRow.counts``를 코드가 세게 했고
(*"모델이 옮겨 적다 틀리면 운영자가 잘못된 건수를 근거로 판단한다"*),
``gaps``도 코드가 기록하게 했다. 이 모듈은 그 원칙을 최종 리포트까지 밀어
올린다.

경계가 둘이다.

  ``Observations``  코드가 관측한 사실. 모델을 거치지 않는다.
  ``Narrative``     모델의 판단. 근거는 관측값에서 인용한다.

``Narrative``가 ``None``일 수 있는 것이 중요하다. 구조화 출력이 실패해도
관측값 섹션은 그대로 렌더되어야 한다 — 이 저장소의 "리포트는 항상 전달된다"
원칙이 그것을 요구한다. 그래서 포트는 합집합 타입(``IncidentAnalysisReport | str``)을
갖지 않는다. "구조화됐는가"는 이 객체 **안에서** 표현된다.

이 전달 모델은 dataclass로 표현한다. 모델 응답 스키마 ``DraftReport``는
``incident_analysis_agent/service/report_generation/schema.py``에 있고,
Analysis Agent가 먼저 ``LogAnalysisReport``로 변환한다. 이후
``report_delivery/projection/output_mapping.to_incident_analysis_report``가
관측값과 근거를 결합해 이 모듈의 ``IncidentAnalysisReport``를 만든다. 모델
응답 규약과 운영자 전달 형태의 경계다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import SuspectPick


@dataclass(frozen=True)
class Finding:
    """모델이 지목한 문제 하나.

    ``severity``가 필드인 것이 요점이다. 모델이 ``"Critical: …"``처럼 본문에
    섞어 쓰게 두면 notifier가 정규식으로 앞머리를 떼야 하고, 본문에 우연히 들어간
    "Info"까지 배지로 승격된다.

    비어 있을 수 있다. 그때는 렌더러가 "모델이 분류하지 않음"으로 그린다 —
    코드가 대신 채우지 않는다. 이 필드는 **모델의 판단**이고, 관측값에서
    따라 나오는 심각도는 ``observed_severity``가 따로 낸다.
    """

    severity: str
    title: str
    evidence: tuple[str, ...] = ()


@dataclass(frozen=True)
class Narrative:
    """모델의 판단.

    관측값은 하나도 담지 않는다. 담으면 모델이 옮겨 적어야 하고, 옮겨 적으면
    틀린다 — 관측값과 최종 출력의 경계를 분리한다.
    """

    headline: str = ""
    context: tuple[str, ...] = ()
    findings: tuple[Finding, ...] = ()
    root_cause: str = ""
    supporting: tuple[str, ...] = ()
    contradicting: tuple[str, ...] = ()
    unverified: tuple[str, ...] = ()
    suspect_picks: tuple[SuspectPick, ...] = ()
    recommendations: tuple[str, ...] = ()


@dataclass(frozen=True)
class TimelineAnnotation:
    """검증 전 모델 타임라인의 구조를 잃지 않고 전달하는 값.

    표시 가능 여부는 최종 리포트의 ``verification_status``와 실제 Evidence
    시각을 함께 보는 projector가 결정한다. 여기서 문자열로 평탄화하면 그
    검증을 다시 수행할 수 없다.
    """

    at: datetime
    description: str
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class IncidentAnalysisReport:
    """운영자에게 전달되는 리포트 한 건.

    ``narrative``와 ``narrative_text``는 배타적이다.

      narrative 있음        구조화 출력 성공. 정상 경로.
      narrative_text 있음   구조화는 실패했지만 모델이 평문은 남겼다.
                            notifier가 기존 정규식 파서로 그린다.
      둘 다 없음            모델이 아무것도 남기지 못했다. 관측값만 그린다.

    셋 중 어느 경우든 ``observations``는 그대로 렌더된다. 그것이 이 설계의
    요점이다 — 모델이 실패해도 운영자는 그 시각에 무슨 일이 있었는지 본다.
    """

    observations: Observations
    narrative: Narrative | None = None
    narrative_text: str = ""
    evidence: tuple[Evidence, ...] = ()
    timeline_annotations: tuple[TimelineAnnotation, ...] = ()
    verification_status: str = "NOT_VERIFIED"
