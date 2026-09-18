"""datasource 하나를 Triage 그래프에 물리는 방법.

그래프는 slowlog도 마스터 로그도 모른다. 아는 것은 "레코드 목록을 받아 분으로
쪼개고, 분마다 골라내고, 전체를 다시 훑어 남긴다"는 절차뿐이다. datasource마다
다른 것 — 무엇이 의미 있는 이벤트인가, 한 줄을 어떻게 그리는가 — 은 전부
``TriageSpec``에 담아 주입한다.

Strategy 패턴을 쓰는 자리가 여기인 이유: datasource는 실제로 늘어난다(지금
넷, 앞으로 더). 절차가 같고 판단 기준만 다르다면 그것이 바로 전략이다.
"""

from __future__ import annotations

from dataclasses import dataclass

from cluster_doctor.application.service.guardrails import MAX_EVIDENCE_PER_SOURCE
from cluster_doctor.domain.model.evidence import EvidenceSource


@dataclass(frozen=True)
class TriageSpec:
    """한 datasource의 Triage 설정.

    ``what_matters``와 ``what_is_noise``를 나눠 받는 이유: 모델에게 "중요한 것을
    고르라"만 주면 전부 중요해진다. 실측에서 마스터 로그 INFO 10건 중 진단에
    필요한 것은 1건이었고 나머지 9건은 ML 유지보수·만료 데이터 삭제였다.
    무엇을 버릴지 명시해야 볼륨이 내려간다.
    """

    source: EvidenceSource
    # 프롬프트와 로그에 쓰는 사람이 읽을 이름.
    label: str
    # 이 소스에서 후속 장애 분석에 의미 있는 것.
    what_matters: str
    # 이 소스에서 정상이거나 반복이라 버려야 하는 것.
    what_is_noise: str
    max_evidence: int = MAX_EVIDENCE_PER_SOURCE
