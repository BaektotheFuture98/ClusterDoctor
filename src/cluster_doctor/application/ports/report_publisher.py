from abc import ABC, abstractmethod
from dataclasses import dataclass

from cluster_doctor.domain.analysis.incident_analysis_report import IncidentAnalysisReport


@dataclass(frozen=True)
class ReportPublication:
    """보고서 전달 구현이 반환하는 게시 결과 메타데이터.

    전달할 내용인 IncidentAnalysisReport와 달리 현재는 생성된 텍스트 길이만 담는다.
    """

    text_length: int = 0


class ReportPublisher(ABC):
    """운영자용 IncidentAnalysisReport를 실제 전달 매체로 게시하는 포트.

    애플리케이션은 매체 구현 대신 게시 결과와 분석 누락 여부를 주고받는다.
    """

    @abstractmethod
    async def publish(
        self,
        report: IncidentAnalysisReport,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> ReportPublication:
        """리포트를 운영자에게 전달한다.

        ``gaps``와 ``analysis_failed``를 본문에 섞어 넘기지 않고 따로 받는
        이유: 그것을 어떻게 보여줄지는 표현의 관심사다. 이 코드베이스는 그
        경계를 지킨다(로그 줄을 그리는 ``format_log_line``이 도메인이 아니라
        프롬프트 쪽에 있는 것과 같은 이유다). 문자열에 이어 붙이면 모델이 쓴
        본문과 시스템이 덧붙인 것이 구별되지 않는다.

        기본값을 둔 것은 두 값이 없는 호출을 허용하기 위해서다 — 알림 자체는
        분석 결과의 완전성을 모르고도 성립한다.

        ``report``가 문자열이 아니라 객체인 이유: 리포트에는 코드가 관측한
        사실(분 단위 건수·노드 최대값·마스터 로그·상태 이력)과 모델의 판단이
        함께 실린다. 문자열 하나로 만들면 그 둘을 모델이 옮겨 적어야 하고,
        옮겨 적으면 틀린다 — 실측으로 es_query_log 264건이 slowlog 건수로,
        slowlog의 took이 "미확인"으로 실린 적이 있다.

        구조화가 실패해도 이 타입은 바뀌지 않는다. "모델의 판단이 있는가"는
        ``IncidentAnalysisReport`` 안의 ``narrative``/``narrative_text``가 표현하므로,
        구현체는 언제나 같은 타입을 받고 관측값은 언제나 그릴 수 있다.

        Args:
            report:          리포트. 분석이 실패했더라도 항상 전달된다.
            gaps:            수집하지 못한 보조 근거. 리포트는 유효하다.
            analysis_failed: 분석 자체가 실패했는가. 본문을 신뢰할 수 없다는
                             뜻이므로 눈에 띄게 알려야 한다.
        """
        ...
