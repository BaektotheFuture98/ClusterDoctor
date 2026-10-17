from abc import ABC, abstractmethod
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import ReportPublication

from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)


class ReportPublisher(ABC):
    """운영자용 IncidentAnalysisReport를 실제 전달 매체로 게시하는 포트.

    호출부는 리포트·수집 공백·분석 실패 여부를 넘기고 게시 결과를 받는다.
    """

    @abstractmethod
    async def publish(
        self,
        report: IncidentAnalysisReport,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> ReportPublication:
        """관측값과 모델 판단을 담은 리포트를 게시한다.

        ``gaps``와 ``analysis_failed``는 본문과 분리해 전달하며, 표시 방식은
        구현체가 정한다. 모델 판단이 없거나 검증을 통과하지 못해도 관측값은
        표시할 수 있다. 로컬 저장 경로와 SFTP 경로는 게시 결과에 담는다.

        Args:
            report: 관측값과 선택적인 모델 판단.
            gaps: 수집 공백과 진단 정보. 내부 검증 사유는 본문에서 제외한다.
            analysis_failed: 분석 실패 안내와 관측 기반 대체 표시가 필요한가.
        """
        ...
