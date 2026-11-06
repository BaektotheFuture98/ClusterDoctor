"""대조 0건(UNCHECKED)의 표시와 로그 문구."""
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.validation import UNVERIFIABLE_PREFIX
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport, Narrative
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import (
    is_validation_diagnostic, result_caveats, verification_gap, withheld_reason)


def _report(status):
    return IncidentAnalysisReport(observations=Observations(), narrative=Narrative(), verification_status=status,
                                  cluster='es')


def test_UNCHECKED면_해석을_숨기고_대조_못_함_문구를_쓴다():
    assert withheld_reason(_report('UNCHECKED')) == 'AI 분석을 원문과 대조하지 못해 관측값만 표시합니다.'
    assert result_caveats(_report('UNCHECKED')) == ()


def test_기존_판정의_표시는_그대로다():
    assert withheld_reason(_report('PASSED')) == ''
    assert withheld_reason(_report('NOT_VERIFIED')) == ''
    assert 'OK' not in withheld_reason(_report('MISMATCH')) and withheld_reason(_report('MISMATCH'))


def test_verification_gap_접두어를_구분한다():
    assert verification_gap(f'{UNVERIFIABLE_PREFIX}호출 실패') == f'리포트 검증 미완료: {UNVERIFIABLE_PREFIX}호출 실패'
    assert verification_gap('summary: 수치가 다르다') == '리포트 검증 불일치: summary: 수치가 다르다'
    assert is_validation_diagnostic(verification_gap(f'{UNVERIFIABLE_PREFIX}x'))
    assert is_validation_diagnostic(verification_gap('x'))
