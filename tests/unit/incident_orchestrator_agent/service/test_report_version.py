import tomllib
from pathlib import Path
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import REPORT_VERSION
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_renderer import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report

ROOT = Path(__file__).resolve().parents[4]


def test_report_version_matches_pyproject():
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']
    assert REPORT_VERSION == project['version'], '/release로 pyproject.toml과 REPORT_VERSION을 함께 올린다'


def test_version_is_shown_in_overview_and_text():
    overview = render_report(report()).split('id="overview"', 1)[1].split('</section>', 1)[0]
    assert f'ClusterDoctor v{REPORT_VERSION}' in overview
    assert f'ClusterDoctor v{REPORT_VERSION}' in render_text(report())
