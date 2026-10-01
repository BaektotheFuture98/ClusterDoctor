from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, Observations, observed_severity


def test_lifetime_rejected_is_not_current_critical():
    level,reasons=observed_severity(Observations(nodes=(NodeMetricRow(node='data',samples=1,search_rejected_max=900),)))
    assert level!='Critical'
    assert not any('900건' in r for r in reasons)
