from cluster_doctor.incident_analysis_agent.model.log_entries import NodeMetricEntry
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, Observations, merge_node_row
from cluster_doctor.incident_analysis_agent.service.observation.compute import merge_node_rows, node_metric_summary
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links, cross_links_payload
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.cross_links_text import metric_gap_line
from tests.unit.incident_analysis_agent.service.test_correlation import _at


def _sample(node: str, minute: int) -> NodeMetricEntry:
    return NodeMetricEntry(timestamp=_at(minute, 0), node_name=node, node_ip="10.0.0.1", os_cpu_percent=1,
                           os_mem_used_percent=1, process_cpu_percent=1, jvm_heap_used_percent=1, search_active=0,
                           search_queue=0, search_rejected=0, write_active=0, write_queue=0, write_rejected=0)


def _links(samples):
    rows = node_metric_summary(samples)
    return build_cross_links(Observations(nodes=tuple(rows.values())))


def test_summary_records_first_and_last_seen():
    row = node_metric_summary([_sample("a", 5), _sample("a", 1), _sample("a", 9)])["a"]
    assert (row.first_seen, row.last_seen) == (_at(1, 0), _at(9, 0))


def test_merge_takes_min_and_max():
    a = NodeMetricRow(node="a", first_seen=_at(3, 0), last_seen=_at(5, 0))
    b = NodeMetricRow(node="a", first_seen=_at(1, 0), last_seen=_at(4, 0))
    merged = merge_node_row(a, b)
    assert (merged.first_seen, merged.last_seen) == (_at(1, 0), _at(5, 0))
    assert merge_node_row(NodeMetricRow(node="a"), b).first_seen == _at(1, 0)
    acc = {"a": a}
    merge_node_rows(acc, {"a": b})
    assert acc["a"].last_seen == _at(5, 0)


def test_node_that_stops_midway_is_reported():
    samples = [_sample(n, m) for n in ("a", "b") for m in range(0, 10)]
    samples += [_sample("c", m) for m in range(0, 4)]
    links = _links(samples)
    gaps = cross_links_payload(links, {})["node_metric_gaps"]
    assert [g["node"] for g in gaps] == ["c"]
    assert gaps[0]["last_seen"] == _at(3, 0).isoformat()
    assert "지표 공백" in metric_gap_line(links)


def test_node_that_starts_late_is_reported():
    samples = [_sample(n, m) for n in ("a", "b") for m in range(0, 10)] + [_sample("c", m) for m in range(6, 10)]
    assert [g["node"] for g in cross_links_payload(_links(samples), {})["node_metric_gaps"]] == ["c"]


def test_no_gap_when_all_nodes_complete_or_within_tolerance():
    samples = [_sample(n, m) for n in ("a", "b") for m in range(0, 10)]
    samples += [_sample("c", m) for m in range(0, 9)]
    links = _links(samples)
    assert "node_metric_gaps" not in cross_links_payload(links, {})
    assert metric_gap_line(links) == ""


def test_rows_without_timestamps_are_ignored():
    links = build_cross_links(Observations(nodes=(NodeMetricRow(node="a", samples=3),)))
    assert "node_metric_gaps" not in cross_links_payload(links, {})
