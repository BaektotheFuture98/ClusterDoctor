from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import NodeMetricEntry
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, merge_node_row
from cluster_doctor.incident_analysis_agent.service.observation.compute import node_metric_summary


def _metric(node: str, ip: str, cpu: int = 10) -> NodeMetricEntry:
    return NodeMetricEntry(
        timestamp=datetime(2026, 10, 11, 19, 3, tzinfo=KST), node_name=node, node_ip=ip,
        os_cpu_percent=cpu, os_mem_used_percent=0, process_cpu_percent=0, jvm_heap_used_percent=50,
        search_active=0, search_queue=0, search_rejected=0,
        write_active=0, write_queue=0, write_rejected=0,
    )


def test_node_metric_summary_keeps_node_ip():
    rows = node_metric_summary([_metric("RC6-18", "192.168.0.143"), _metric("RC6-18", "192.168.0.143", cpu=12)])
    assert rows["RC6-18"].node_ip == "192.168.0.143"
    assert rows["RC6-18"].cpu_max == 12


def test_merge_node_row_fills_missing_ip():
    merged = merge_node_row(NodeMetricRow(node="RC6-18"), NodeMetricRow(node="RC6-18", node_ip="192.168.0.143"))
    assert merged.node_ip == "192.168.0.143"
