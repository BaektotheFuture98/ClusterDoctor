import json
from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry, query_record_key
from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent, Observations
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import build_analysis_context

FOLLOWER = (
    "Received response for a request that has timed out, sent [13.4s/13415ms] ago, "
    "timed out [3.4s/3402ms] ago, action [internal:coordination/fault_detection/follower_check], "
    "node [{RC6-18}{Vt2xoFlXTDmYm-BHlsgvBg}{JULUWPt3SymLu6RSbm2GyQ}{RC6-18}{192.168.0.143}"
    "{192.168.0.143:9300}{d}], id [1]"
)


def test_context_carries_cross_links_with_evidence_ids():
    query = QueryLogEntry(
        reg_date=datetime(2026, 10, 11, 19, 3, 28, tzinfo=KST), host="h", run_time=Decimal("10.88"),
        success="Y", s_date=0, e_date=0, date_range=365, keyword=(), target_host="192.168.1.33",
        index_name="a", conditions=(), cmd="agg", service="", env="", project="", company="",
        user="", search_count=0, etc="", cluster="",
    )
    evidence = Evidence(evidence_id="E7", event_time=query.reg_date, source=EvidenceSource.QUERY_LOG,
                        message="run_time=10.88", record_key=query_record_key(query))
    observations = Observations(
        query_requests=(query,),
        master_events=(MasterEvent(timestamp=datetime(2026, 10, 11, 19, 3, 37, 967471, tzinfo=KST),
                                   node="RC13-03", level="WARN", logger="o.e.t.TransportService",
                                   line=FOLLOWER),),
    )
    data = json.loads(build_analysis_context(observations, [evidence]))["cross_links"]
    [overlap] = data["master_wait_overlaps"]
    assert overlap["target_node"] == "RC6-18"
    assert overlap["overlap_under_both_assumptions"][0]["evidence_id"] == "E7"
    assert data["minutes"][0]["master_waits"] == 1
    assert data["verdict"]["scope"] == "undetermined"
