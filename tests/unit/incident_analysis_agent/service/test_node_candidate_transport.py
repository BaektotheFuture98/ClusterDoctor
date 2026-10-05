from datetime import datetime, timezone
import json
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.service.node_investigation.node_investigation import find_problem_nodes


def test_node_selection_json_object_preserves_reference_filter_and_limit():
    evidence=Evidence(evidence_id='E1',event_time=datetime.now(timezone.utc),source=EvidenceSource.MASTER_LOG,message='node-A node-B disconnected')
    seen=[]
    def call(messages,**kwargs):
        seen.append((messages,kwargs))
        return json.dumps({'candidates':[{'node_id':'node-A','reason':'확인 필요','evidence_refs':['fake']},{'node_id':'node-B','reason':'연결 확인','evidence_refs':['E1']},{'node_id':'node-A','reason':'연결 확인','evidence_refs':['E1']}]})
    result=find_problem_nodes([evidence],call,limit=1)
    assert seen[0][1]['response_format']=={'type':'json_object'}
    assert '"candidates"' in seen[0][0][0]['content']
    assert len(result)==1 and result[0].node_id=='node-B'
    assert result[0].evidence_refs==('E1',)
