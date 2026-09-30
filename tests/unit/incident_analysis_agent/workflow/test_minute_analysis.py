import json
from datetime import UTC, datetime, timedelta
from itertools import count

import pytest

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.incident_analysis_agent.model.evidence import EvidenceSource
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.graph import run_analysis
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import RawRecord, group_into_buckets
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.schema import MapOutput
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.spec import AnalysisSpec


@pytest.mark.parametrize('reduce_fails', [False, True])
def test_parallel_minute_map_reduce_preserves_original_records(reduce_fails):
    start = datetime(2024, 1, 1, tzinfo=UTC)
    records = [RawRecord(record_id=n, event_time=start + timedelta(minutes=n), line=f'original-{n}', node_name='node') for n in range(2)]
    def llm(messages, tokens, response_format):
        if response_format is MapOutput:
            n = 0 if '#0 ' in messages[0]['content'] else 1
            return json.dumps({'selected':[{'record_id':n,'event_type':'failure'}, {'record_id':99}]})
        if reduce_fails:
            raise LlmApiError('offline')
        return json.dumps({'keep':[{'record_id':1}, {'record_id':0}, {'record_id':99}]})
    sequence = count(1)
    result = run_analysis(AnalysisSpec(source=EvidenceSource.NODE_LOG, label='logs', what_matters='failures', what_is_noise='normal'), group_into_buckets(records), llm, new_evidence_id=lambda:f'E-i-{next(sequence)}')
    assert result.analyzed_minutes == 2
    assert result.failed_minutes == 0
    assert result.reduce_degraded is reduce_fails
    assert [e.raw for e in result.evidence] == ['original-0','original-1']
    assert [e.event_time for e in result.evidence] == [r.event_time for r in records]
    assert len({e.evidence_id for e in result.evidence}) == 2


def test_empty_minute_input_never_calls_llm_or_allocator():
    def forbidden(*args, **kwargs):
        raise AssertionError('unexpected call')
    result = run_analysis(AnalysisSpec(source=EvidenceSource.NODE_LOG, label='logs', what_matters='', what_is_noise=''), [], forbidden, new_evidence_id=forbidden)
    assert result.evidence == []
    assert result.analyzed_minutes == 0
