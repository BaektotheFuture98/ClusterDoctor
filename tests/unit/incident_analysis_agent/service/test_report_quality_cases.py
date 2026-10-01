import importlib.util
from pathlib import Path

ROOT=Path(__file__).resolve().parents[4]

def test_ten_offline_cases_have_no_false_protocol_passes():
    spec=importlib.util.spec_from_file_location('evaluate',ROOT/'scripts/evaluate_report_prompts.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    result=module.evaluate(mode='offline',cases_path=ROOT/'tests/fixtures/report_quality/cases.json')
    assert result['case_count']==10 and result['contract_failures']==0
    assert result['false_passes']==0
    assert result['live_model_evaluation']=='not_run'
    bulk=next(c for c in result['cases'] if c['id']=='bulk_rank')
    assert bulk['execution_count']==3 and bulk['maximum_seconds']=='1.96' and bulk['ranked_cmds']==['search','bulk','search']
