import importlib.util
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT=Path(__file__).resolve().parents[4]

def generator():
    spec=importlib.util.spec_from_file_location('docker_mock',ROOT/'scripts/generate_docker_mock.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_mock_source_presence_and_runtime_inputs_match_manifest(tmp_path):
    start=datetime(2026,10,2,9,0,tzinfo=timezone(timedelta(hours=9)))
    for scenario,masters,slows in [('query-only',0,0),('master-no-ssh',3,0),('master-slowlog',3,4)]:
        dest=tmp_path/scenario
        manifest=generator().generate(dest,scenario,start)
        records=json.loads((dest/'records.json').read_text())
        assert manifest['counts']=={'query':80,'metric':12,'master':masters,'slowlog':slows}
        assert len(records['query'])==80
        assert max(r['run_time'] for r in records['query'])==17.6
        assert {r['cmd'] for r in records['query']} >= {'search','bulk','agg','count'}
        assert records['query'][0]['url'] != records['query'][1]['url']
        assert records['query'][0]['keyword']==records['query'][1]['keyword']
        assert '2026-10-02 00:00:' in records['query'][0]['reg_date']
        assert scenario.replace('-','_') in manifest['tables']['query']
        assert manifest['tables']['query'] in (dest/'mock.env').read_text()
        sql=(dest/'seed.sql').read_text()
        assert 'TRUNCATE' not in sql and 'DROP ' not in sql
        assert 'ADD COLUMN IF NOT EXISTS url' in sql
        assert "DateTime64(3, 'UTC')" in sql
        assert "DateTime64(3, 'Asia/Seoul')" not in sql
        assert manifest['tables']['query'].endswith('_utc')
        trigger=json.loads((dest/'kafka-trigger.json').read_text())
        assert trigger['_source']['@timestamp']==manifest['trigger_at_utc']
