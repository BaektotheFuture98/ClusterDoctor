"""Create recent, isolated ClickHouse fixture tables; no network or database mutation."""
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST=timezone(timedelta(hours=9))
SCENARIOS=('query-only','master-no-ssh','master-slowlog')
TABLES={'query':'mock_log','metric':'mock_es_node_metric','master':'mock_loki_logs','slowlog':'mock_slowlog_v2'}


def literal(value):
    if isinstance(value,(tuple,list)):
        bracket=('(',')') if isinstance(value,tuple) else ('[',']')
        return bracket[0]+','.join(literal(v) for v in value)+bracket[1]
    if isinstance(value,str):
        return "'"+value.replace('\\','\\\\').replace("'","\\'").replace('\n','\\n')+"'"
    return str(value)


def generate(output_dir,scenario,at):
    if scenario not in SCENARIOS:raise ValueError('Unknown scenario')
    if at.utcoffset() is None:at=at.replace(tzinfo=KST)
    at=at.astimezone(KST).replace(second=0,microsecond=0)
    out=Path(output_dir);out.mkdir(parents=True,exist_ok=True)
    tables={key:value+'_'+scenario.replace('-','_')+'_utc' for key,value in TABLES.items()}
    records={key:[] for key in tables}
    stamp=lambda t:t.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    counts=(8,18,24,16,10,4)
    seq=0
    for minute,count in enumerate(counts):
        for n in range(count):
            seq+=1
            t=at+timedelta(minutes=minute,seconds=n*60/count)
            cmd=('search','agg','count','bulk')[n%4]
            runtime=round(.15+(minute+1)*.12+n*.035,3)
            if minute==2 and n==12:runtime=17.6
            if minute==3 and n==8:runtime=6.55
            target='mock_news_202609,mock_news_202610' if n%2 else 'mock_news_202610'
            dsl={'size':0 if cmd in ('agg','count') else 20+n,'query':{'range':{'in_date':{'gte':20260901,'lte':20261002}}},'track_total_hits':bool(n%2)}
            url='POST http://127.0.0.1:9200/'+target+'/_search\n'+json.dumps(dsl,ensure_ascii=False)
            if cmd=='bulk':url='POST http://127.0.0.1:9200/_bulk\n'+json.dumps({'index':{'_index':'mock_news_202610'}})
            records['query'].append(dict(reg_date=stamp(t),host='mock-client',run_time=runtime,success='Y',cmd=cmd,service='mock-web',env='docker-mock',project=scenario,cluster='elasticsearch',keyword=['반도체','수출','환율','전망','기업'],company='목데이터 회사',user='mock-user',url=url,s_date=20260901,e_date=20261002,date_range=32,search_count=20,etc=f'mock execution {seq}'))
        for node in range(2):
            records['metric'].append(dict(reg_date=stamp(at+timedelta(minutes=minute,seconds=30)),node_name=f'mock-data-{node+1:02}',node_ip='127.0.0.1',os_cpu_percent=(12,18,47,31,22,14)[minute]+node,os_mem_used_percent=60,process_cpu_percent=10,jvm_heap_used_percent=(55,61,77,70,62,58)[minute]+node,search_active=2,search_queue=(0,0,12,4,0,0)[minute],search_rejected=100,write_active=1,write_queue=0,write_rejected=0))
    if scenario!='query-only':
        for minute,logger,line in ((1,'o.e.c.s.MasterService','cluster state update delayed [mock-data-01]'),(2,'o.e.c.r.a.AllocationService','rerouting after shard allocation change'),(3,'o.e.c.s.MasterService','cluster state publication completed')):
            records['master'].append(dict(timestamp=stamp(at+timedelta(minutes=minute,seconds=5)),node='mock-master-01',node_role='master',level='WARN' if minute<3 else 'INFO',detected_level='',logger=logger,filename='/var/log/elasticsearch/mock-master.log',host='127.0.0.1',line=line))
    if scenario=='master-slowlog':
        for n in range(4):
            t=at+timedelta(minutes=2,seconds=30+n)
            records['slowlog'].append({'_source':(stamp(t),(('mock_news_202610',),('mock-data-01',),(f'{1500+n*250}ms','42 hits',2,'service=mock-web,project=docker-mock,env=local,company=mock,user=mock',json.dumps({'size':20,'query':{'match_all':{}}})) ))})
    schema=(Path(__file__).resolve().parents[1]/'docker/clickhouse/init.sql').read_text().split('INSERT INTO slowlog_v2')[0].replace("'Asia/Seoul'", "'UTC'")
    for original,new in [('slowlog_v2',tables['slowlog']),('es_node_metric',tables['metric']),('loki_logs',tables['master'])]:
        schema=schema.replace('CREATE TABLE IF NOT EXISTS '+original, 'CREATE TABLE IF NOT EXISTS '+new)
    schema=schema.replace('CREATE TABLE IF NOT EXISTS log\n','CREATE TABLE IF NOT EXISTS '+tables['query']+'\n')
    sql=[schema]
    for column,typ in [('url','String'),('s_date','UInt32'),('e_date','UInt32'),('date_range','UInt32'),('search_count','UInt32'),('etc','String')]:
        sql.append(f'ALTER TABLE packetbeat.{tables["query"]} ADD COLUMN IF NOT EXISTS {column} {typ};')
    for source,rows in records.items():
        if rows:
            columns=list(rows[0])
            sql.append(f'INSERT INTO packetbeat.{tables[source]} ('+','.join(columns)+') VALUES\n'+',\n'.join('('+','.join(literal(row[c]) for c in columns)+')' for row in rows)+';')
    (out/'seed.sql').write_text('\n\n'.join(sql)+'\n')
    (out/'records.json').write_text(json.dumps(records,ensure_ascii=False,indent=2)+'\n')
    trigger=at+timedelta(minutes=2,seconds=30)
    utc=trigger.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
    (out/'kafka-trigger.json').write_text(json.dumps({'_source':{'@timestamp':utc,'elasticsearch':{'cluster':{'name':'elasticsearch'},'node':{'name':'mock-data-01'},'slowlog':{'took':'17.6s'}}}},ensure_ascii=False,indent=2)+'\n')
    manifest={'scenario':scenario,'start_kst':at.isoformat(),'end_kst':(at+timedelta(minutes=6)).isoformat(),'trigger_at_kst':trigger.isoformat(),'trigger_at_utc':utc,'counts':{k:len(v) for k,v in records.items()},'per_minute_query_counts':list(counts),'tables':tables,'ssh_auto_collection':False}
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    (out/'mock.env').write_text(''.join(f'{key}={tables[source]}\n' for key,source in [('CLICKHOUSE_SLOWLOG_TABLE','slowlog'),('CLICKHOUSE_LOG_TABLE','query'),('CLICKHOUSE_NODE_METRIC_TABLE','metric'),('CLICKHOUSE_NODE_LOG_TABLE','master')]))
    logtime=(at+timedelta(minutes=2,seconds=31)).isoformat(timespec='milliseconds')
    (out/'ssh-data-node.log').write_text(f'[{logtime}][WARN][o.e.m.j.JvmGcMonitorService] [mock-data-01] GC overhead: spent [500ms] collecting in the last [1s]\n    at example.search.QueryPhase.execute(QueryPhase.java:42)\n')
    commands=f'''# ClusterDoctor 저장소에서 실행
# 아래 SQL은 mock_* 테이블에만 추가한다. 재적재하면 중복되므로 다시 생성할 때 새 데이터베이스/테이블 상태를 사용한다.
docker compose up -d
# seed.sql이 있는 디렉터리를 실제 경로로 지정
MOCK_DIR="{out.resolve()}"
docker compose exec -T clickhouse clickhouse-client --user clusterdoctor --password clusterdoctor --multiquery < "$MOCK_DIR/seed.sql"

# .env의 localhost 연결 설정과 LLM API 키가 필요하다. 운영 .env 대신 로컬 설정을 사용한다.
set -a
. "$MOCK_DIR/mock.env"
set +a
export CLICKHOUSE_USER=clusterdoctor CLICKHOUSE_PASSWORD=clusterdoctor
export ES_USER="" ES_PASSWORD=""
# 기존 consumer를 사용 중이면 변경한 환경으로 재시작한다.
.venv/bin/python scripts/run_analysis.py --at "{at.isoformat()}" --count 2 --span 5m --dry-run
# 실제 진단 (ClickHouse/ES/LLM 호출)
.venv/bin/python scripts/run_analysis.py --at "{at.isoformat()}" --count 2 --span 5m

# Kafka 경로: 먼저 .venv/bin/python -m cluster_doctor.main 실행, 다른 터미널에서 같은 mock.env 적용 후:
.venv/bin/python scripts/produce_test_message.py --at "{trigger.isoformat()}"
'''
    (out/'commands.sh').write_text(commands)
    (out/'README.md').write_text(f'''# Docker 목데이터: {scenario}

6분간 실행 로그 80건, 노드 지표 12건, 마스터 {len(records['master'])}건, slowlog {len(records['slowlog'])}건. 최대 실행시간은 17.60초이며 같은 키워드로 서로 다른 조건과 명령을 포함한다. 큐 0과 누적 rejected 100도 포함한다. SQL은 기존 테이블을 수정하거나 삭제하지 않고 별도 mock_*_utc 테이블에 적재한다. 시간 컬럼과 저장 시각은 Docker 서버와 같은 UTC이며, manifest와 분석 입력 시각은 KST다.

commands.sh는 실행 순서를 기록한 안내다. 자동 실행하지 않았다. Docker/ClickHouse/LLM 통합 실행은 별도 확인이 필요하다. seed.sql은 같은 시각에 반복 적재하면 중복된다. 목데이터 생성은 실행 직전에 다시 해야 오래된 이벤트 시간 보정을 피할 수 있다.

마스터 로그와 SSH 결과는 독립적이다. 기존 Compose에는 SSH 서버가 없고 mock-data 노드는 실제 ES 노드가 아니다. 따라서 이 세트의 SSH 섹션은 비어 있는 경우를 확인한다. ssh-data-node.log는 별도 SSH 테스트 서버에 배치할 샘플 파일이며 자동으로 수집되지 않는다. 실모델 선별 결과에 따라 slowlog 근거는 보고서에 없을 수 있다. Kafka 트리거 내용은 ClickHouse 로그를 대신하지 않는다.
''')
    return manifest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario',choices=SCENARIOS,default='master-slowlog')
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--at',help='첫 로그 KST 시각. 생략하면 현재 시각 8분 전.')
    args=parser.parse_args()
    at=datetime.fromisoformat(args.at) if args.at else datetime.now(KST)-timedelta(minutes=8)
    manifest=generate(args.output_dir,args.scenario,at)
    print(json.dumps(manifest,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
