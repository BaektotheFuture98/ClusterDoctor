# ClusterDoctor

ClusterDoctor는 Kafka의 Elasticsearch slowlog 트리거를 받아 유입이 멎기를 기다린 뒤,
인시던트 하나를 진단해 HTML 보고서 한 장을 만든다. 요청을 받는 HTTP 서비스가 아니라 계속
실행되는 Kafka consumer이며, HTTP diagnosis endpoint는 없다.

## 동작과 아키텍처

```text
ES slowlog → Filebeat → Elasticsearch data stream → Kafka source connector
                                                   ├→ ClusterDoctor trigger
                                                   └→ ClickHouse slowlog_v2
```

같은 connector output이 두 갈래로 가므로, 트리거가 도착할 때 ClickHouse에는 대체로 분석할
데이터가 들어와 있다. Kafka 수신부터 최종 보고서까지의 흐름은 다음과 같다.

```text
KafkaConsumerAdapter
  → SlowlogHandler port
  → SlowlogIntake: micro-batch, quiet-period settling, retrigger policy
  → StartIncident
  → DiagnoseIncident: state, timeout/cancellation, delivery, cleanup
  → IncidentAnalyzer port: DeepAgents composite adapter
  → application report finalization and HTML publication
```

`SlowlogIntake`는 `MICRO_BATCH_SECONDS` 동안 도착분을 묶고, 15초 간격으로 유입을
확인해 연속 두 번 신규 항목이 없을 때 분석을 시작한다. 한 번의 대기는 최대 60초, 누적
대기는 최대 5분이다.

의존성은 안쪽으로만 향한다.

```text
inbound adapters → application → domain ← outbound adapters
                         ↑
                    bootstrap composition
```

도메인은 incident 상태, guardrail, time range, evidence, 보고서 타입, 순수
`merge_window_reports` 정책을 가진다. application은 port와 use case를 조정하며 Kafka,
LangChain, LangGraph, DeepAgents, LiteLLM을 import하지 않는다. `bootstrap`만 구현체를
선택한다.

### Port와 adapter

Application port는 `SlowlogHandler`, `IncidentAnalyzer`, `IncidentStateRepository`,
`ArtifactStore`, `LogRepository`, `ClusterRepository`, `NodeResolver`, `NodeLogFetcher`,
`ReportPublisher`다.

- inbound Kafka adapter는 record를 `SlowlogTrigger`로 파싱해 `SlowlogHandler`에 넘긴다.
  `aiokafka` 의존성은 이 adapter에만 있다.
- ClickHouse는 `LogRepository`, Elasticsearch는 `ClusterRepository`와 `NodeResolver`,
  SSH는 `NodeLogFetcher`, persistence는 state/artifact port, HTML reporting은
  `ReportPublisher`를 구현한다.
- `adapters.outbound.deepagents`는 하나의 composite outbound adapter다. 공개 API는
  `DeepAgentsConfig`와 `build_deepagents_incident_analyzer`뿐이다. factory는 설정과
  application port 객체를 받아 structured LiteLLM caller, `ReportWriter`, metric
  threshold, `DiagnosisSeams`, private `_DeepAgentIncidentAnalyzer`를 내부에서 조립한다.
  bootstrap은 runtime, supervisor, diagnosis, pipeline internals를 import하지 않는다.

Main DeepAgent는 incident마다 새 graph를 만들고 tool loop를 돈다. 먼저 후보 window를
열거하고, 허용된 window를 제안한 뒤, 진단 subagent에 task로 위임하고, report를 finalize한
후 incident를 끝낸다. 시간·분·호출 상한은 prompt가 아니라 runtime guardrail이 강제한다.
Subagent는 window마다 evidence를 모으고 report를 작성·검증해 injected port로 중간 산출물을
저장한다.

window report 병합은 `domain.incident.report_merge.merge_window_reports`의 순수 정책이다.
I/O는 application `finalize_incident_report` service가 맡는다. service가 `ArtifactStore`에서
window report를 읽어 병합하고 final report를 저장하며, DeepAgents finalization tool과
`DiagnoseIncident` fallback이 모두 이 service를 사용한다. Candidate를 prompt에 보이는 방식과
HTML에 보이는 방식은 각 adapter 안에서 따로 projection한다.

## 분석 모델

Supervisor는 raw log를 보지 않는다. Agent 사이에는 request/response와 reference만 흐르고,
evidence·원문·report·observation은 `ArtifactStore`에 둔다. 그래야 supervisor context가 raw
log로 불어나지 않는다.

각 datasource는 raw log를 1분 bucket으로 나눠 Map/Reduce로 evidence를 고른다. 모델은 번호와
선택 이유만 돌려주며 timestamp, node, 원문은 code가 원본 record에서 옮긴다. Reduce는 root
cause를 확정하지 않는다. 모든 datasource evidence가 모인 뒤 cross-source 단계가 원인 후보를
다룬다. 비어 있는 분은 호출하지 않고, 실패한 분은 `[분석 실패]`와 gap으로 남긴다.

노드 metric은 LLM을 거치지 않는다. heap, queue, rejected의 임계값을 넘은 최고점을 code가
evidence로 만든다. master node log는 ClickHouse에서 매 window 읽고, data node log는 master
evidence로 후보가 생겼을 때만 `NodeResolver`와 SSH를 통해 읽는다.

## 보고서

Incident 하나당 `REPORT_DIR` 아래 HTML 파일 하나를 쓴다. `DiagnosisReport`는 code가 센
`observations`와 모델이 쓴 `narrative`를 분리한다. 숫자와 slow-query candidate 값은 code가
정확히 아는 값이므로 모델에게 옮겨 적게 하지 않는다. Narrative의 주장에는 evidence reference가
붙고, verification은 없는 reference, evidence 없는 주장, candidate/time/node 불일치, 순서,
과도한 확신, 인과 역전을 검사한다.

모델이 빈 draft를 주거나 report 저장이 실패해도 observation은 전달한다. HTML 파일을 쓸 수
없으면 scrubbed plain text를 log로 남긴다. 모든 text는 escape하며 report 파일은 query 원문과
company/user 식별자를 담을 수 있으므로 `reports/`는 Git에 넣지 않는다.

## 요구 사항

- Python 3.13 이상과 [uv](https://docs.astral.sh/uv/)
- slowlog topic을 전달하는 Kafka
- slowlog, query log, node metric, node log table을 가진 ClickHouse
- cluster health 조회용 Elasticsearch
- Gemini 또는 NVIDIA NIM API key 하나
- 선택: data-node log를 읽을 SSH credential

## 설정

환경 변수 또는 project root `.env`를 `src/cluster_doctor/config/settings.py`가 읽는다.
`.env.example`을 `.env`로 복사하고 실제 key는 commit하지 않는다. 필수 설정이 없으면 첫
slowlog까지 기다리지 않고 기동 시점에 실패하며, 오류는 secret value를 출력하지 않는다.

| 변수 | 기본값 | 설명 |
|---|---:|---|
| `LLM_PROVIDER` | `gemini` | `gemini` 또는 `nvidia_nim` |
| `GEMINI_API_KEY`, `NVIDIA_API_KEY` | — | 선택한 provider의 key |
| `GEMINI_MODEL` | `gemini-3.5-flash-lite` | Gemini model |
| `NVIDIA_MODEL` | `google/gemma-4-31b-it` | NIM model; local cost map에 없어 cost가 0일 수 있음 |
| `CLICKHOUSE_URL` | — | 예: `jdbc:clickhouse://host:8123/packetbeat`; 이 database에 아래 네 table이 있어야 함 |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | `default`, empty | ClickHouse credential |
| `CLICKHOUSE_SLOWLOG_TABLE`, `CLICKHOUSE_LOG_TABLE` | `slowlog_v2`, `log` | slowlog와 query log table |
| `CLICKHOUSE_NODE_METRIC_TABLE`, `CLICKHOUSE_NODE_LOG_TABLE` | `es_node_metric`, `loki_logs` | node metric과 master-node log table |
| `ES_HOST`, `ES_PORT` | —, `9200` | comma-separated host와 port |
| `ES_USER`, `ES_PASSWORD` | empty | 비면 basic auth를 사용하지 않음 |
| `SSH_USER`, `SSH_PASSWORD`, `SSH_PORT` | empty, empty, `22` | data-node log용 SSH |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID` | `localhost:9092`, `slowlog`, `clusterdoctor` | consumer 설정 |
| `MICRO_BATCH_SECONDS`, `CLUSTER_NAME` | `10`, `elasticsearch` | batch interval과 표시 이름 |
| `NODE_HEAP_WARN_PERCENT`, `NODE_QUEUE_WARN` | `85`, `100` | metric evidence threshold |
| `REPORT_DIR` | `reports` | Incident HTML output directory |
| `LITELLM_LOCAL_MODEL_COST_MAP` | `True` | litellm의 GitHub cost-map fetch를 막음 |

Kafka offset은 `(group, topic, partition)` 기준이다. 새 topic에 committed offset이 없으면
`auto_offset_reset=latest`로 끝에서 시작한다.

### 고정 한도

다음은 환경 변수가 아닌 runtime 상수다. 위치는 현재 hexagonal package 구조를 기준으로 한다.

| 한도 | 값 | 위치 |
|---|---:|---|
| analysis window | 10분 | `domain/diagnosis/time_range.py` |
| incident analysis budget | 60분, 12회 | `domain/incident/guardrails.py` |
| supervisor cycle / rejected decision | 16회 / 3회 | `domain/incident/guardrails.py` |
| report revision | 1회 | `domain/incident/guardrails.py` |
| settling wait | 60초 each, 300초 total | `domain/incident/guardrails.py` |
| incident timeout | 30분 | `domain/incident/guardrails.py` |
| evidence | source당 25, 전체 80 | `domain/incident/guardrails.py` |
| raw prompt text | 60,000자 | `domain/incident/guardrails.py` |
| node investigation | 2 nodes | `adapters/outbound/deepagents/diagnosis/pipeline/node_investigation.py` |
| consecutive retrigger | 3회 | `application/use_cases/slowlog_intake.py` |
| source query rows | source·minute당 10,000 | `adapters/outbound/clickhouse/reader.py` |
| master log | window당 300, report당 120 | `adapters/outbound/deepagents/diagnosis/pipeline` |
| ClickHouse node-log | default 300, hard cap 2,000 | `application/ports/log_repository.py` |
| SSH node-log | default 300 | `application/ports/node_log_fetcher.py` |
| SSH timeout | connect 10초, command 30초 | `domain/incident/guardrails.py` |
| LLM timeout | 120초 | `adapters/outbound/deepagents/runtime/litellm_client.py` |
| ClickHouse timeout | 30초 | `bootstrap/dependencies.py` |

### 재시도와 rate limit

LLM 호출은 `litellm.completion(num_retries=0)` 경로만 쓴다. 가장 흔한 실패는 429이고, 같은 큰
prompt를 재시도해도 quota를 회복하지 못하며 token 소비만 증가한다. Gemini free tier의
분당 입력 token 250,000에 대해 5분 window가 약 513,122 input token을 쓴 실측이 있다. retry와
retrigger가 곱해지면 이 비용이 더 커진다.

client는 status와 whitelist된 rate-limit header만 log로 남긴다. response body, request URL,
`str(exc)`는 key를 포함할 수 있어 기록하지 않는다. retry를 끈 대가로 일시적인 5xx도 자동
retry하지 않지만, failed minute은 전체 incident를 버리지 않고 gap으로 보고된다.

## 설치와 실행

```bash
uv sync
uv run python -m cluster_doctor.main
```

Kafka consumer는 block하며 log는 stderr와 `logs/app.log`, report는 `REPORT_DIR`에 남긴다.
실제 slowlog를 기다리지 않는 trigger 확인에는 다음을 쓴다.

```bash
uv run python scripts/produce_test_message.py --at "2026-09-16T04:22:00" --count 3
```

Kafka와 settling을 건너뛰고 같은 `DiagnoseIncident` path를 특정 시각에 실행하려면
`run_diagnosis.py`를 쓴다. 아래 첫 명령은 **preflight only**이며 moment와 range만 계산하고
`RunManualDiagnosis`를 호출하지 않는다. 두 번째 명령이 실제 ClickHouse, Elasticsearch, SSH,
LLM을 호출하는 direct diagnosis다.

```bash
# dry-run preflight: no RunManualDiagnosis, no external diagnosis calls
uv run python scripts/run_diagnosis.py --at "2026-09-16T04:22:00" --span 6m --dry-run

# actual direct diagnosis: invokes RunManualDiagnosis
uv run python scripts/run_diagnosis.py --at "2026-09-16T04:22:00" --span 6m
```

## 데이터 소스와 metric 해석

| table | time column | 내용 |
|---|---|---|
| `slowlog_v2` | `_source.@timestamp` | slow query index, node, took, hits, shards, query, opaque-id fields |
| `log` | `reg_date` | ES query execution host, duration, success, command, keyword, company, user |
| `es_node_metric` | `reg_date` | CPU, OS memory, JVM heap, search/write queue와 rejected |
| `loki_logs` | `timestamp` | master node log line, role, level, logger, filename, host |

시간은 KST-aware value로 읽는다. `slowlog_v2`는 ClickHouse ingest 시각인 `ch_ingested_at`이
아닌 Elasticsearch event time인 `_source.@timestamp`로 filter한다. ingest delay가 분 경계를
넘으면 trigger를 만든 원본 slowlog 자체를 빠뜨릴 수 있기 때문이다. `slowlog_v2`와 `loki_logs`는
평시 0건이 정상인 incident-oriented log다.

`os_mem`은 `_nodes/stats`의 `os.mem.used_percent`로 page cache를 포함한다. Elasticsearch는
남는 RAM을 cache로 쓰므로 95–99% 자체는 memory pressure가 아니다. pressure는 `jvm_heap`,
GC warning, `search_rejected`와 함께 판단한다. `_source` document는 필요한 named JSON field만
선택해 prompt token을 줄인다.

## 배포 메모

최초 `import litellm`은 tiktoken `cl100k_base` encoding을
`openaipublic.blob.core.windows.net`에서 받을 수 있다. cache가 비어 있는 network-isolated
환경에서는 import가 지연된 뒤 `ProxyError`로 process가 시작하지 못할 수 있다. image build
중 cache를 데우면 된다.

```dockerfile
RUN python -c "import litellm"
```

`LITELLM_LOCAL_MODEL_COST_MAP=True`는 별도의 GitHub cost-map fetch만 막고 tiktoken fetch에는
영향이 없다. litellm image 크기를 줄일 때 proxy experimental output·swagger 같은 사용하지 않는
asset만 제거하고, `litellm/proxy/` 전체는 실제 `completion()` 경로가 사용하므로 제거하지 않는다.

## 알려진 한계

- 무료 tier token quota에는 여전히 닿을 수 있다. prompt 크기 자체를 줄이지 않으면 retry를
  막아도 busy window가 quota를 넘는다.
- 모델 narrative field는 실행마다 비어 있을 수 있다. 구조는 빈 narrative에도 observation과
  gap을 전달하지만, 모델 판단의 근본 해결은 아니다.
- settling이 queue를 비운 뒤 실행이 심하게 실패하면 그 trigger는 재trigger되지 않을 수 있다.
- master log 0건과 아직 ingest되지 않음을 구별하지 못한다. SSH fallback은 ClickHouse query가
  실패한 경우에만 동작한다.
- data-node log는 SSH credential과 network reachability에 의존한다. Elasticsearch `_nodes`가
  내부 주소를 돌려주면 외부 실행 host의 SSH는 timeout 뒤 gap으로 남는다.

## 검증

```bash
uv run pytest -q tests/architecture/test_dependency_rules.py
uv run pytest -q
git diff --check
uv run python -c "import cluster_doctor; import cluster_doctor.bootstrap.dependencies"
```
