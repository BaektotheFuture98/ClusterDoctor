# 주요 타입과 진단 흐름

폴더 위치는 구현 기술이 아니라 역할과 owner를 따른다. `model/`은 owner 안의
여러 component가 공유하는 구조화된 데이터다. State는 실행 owner 가까이,
특정 LLM 응답 schema는 그 service/workflow 가까이에 둔다.

## 실행 State

`main.compose_analysis_service(settings, resources)`가 datasource adapters, model,
`AnalysisSeams`, `AnalysisAgent`, `AnalysisSubagentAdapter`, `MainAgent`, `AnalyzeIncident`를 직접 만든다.
공개 `MainAgent(model=..., seams=..., analysis_subagent=...)`와
`AnalysisAgent(model=..., seams=...)`는 같은 model·seams를 주입받고,
`AnalysisSubagentAdapter(analysis_agent=...)`는 생성한 AnalysisAgent를 주입받는다.
`MainAgent`는 window 위임 이후 incident 전체 리포트 합성에도 seams를 사용한다.
같은 `main.py`가 `build_runtime_resources(settings)`로 공유 client를 만들고,
`build_report_publisher(settings)`와 `build_sftp_uploader(settings)`로 report/SFTP를 조립한다.
`bootstrap/resources.py`의 `RuntimeResources`는 client 종료를 맡으며,
생성 중 일부 client만 만들어지면 `build_runtime_resources`의 `ExitStack`이 정리한다.
환경 설정은 `bootstrap/settings.py`가 읽고, bootstrap은 이 두 모듈로 구성한다.

공용 chat model은 `incident_orchestrator_agent/agent/chat_model.py`에 둔다.
Analysis의 `agent/runtime/`에는 `harness.py`, `litellm_client.py`,
`rate_limit_retry.py`, `pseudonym.py`, `llm_call_log.py`, `tool_batch.py`가 있으며,
Main·Analysis가 필요한 harness 제한·LLM 호출·재시도·가명화·로그·도구 실행 제어를 공유한다.

실행 객체·model·seams와 공유 client는 실행 시작 시 한 번 생성한다. Main graph와 State,
`AnalysisSubagentAdapter.bind(incident)`의 delegate는 Incident마다 생성하며,
Analysis graph와 State는 분석 window마다 생성한다. 공유 어댑터는 Incident와 실행 State를
보관하지 않는다. 모델 검증·harness 등록은 Kafka consumer 생성 전에 완료하고,
조립 자체는 LLM 추론을 실행하지 않는다.
독립 Analysis 실행에서도 harness 제한을 확인하며, 이미 적용되어 있으면 재등록하지 않는다.

| 실행 owner | 유일한 mutable source of truth | 생성과 종료 |
| --- | --- | --- |
| Main DeepAgent | `incident_orchestrator_agent/agent/state.py: MainAgentState` | 입력 DTO에서 초기화 → tool/middleware update → 결과 DTO |
| Analysis DeepAgent | `incident_analysis_agent/agent/state.py: AnalysisAgentState` | 위임마다 새 요청에서 초기화 → tool/after_agent update → WindowAnalysisResult |
| Minute LangGraph | `workflow/minute_analysis/state.py: MinuteAnalysisState` | datasource/window 호출마다 초기화 → map/reduce 결과 |

Agent State 안에 다른 mutable State를 넣지 않는다. 도구는 `ToolRuntime.state`를
읽고 `Command(update=...)`로 값 교체와 matching `ToolMessage`를 반환한다.
messages는 framework reducer, Agent의 관측값·근거·구간·counter는 replacement
channel이다. tuple/frozen model snapshot을 쓰며 in-place 변경하지 않는다.

한 AI turn의 첫 상태 변경 도구만 실행하고 나머지는 오류 ToolMessage로 응답한다.
Main의 readonly 후보 조회는 병렬로 가능하다. Main after_model은 task 실행 전에
분/호출 예산·분석 구간·승인 call ID를 함께 예약한다. 실행 실패도 예산을 소비하며
latest 분석 실패를 남긴다. middleware에는 별도 실행 counter가 없다.
Minute map 결과만 append reducer로 합치고 reduce 결과는 교체한다.

## 공유 데이터와 local schema

아래 경로의 `orchestrator/`와 `analysis/`는 각각
`incident_orchestrator_agent/`와 `incident_analysis_agent/`를 줄인 표기다.
`workflow/`와 `service/`는 Analysis 내부 경로다.

| 역할과 타입 | 위치/구현 | producer → consumer |
| --- | --- | --- |
| Incident | orchestrator/model/incident.py · frozen BaseModel | intake/manual → lifecycle/Agent |
| StartIncident, IncidentAnalysisRequest/Result, IncidentOutcome/Details | orchestrator/model/lifecycle.py · frozen dataclass | intake → lifecycle → analyzer → publication/caller |
| LogAnalysisRequest/Response | analysis/model/analysis_contract.py · frozen BaseModel | Main 어댑터 → Analysis / 좁은 요약 → Main 모델 |
| WindowAnalysisResult | analysis/model/analysis_contract.py · frozen dataclass | Analysis 최종 State → Main 어댑터 |
| TimeRange, log entries, HealthPoint, observations 계열 | analysis/model/ · frozen dataclass | 내부 조회/계산 → collector/report/출력 |
| Evidence, EvidenceProvenance | analysis/model/evidence.py · frozen BaseModel | collector → State/report/grounding/출력 |
| LogAnalysisReport, ReportFinding, RootCause, TimelineEvent | analysis/model/report.py · frozen BaseModel | draft 변환 → 검증/State/출력 |
| VerificationIssue/Type | analysis/model/validation.py · frozen BaseModel/StrEnum | grounding → 검증 middleware |
| IncidentAnalysisReport, Finding, Narrative, CauseAssessment, Recommendation, TimelineAnnotation | orchestrator/model/incident_report.py · frozen dataclass | projection → rendering/publication |
| WindowResult | orchestrator/model/window_result.py · frozen dataclass | 위임 결과 → Main State/result projection |
| ReportPublication | orchestrator/model/report_delivery.py · frozen dataclass | publisher → lifecycle 결과 (text_length: 평문 길이, path: 로컬 저장 파일, remote_path: SFTP로 올린 경로) |
| SftpTarget | orchestrator/service/report_delivery/publication/sftp/sftp_uploader.py · frozen dataclass | main.build_sftp_uploader(Settings) → SftpUploader |
| RawRecord, MinuteBucket, SelectedRecord, MinuteResult, AnalysisResult | workflow/minute_analysis/model.py · frozen dataclass | minute 입력/선별/결과 |
| MapSelection/Output, ReduceSelection/Output | workflow/minute_analysis/schema.py · BaseModel | LLM → minute nodes |
| DraftReport/Draft* | service/report_generation/schema.py · BaseModel | LLM → report writer/초안 변환 |
| _CandidateOutput | service/node_investigation · BaseModel | 노드 후보 LLM 응답 검증 |

BaseModel은 LLM/비신뢰 입력 검증, model_copy/validate/dump 등의 실제 사용 근거로
유지한다. 내부에서 생성하는 immutable value는 frozen dataclass로 표현한다.
TypedDict는 reducer가 필요한 mapping State에 사용한다. BaseModel/dataclass라는
이유로 폴더를 나누지 않는다. Enum은 상태/분류값이며 mutable State가 아니다.

`CollectedEvidence`는 collector 한 호출의 local result로, Agent State에 보관하지
않는다. `ObservationBuilder` 역시 호출 안에서 snapshot으로 생성해 parsing·계산·
prompt projection을 수행한 뒤 immutable update를 반환하고 폐기한다. 도구 closure나
State에서 수명 전체에 걸쳐 살아 있는 별도 실행 상태가 아니다.
Kafka settling의 `InflowTracker`, deadline, 취소 신호와 service 큐는 독립 owner의
수명/동기화 책임이므로 Agent의 두 번째 State가 아니다.

## lifecycle과 검증

자동 실행의 `main`은 `ProblemLogProcessor(max_pending=10_000)`와
`KafkaConsumerAdapter`를 직접 만든다. 수동 스크립트는 같은
`compose_analysis_service` 결과를 `RunManualAnalysis`에 주입하며 Kafka 객체를
만들지 않는다. `--dry-run`은 서비스 조립 없이 시각·범위만 확인한다.
로깅 초기화는 async `main()`과 수동 `run()` 시작 시 명시적으로 수행하며, 재호출해도
handler를 중복 생성하지 않는다. main·수동·SFTP 스크립트 import와 수동 `--dry-run`은
로그 파일이나 ClusterDoctor 소유 handler를 만들지 않는다. SFTP 확인 스크립트는
로깅을 초기화하지 않으며 업로드 한 번만 시도한다. 일반 publication은 기본 두 번 시도한다.
정상 종료는 processor 정리 후 공유 client를 닫고, 수동 실행의 실패·중단도 client를
정리한다. Kafka 치명적 장애는 consumer 정리를 시도한 뒤 `os._exit(1)`로 즉시
종료하며 실행 중인 분석의 완료를 기다리지 않는다.

`StartIncident → IncidentAnalysisRequest → MainAgentState → IncidentAnalysisResult
→ IncidentOutcome/publication` 경계를 유지한다. lifecycle은 Agent State를 공유하지
않는다. timeout 시 실행 중인 thread가 종료할 때까지 기다리되 이미 확보한 결과를
버리지 않고 실패 상태와 함께 전달한다.

Main 승인 window/goal/이전 report/ID sequence를 Analysis 요청으로 옮긴다.
Analysis는 자신의 fresh State에서 collect/write/insufficient를 실행한다.
같은 DeepAgent의 after_agent에서 구조 검증과 grounding을 수행한다. 검증은 최대
3회, 표현 수정 최대 2회, 원문 불일치 재수집/작성은 최대 1회다. 재분석에는 실제
불일치 사유를 전달하고 최종 evidence/report/observations를 같은 snapshot에서
투영한다. 재분석 실패 시 기존 산출물을 유지하고 소모된 ID sequence는 돌려주지 않는다.

Evidence ID는 수집 호출의 local allocator가 `E-{incident_id}-{number}`로 발급한다.
호출 완료 후 Analysis State에 sequence를 반영하고 결과 DTO로 Main에 돌려준다.
외부 mutable State를 바꾸는 callback은 없다. raw는 grounding과 리포트가 소비하므로 유지한다.
Graph 예외로 after_agent까지 도달하지 못한 초안은 보존하지만 분석 성공으로
표시하지 않는다. `NOT_VERIFIED`는 `PASSED`가 아니다.
재분석 후 검증 서비스가 예외로 끝나도 새 evidence sequence와 산출물을
같은 State update에 커밋하고 실패/미검증 상태를 반환한다.

구간별 report는 Main window_results에 그대로 누적한다. MainAgent는 누적
observations/evidence와 window report로 incident 전체 리포트를 합성한다.
최종 publication에는 합성 report와 누적 observations/evidence를 전달한다.
합성 실패 시 마지막 window report로 대체하지 않고 실패를 반환한다.
failure indicator에는 실행 실패·최신 분석 FAILED와 최종 합성 report의 검증
MISMATCH를 반영한다.

## 리포트와 수집 출처

`LogFetchResult`는 세 소스의 조회 레코드와 `LogSourceFailure`(소스·구간·오류)를
함께 전달한다. ClickHouse 어댑터는 분마다 slowlog·쿼리 로그·노드 메트릭을
병렬 조회하고 소스별 실패를 격리한다. collector는 성공 결과를 분석하며 실패는
gap에 기록한다. 모든 분의 세 소스가 실패한 경우에만 이 조회 단계를 degraded로 표시한다.

`EvidenceProvenance`는 수집 코드가 채우는 immutable 값이다. SSH의 실제 접속
호스트·파일 경로, ClickHouse의 실제 조회 DB·테이블과 원본 위치, 조회 구간·수집 시각을
`LogEntry → RawRecord → Evidence`로 전달한다. 수집 출처는 LLM이 작성하지 않는다.
`message`는 선별·분석용 설명이며 `raw`는 로그 원문 또는 조회된 레코드의 직렬화 값이다.
`raw_kind`는 표시 형태, `raw_truncated`는 길이 제한, `time_origin`은 로그 시각의
파싱·상속·대체 여부를 나타낸다.

`EvidenceCitation`은 근거 ID와 실제 Evidence를 함께 보관한다. 누락된 ID도 보존해
존재하지 않는 참조임을 표시한다. 전달 모델의 `CauseAssessment`는 원인별
판단 설명과 지지·반박 인용을 분리한다. `Narrative`는 화면에서 사용하는 원인 후보,
관측 사항, 후보 선택 이유와 조치만 보관한다. `IncidentAnalysisReport`는
관측값·narrative·evidence·타임라인 주석·검증 상태와 사고 범위를 전달한다.
내부 분석의 요약과 요약 근거 참조는
`LogAnalysisReport`에서 후속 구간 분석·검증에 사용하며 전달 모델에 중복 보관하지 않는다.

HTML은 결론(지연 범위 판정·원인·조치), 지표 카드, 쿼리 추이, 느린 개별 실행 순으로
표시한다. 주요 관측·소스 간 연결·타임라인·원문 로그·시스템 지표·인용 근거는
접힌 `근거 보기` 영역에 두며 인쇄 시 펼친다. 원인 확신도·미확인 사항 블록은 표시하지 않는다.
타임라인은 시작 시각별 사건으로 나누고 동일 유형의 반복 로그는 구간으로 유지한다.
평문 출력도 같은 원인과 인용 정보를 사용하며 출력 시각은 KST다.

`report_delivery/publication/report_publisher.py`의 `ReportPublisher`는 발행 매체 공통 계약이다.
`publication/file/html_file_publisher.py`가 로컬 파일 저장·실패 처리를 맡으며,
`rendering/html/report_renderer.py`의 `render_report`는 파일 저장 없이 HTML 문자열을 만든다.
SFTP 발행은 같은 계약을 구현한 게시자를 감싸 로컬 저장 결과를 업로드한다.

검증은 `uv run pytest`로 실행하며 collection I/O와 LLM 호출은 테스트에서 대체한다.
실제 외부 시스템의 연결 상태나 실제 사고의 원인 정확성을 보장하는 검사는 아니다.

`Observations.query_requests`는 ClickHouse의 `QueryLogEntry` 전체 수집 기록을 보존한다.
느린 요청 후보 및 LLM 선별과 독립적이며 Analysis state → Subagent 결과 → Main 병합을
거쳐 전달된다. 겹치는 조회는 메타데이터를 제외한 전체 기록 값별 최대 건수로 병합한다.
검색 요청은 유한하고 0 이상인 `run_time`으로 개별 실행 순위를 매기며 보고서에
최대 10건을 표시한다. 키워드 조합별로 합치거나 평균을 내지 않는다.
`QueryLogEntry`는 필요한 컬럼을 명시적으로 보관한다. 원본 `url`은 수집 단계에서
`target_host`·`index_name`·`conditions`로 파싱하고 저장하지 않는다.
`keyword`는 앞 5개만 저장하고 버린 개수를 계산 필드 `keyword_omitted`에 둔다.
`reg_date`가 저장 필드이고 `timestamp`는 공통 파이프라인용 읽기 전용 접근자다.
`success`는 Y/N 원문으로 보존하며 `is_success`로 판정한다.
검색 기간은 s_date/e_date(YYYYMMDD), 검색 일수는 date_range 원본 값이다.
slowlog와의 공통 요청 ID가 없으므로 사용자별 slowlog 연결을 임의로 추정하지 않는다.
