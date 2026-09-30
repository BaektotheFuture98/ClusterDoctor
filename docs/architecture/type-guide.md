# 주요 타입과 진단 흐름

폴더 위치는 구현 기술이 아니라 역할과 owner를 따른다. `model/`은 owner 안의
여러 component가 공유하는 구조화된 데이터다. State는 실행 owner 가까이,
특정 LLM 응답 schema는 그 service/workflow 가까이에 둔다.

## 실행 State

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

| 역할과 타입 | 위치/구현 | producer → consumer |
| --- | --- | --- |
| Incident | orchestrator/model/incident.py · frozen BaseModel | intake/manual → lifecycle/Agent |
| StartIncident, IncidentAnalysisRequest/Result, IncidentOutcome/Details | orchestrator/model/lifecycle.py · frozen dataclass | intake → lifecycle → analyzer → publication/caller |
| LogAnalysisRequest/Response | analysis/model/analysis_contract.py · frozen BaseModel | Main 어댑터 → Analysis / 좁은 요약 → Main 모델 |
| WindowAnalysisResult | analysis/model/analysis_contract.py · frozen dataclass | Analysis 최종 State → Main 어댑터 |
| TimeRange, log entries, HealthPoint, observations 계열 | analysis/model/ · frozen dataclass | 내부 조회/계산 → collector/report/출력 |
| Evidence | analysis/model/evidence.py · frozen BaseModel | collector → State/report/grounding/출력 |
| LogAnalysisReport, ReportFinding, RootCause, TimelineEvent | analysis/model/report.py · frozen BaseModel | draft 변환 → 검증/State/출력 |
| VerificationIssue/Type | analysis/model/validation.py · frozen BaseModel/StrEnum | grounding → 검증 middleware |
| IncidentAnalysisReport, Finding, Narrative, TimelineAnnotation | orchestrator/model/incident_report.py · frozen dataclass | projection → rendering/publication |
| WindowResult | orchestrator/model/window_result.py · frozen dataclass | 위임 결과 → Main State/result projection |
| ReportPublication | orchestrator/model/report_delivery.py · frozen dataclass | publisher → lifecycle 결과 |
| RawRecord, MinuteBucket, SelectedRecord, MinuteResult, AnalysisResult | workflow/minute_analysis/model.py · frozen dataclass | minute 입력/선별/결과 |
| MapSelection/Output, ReduceSelection/Output | workflow/minute_analysis/schema.py · BaseModel | LLM → minute nodes |
| DraftReport/Draft* | service/report_generation/schema.py · BaseModel | LLM → report writer/초안 변환 |
| Grounding*/_CandidateOutput | 해당 validation/node_investigation 모듈 · BaseModel | 해당 LLM 호출 내부 검증 |

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
외부 mutable State를 바꾸는 callback은 없다. raw는 grounding이 소비하므로 유지한다.
Graph 예외로 after_agent까지 도달하지 못한 초안은 보존하지만 분석 성공으로
표시하지 않는다. `NOT_VERIFIED`는 `PASSED`가 아니다.
재분석 후 검증 서비스가 예외로 끝나도 새 evidence sequence와 산출물을
같은 State update에 커밋하고 실패/미검증 상태를 반환한다.

구간별 report는 Main window_results에 그대로 누적한다. 최종 publication에는
마지막 report와 누적 observations/evidence를 전달하고 모든 window의 검증 issue를
중복 제거한 gap으로 표시한다. 완료 상태라도 최신 분석 FAILED 또는 검증 MISMATCH면
failure indicator를 유지한다.

## 구현 검증 기록 (2026-10-01)

실제 LangGraph ToolNode, create_agent 및 composite DeepAgents 실행을 포함하는
회귀 테스트로 ToolMessage 규약, 첫 mutable call 정책, 사전 예산 예약,
fresh delegation, 승인 목표 전달, 연속 evidence ID, 재분석 관측값/실패 보존,
미작성 report, 모든 window 검증 issue, timeout 결과, minute map/reduce와
HTML publication을 확인했다. 외부 LLM/ClickHouse/Elasticsearch/SSH에는 접속하지
않았으며 테스트에서는 해당 I/O만 대체한다.

독립 리뷰는 사용량 제한으로 전체 완료되지 않았다. 리뷰 도중 지적된 두
실패 상태 누락은 실제 graph 회귀 테스트의 RED→GREEN으로 수정했고, 남은
자체 점검에서는 검증 예외 후 sequence 보존과 미작성 report의 supervisor
실패 응답을 추가 검증했다. main 작업 트리를 유지하며 commit/push는 하지 않았다.
