# 주요 타입과 진단 흐름

타입의 위치는 데이터가 태어난 소스보다 책임을 따른다. 각 실행 주체(`kafka_consumer`,
`incident_orchestrator_agent`, `incident_analysis_agent`)의 `model/basemodel`은 업무
값과 검증 규칙을, `model/state`는 실행 중 갱신되는 상태를, `agent/`는 모델 응답과
그래프 실행 계약을 소유한다.

최종 전달 타입은
`src/cluster_doctor/incident_orchestrator_agent/model/basemodel/incident_analysis_report.py`,
구간 위임 계약은 `src/cluster_doctor/incident_analysis_agent/agent/contracts.py`,
근거 일관성 규칙은
`src/cluster_doctor/incident_analysis_agent/service/validation/consistency/report_validation.py`에
있다.

## 업무 값과 실행 계약

| 타입 | 소속 모듈 · 종류 | 목적 | 생성자 → 소비자 |
| --- | --- | --- | --- |
| `Settings` | `bootstrap/configuration` · BaseSettings | 환경 변수/.env 검증과 기동 설정 | 설정 로더 → 조립 코드 |
| `SlowlogTrigger` | `kafka_consumer` · dataclass | 프레임워크와 무관한 유입 시각 | inbound 파서 → trigger_settling |
| `Incident` | `incident_orchestrator_agent/model/basemodel` · frozen BaseModel | 식별자·클러스터·트리거/수신 시각의 시작 맥락 | intake/수동 진단 → 분석기 |
| `IncidentState` | `incident_orchestrator_agent/model/state` · BaseModel | 진행 구간·예산·상태·window별 산출물 | AnalyzeIncident → Main Agent Tool/Analysis SubAgent가 같은 참조로 공유 |
| `TimeRange` | `incident_analysis_agent/model/basemodel` · frozen dataclass | 시간대와 순서, 최대 10분 상한을 검증하는 구간 | 구간 계획/요청 변환 → 조회·분석·guardrail |
| `StartIncident` | `incident_orchestrator_agent/service/incident_lifecycle` · frozen dataclass | 정착된 Incident 전체 진단 명령 | intake/수동 진단 → AnalyzeIncident |
| `IncidentOutcome` / `IncidentAnalysisDetails` | `incident_orchestrator_agent/service/incident_lifecycle` · frozen dataclass | 종료 상태와 호출자가 읽는 진단 상세 | AnalyzeIncident → inbound 호출자 |
| `_Arrival` / `SlowlogIntake` | `kafka_consumer/trigger_settling` · 수신 값/실행 객체 | 수신 시각 보존, 정착과 순차 분석 루프 관리 | handle → 정착·분석 루프 |
| `LogAnalysisRequest` | `incident_analysis_agent/agent` · BaseModel | 승인된 한 구간의 SubAgent 위임 계약 | Main Agent → Analysis SubAgent |
| `LogAnalysisResponse` | `incident_analysis_agent/agent` · frozen BaseModel | 검증을 마친 리포트 상태·요약을 되돌리는 좁은 계약(Main Agent의 structured_response) | SubAgent → Main Agent |
| `WindowOutcome`(private) | `incident_analysis_agent/agent` · frozen BaseModel | 구간 결과를 IncidentState에 접기 위한 내부 전용 계약 — Main Agent 모델은 보지 않음 | SubAgent 응답 조립 → 상태 갱신 |
| `VerificationIssueType` / `VerificationIssue` | `incident_analysis_agent/model/basemodel` · StrEnum/frozen BaseModel | 원문 대조 불일치의 분류(분석·표현·검증 불가)와 내용 | GroundingValidator → 검증 루프 |

`Incident`는 시작 맥락이고 `IncidentState`는 갱신되는 업무 상태다. `StartIncident`는 Incident 생명주기를 시작하지만 `LogAnalysisRequest`는 그 안의 한 구간만 분석한다. `IncidentStatus`는 Incident 종료 여부, `LogAnalysisStatus`는 구간 분석 결과, `VerificationStatus`는 근거 일관성 검증 여부를 표현한다. 검증 미실행은 통과가 아니다.

## 원자료, 근거, 보고서

| 타입 | 소속 모듈 · 종류 | 목적 | 생성자 → 소비자 |
| --- | --- | --- | --- |
| `LogEntry`와 하위 타입 | `incident_analysis_agent/model/basemodel` · dataclass | 소스별 조회 결과 계약 | datasource 어댑터 → collector/관측 집계 |
| `RawRecord` | `incident_analysis_agent/workflow/minute_analysis` · frozen dataclass | 실행 내 record_id로 원문 선별 | datasource 변환 → minute map/reduce |
| `Evidence` | `incident_analysis_agent/model/basemodel` · frozen BaseModel | Incident에서 식별 가능한 선별 근거. 원문은 `raw` 필드에 직접 담긴다(별도 저장소 참조가 아니다) | 선별 코드/`IncidentState.next_evidence_id` → 보고서 작성/검증/출력 |
| `CollectedEvidence` | `incident_analysis_agent/service/evidence_collection` · dataclass | 근거 묶음, 마스터 근거, 조사 노드와 실패한 분 | EvidenceCollector → AnalysisSession |
| `Observations` | `incident_analysis_agent/model/basemodel` · frozen dataclass | 모델을 거치지 않은 건수·노드 지표·상태·요청 후보 | AnalysisRunState → IncidentState/최종 출력 |
| `DraftReport`와 `Draft*` | `incident_analysis_agent/service/report_generation` · BaseModel | 모델의 구조화 응답 초안 | report writer → 변환 코드 |
| `LogAnalysisReport` | `incident_analysis_agent/model/basemodel` · frozen BaseModel | 구간 보고서, 근거 참조와 검증 결과 | 초안 변환 → 검증/`IncidentState.window_results`/출력 매핑 |
| `ReportFinding` / `RootCause` / `TimelineEvent` | `incident_analysis_agent/model/basemodel` · frozen BaseModel | 문제·원인·시각별 주장과 근거 참조 | 초안 변환 → 일관성 검증 |
| `IncidentAnalysisReport` | `incident_orchestrator_agent/model/basemodel` · frozen dataclass | 관측값과 서술·근거를 함께 전달하는 운영자용 값 | output_mapping → ReportPublisher |
| `Finding` / `Narrative` | `incident_orchestrator_agent/model/basemodel` · frozen dataclass | 전달용 판단 구조 | output_mapping → 보고서 표현 |
| `ReportPublication` | `incident_orchestrator_agent/service/report_delivery/publication` · frozen dataclass | 게시 결과 메타데이터(현재 텍스트 길이) | publisher → IncidentAnalysisDetails |

`LogEntry`는 조회한 원자료, `RawRecord`는 실행 내부 선별 입력, `Evidence`는 보고서가 인용하는 근거다. `CollectedEvidence`는 근거 한 건이 아니라 수집 결과 묶음이다. 모델은 원문을 다시 작성하지 않고 id를 고르며 코드가 시각·노드·원문을 옮긴다.

`DraftFinding`은 응답 스키마, `ReportFinding`은 검증 가능한 보고서 값, `Finding`은 운영자 전달용 서술이다. `DraftReport`를 파싱했다고 근거 검증까지 끝난 것은 아니다. `LogAnalysisReport`는 window 하나의 분석 산출물이고, `IncidentAnalysisReport`는 코드 관측값을 포함한 전달 모델이다. `ReportPublication`은 전달 내용이 아니라 전달 구현의 반환값이다.

## 실행 상태

| 타입 | 범위 | 생성자 → 소비자 |
| --- | --- | --- |
| `AnalysisState` (TypedDict) | 한 datasource의 분별 map/reduce 그래프; 병렬 결과는 reducer로 합침 | minute graph → map/reduce 노드 |
| `AnalysisRunState` (일반 클래스) | 한 analysis window의 코드 관측값과 누락 집계 | AnalysisSession → collector/관측 저장 |
| `MainAgentState` (DeepAgentState) | Main DeepAgent의 승인 구간·목표 등 실행 상태 | Main 그래프 → 도구/task 위임(SubAgent로 복사) |
| `AnalysisAgentState` (DeepAgentState) | Analysis SubAgent 자신의 내부 그래프 실행 상태(커스텀 필드 없음) | SubAgent 내부 그래프 전용 |
| `AnalysisSession` | 한 위임의 수집·작성 결과 | SubAgent → 진단 도구, `LogAnalysisResponse` 조립 |

업무 누적 상태는 `IncidentState`가 맡고, window별 원문·근거·보고서 실체도 `IncidentState`가 직접 소유한다(별도 ArtifactStore를 두지 않는다). Agent 상태와 분별 그래프 상태를 Incident 저장 상태로 합치지 않는다.

## 현재 큐와 책임 흐름

Kafka inbound가 파싱한 `SlowlogTrigger`를 `SlowlogIntake.handle`로 전달한다. `_pending`은 동기 `queue.Queue[_Arrival]`이며 quiet period(연속 2번 무유입)와 전체 대기 예산으로 유입을 정착시킨다. 정착과 분석은 하나의 순차 루프다 — 정착이 끝나면 그 자리에서 바로 `StartIncident`를 만들어 `AnalyzeIncident.handle`을 실행하고, 그 분석이 끝나야 큐의 다음 트리거를 정착시킨다. 큐가 가득 차면 해당 트리거를 버리고 경고한다.

`AnalyzeIncident`는 `IncidentState`를 만들고 분석기를 별도 스레드에서 실행한 뒤 종료·최종 전달을 담당한다. `IncidentState`는 Main Agent Tool/미들웨어/Analysis SubAgent가 전부 같은 참조로 공유하므로, 스레드 실행이 끝난 뒤 별도 저장소를 다시 조회할 필요가 없다. 실행 시간 초과에도 이미 실행 중인 스레드가 끝날 때까지 정착 루프가 다음 트리거를 처리하지 못한다. 수동 진단은 같은 시작 명령과 진단 생명주기를 사용한다.

Main Agent는 다음 구간과 목표를 고르고 코드 guardrail이 범위·중복·예산을 승인한다. Analysis SubAgent는 승인된 범위에서 근거 수집·분별 선별·노드 조사·초안 작성을 수행한 뒤, 코드 검증 루프로 Window Report를 검증하고 `LogAnalysisResponse`를 반환한다. 구조화 파싱과 타입 변환은 근거 일관성 검증이 아니다. 의미 검증은 `validate_report`(근거 id, 시각, 노드, 지지/반증 충돌 등)와 `GroundingValidator`(Claim과 원문 대조)가 수행한다. 표현 불일치는 리포트를 수정하고, 분석 불일치는 같은 구간을 다시 분석하며, 해소되지 않으면 `MISMATCH`, 원문을 대조하지 못하면 `NOT_VERIFIED`로 남는다. 이 검증은 모델 판단의 정답 보증은 아니다.

구간 보고서는 합치지 않고 구간마다 `IncidentState.window_results`에 리포트 객체 그대로 쌓이며, 전달 단계는 마지막 구간의 보고서를 대표로 사용하고 모든 구간의 검증 불일치를 gap으로 함께 싣는다. 출력 매핑은 보고서와 누적 `Observations`, `Evidence`를 `IncidentAnalysisReport`로 만들고 publisher는 누락과 실패 정보도 받는다. 검증 불일치가 있더라도 관측값을 운영자에게 전달한다.

## 타입 형태를 유지하는 이유

BaseModel은 구조화 LLM 응답 스키마, 직렬화, 필드 기본값, 기존 변환/검증 호출에 사용된다. 특정 모듈에 있다는 이유만으로 일괄 dataclass 변환하지 않는다. dataclass는 이미 프레임워크 검증이 필요 없는 값에 사용하고 TypedDict는 그래프 상태 계약에 사용한다. 이번 리팩터링은 기존 타입 형태와 동작을 유지했다 — 파일 위치와 의존 방향만 실행 주체 중심으로 재배치했다.

`IncidentAnalyzer`, `NodeResolver`의 Protocol은 구조적 계약이다. `LogRepository`, `ClusterRepository`, `NodeLogFetcher`, `ReportPublisher`의 ABC는 기존 상속 계약이다. 두 형태 모두 실행 경계를 표현하므로 이름이나 모양의 통일만을 위해 강제 변환하지 않는다. `IncidentStateRepository`와 `ArtifactStore`는 제거했다 — `IncidentState`/`AnalysisSession`이 데이터를 직접 소유하면서 그 경계가 필요 없어졌다.
