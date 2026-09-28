# 주요 타입과 진단 흐름

타입의 위치는 데이터가 태어난 소스보다 책임을 따른다. Domain은 업무 값과 검증 규칙, Application은 실행 명령과 포트, DeepAgents 어댑터는 모델 응답과 그래프 실행 상태를 소유한다.

최종 전달 타입은 `src/cluster_doctor/domain/diagnosis/diagnosis_report.py`, 구간 위임 계약은 `src/cluster_doctor/adapters/outbound/deepagents/diagnosis/contracts.py`, 근거 일관성 규칙은 `src/cluster_doctor/domain/diagnosis/report_validation.py`에 있다.

## 업무 값과 실행 계약

| 타입 | 계층·종류 | 목적 | 생성자 → 소비자 |
| --- | --- | --- | --- |
| `Settings` | Config · BaseSettings | 환경 변수/.env 검증과 기동 설정 | 설정 로더 → 조립 코드 |
| `SlowlogTrigger` | Domain · dataclass | 프레임워크와 무관한 유입 시각 | inbound 파서 → SlowlogIntake |
| `Incident` | Domain · frozen BaseModel | 식별자·클러스터·트리거/수신 시각의 시작 맥락 | intake/수동 진단 → 분석기 |
| `IncidentState` | Domain · BaseModel | 진행 구간·예산·상태·산출물 참조 | DiagnoseIncident → Supervisor/상태 저장소/전달 단계 |
| `TimeRange` | Domain · frozen dataclass | 시간대와 순서, 최대 10분 상한을 검증하는 구간 | 구간 계획/요청 변환 → 조회·분석·guardrail |
| `StartIncident` | Application · frozen dataclass | 정착된 Incident 전체 진단 명령 | intake/수동 진단 → DiagnoseIncident |
| `IncidentOutcome` / `IncidentDiagnostics` | Application · frozen dataclass | 종료 상태와 호출자가 읽는 진단 상세 | DiagnoseIncident → inbound 호출자 |
| `_Arrival` / `SlowlogIntake` | Application · 수신 값/실행 객체 | 수신 시각 보존, 정착과 워커 큐 관리 | handle → 정착 태스크/진단 워커 |
| `LogAnalysisRequest` | DeepAgents · BaseModel | 승인된 한 구간의 SubAgent 위임 계약 | Supervisor → Diagnosis SubAgent |
| `LogAnalysisResponse` / `DiagnosisHandback` | DeepAgents · BaseModel | 구간 결과와 미검증 보고서 참조를 되돌리는 계약 | SubAgent → Supervisor |

`Incident`는 시작 맥락이고 `IncidentState`는 갱신되는 업무 상태다. `StartIncident`는 Incident 생명주기를 시작하지만 `LogAnalysisRequest`는 그 안의 한 구간만 분석한다. `IncidentStatus`는 Incident 종료 여부, `LogAnalysisStatus`는 구간 분석 결과, `VerificationStatus`는 근거 일관성 검증 여부를 표현한다. 검증 미실행은 통과가 아니다.

## 원자료, 근거, 보고서

| 타입 | 계층·종류 | 목적 | 생성자 → 소비자 |
| --- | --- | --- | --- |
| `LogEntry`와 하위 타입 | Domain · dataclass | 소스별 조회 결과 계약 | 로그 어댑터 → collector/관측 집계 |
| `RawRecord` | DeepAgents · frozen dataclass | 실행 내 record_id로 원문 선별 | datasource 변환 → minute map/reduce |
| `Evidence` | Domain · frozen BaseModel | Incident에서 식별 가능한 선별 근거 | 선별 코드/저장소 id 발급 → 보고서 작성/검증/출력 |
| `CollectedEvidence` | DeepAgents · dataclass | 근거 묶음, 마스터 근거, 조사 노드와 실패한 분 | EvidenceCollector → DiagnosisSession |
| `Observations` | Domain · dataclass | 모델을 거치지 않은 건수·노드 지표·상태·요청 후보 | AnalysisRunState → 저장소/최종 출력 |
| `DraftReport`와 `Draft*` | DeepAgents · BaseModel | 모델의 구조화 응답 초안 | report writer → 변환 코드 |
| `LogAnalysisReport` | Domain · frozen BaseModel | 구간 및 병합 보고서, 근거 참조와 검증 결과 | 초안 변환/병합 → 검증/저장소/출력 매핑 |
| `ReportFinding` / `RootCause` / `TimelineEvent` | Domain · frozen BaseModel | 문제·원인·시각별 주장과 근거 참조 | 초안 변환 → 일관성 검증/병합 |
| `DiagnosisReport` | Domain · frozen dataclass | 관측값과 서술·근거를 함께 전달하는 운영자용 값 | output_mapping → ReportPublisher |
| `Finding` / `Narrative` | Domain · frozen dataclass | 전달용 판단 구조 | output_mapping → 보고서 표현 |
| `ReportPublication` | Application · frozen dataclass | 게시 결과 메타데이터(현재 텍스트 길이) | publisher → IncidentDiagnostics |

`LogEntry`는 조회한 원자료, `RawRecord`는 실행 내부 선별 입력, `Evidence`는 보고서가 인용하는 근거다. `CollectedEvidence`는 근거 한 건이 아니라 수집 결과 묶음이다. 모델은 원문을 다시 작성하지 않고 id를 고르며 코드가 시각·노드·원문을 옮긴다.

`DraftFinding`은 응답 스키마, `ReportFinding`은 검증 가능한 보고서 값, `Finding`은 운영자 전달용 서술이다. `DraftReport`를 파싱했다고 근거 검증까지 끝난 것은 아니다. `LogAnalysisReport`는 분석 산출물이고, `DiagnosisReport`는 코드 관측값을 포함한 전달 모델이다. `ReportPublication`은 전달 내용이 아니라 전달 구현의 반환값이다.

## 세 실행 상태

| 타입 | 범위 | 생성자 → 소비자 |
| --- | --- | --- |
| `AnalysisState` (TypedDict) | 한 datasource의 분별 map/reduce 그래프; 병렬 결과는 reducer로 합침 | minute graph → map/reduce 노드 |
| `AnalysisRunState` (일반 클래스) | 한 analysis window의 코드 관측값과 누락 집계 | DiagnosisSession → collector/관측 저장 |
| `IncidentAgentState` (DeepAgentState) | Supervisor 메시지·승인 구간·직전 응답 등 Agent 실행 상태 | Supervisor graph → 도구/SubAgent |
| `DiagnosisSession` (`_DiagnosisSession`) | 한 위임의 수집·작성 결과 | SubAgent → 진단 도구/handback 생성 |

업무 누적 상태는 `IncidentState`가 맡고 원문·근거·보고서 실체는 `ArtifactStore`가 맡는다. Agent 상태와 분별 그래프 상태를 Incident 저장 상태로 합치지 않는다.

## 현재 큐와 책임 흐름

Kafka inbound가 파싱한 `SlowlogTrigger`를 `SlowlogIntake.handle`로 전달한다. `_pending`은 동기 `queue.Queue[_Arrival]`이며 micro batch와 quiet period/대기 예산으로 유입을 정착시킨다. 정착 태스크는 `StartIncident`를 용량이 있는 `asyncio.Queue`인 `_incidents`에 넣고, 진단 워커는 이를 꺼내 `DiagnoseIncident.handle`을 실행한다. 유입 큐가 가득 차면 해당 트리거를 버리고 경고한다. 진단 큐가 가득 차면 put이 기다린다. 워커 수가 동시 Incident 분석 수를 제한한다.

`DiagnoseIncident`는 업무 상태를 만들고 분석기를 별도 스레드에서 실행한 뒤 종료·최종 전달을 담당한다. 실행 시간 초과에도 이미 실행 중인 스레드가 끝날 때까지 워커를 점유한다. intake 종료는 버퍼를 비우고 활성 진단이 끝나기를 기다린다. 수동 진단은 같은 시작 명령과 진단 생명주기를 사용한다.

Supervisor는 다음 구간과 목표를 고르고 코드 guardrail이 범위·중복·예산을 승인한다. Diagnosis SubAgent는 승인된 범위에서 근거 수집·분별 선별·노드 조사·초안 작성을 수행하고, 의미 검증 전인 Window Report를 반환한다. 구조화 파싱과 타입 변환은 근거 일관성 검증이 아니다. 의미 검증은 Main Agent의 `validate_final_report`가 병합 후 수행하며, 근거 id, 시각, 노드, 지지/반증 충돌 등을 검사한다. 이 검증은 모델 판단의 정답 보증은 아니다.

Supervisor는 구간 보고서를 병합한 뒤 `validate_final_report`로 최종 일관성을 검증한다. 최종 참조 없이 종료하면 애플리케이션 전달 단계가 `finalize_incident_report`로 병합·저장하는 안전망을 사용한다. 이 안전망 함수 자체는 의미 검증이나 모델 수정을 수행하지 않는다. 출력 매핑은 보고서와 누적 `Observations`, `Evidence`를 `DiagnosisReport`로 만들고 publisher는 누락과 실패 정보도 받는다. 검증 불일치가 있더라도 관측값을 운영자에게 전달한다.

## 타입 형태를 유지하는 이유

BaseModel은 구조화 LLM 응답 스키마, 직렬화, 필드 기본값, 기존 변환/검증 호출에 사용된다. 도메인에 있다는 이유만으로 일괄 dataclass 변환하지 않는다. dataclass는 이미 프레임워크 검증이 필요 없는 값에 사용하고 TypedDict는 그래프 상태 계약에 사용한다. 이번 문서화는 기존 타입 형태와 동작을 유지한다.

`ArtifactStore`, `IncidentStateRepository`, `IncidentAnalyzer`, `NodeResolver`, `SlowlogHandler`의 Protocol은 구조적 계약이다. `LogRepository`, `ClusterRepository`, `NodeLogFetcher`, `ReportPublisher`의 ABC는 기존 상속 계약이다. 두 형태 모두 애플리케이션 경계를 표현하므로 이름이나 모양의 통일만을 위해 강제 변환하지 않는다.
