# incident_orchestrator_agent (Main Agent)

## 목표

사건(incident) 하나의 수명을 맡는다. 어느 구간을 분석할지 고르고, 승인된 구간을 Analysis SubAgent에 위임하고, 언제 끝낼지 정하고, 끝난 뒤 보고서를 만들어 발행한다. **근거 수집·리포트 작성·검증은 하지 않는다**(`incident_analysis_agent` 소관).

## 흐름

```text
Kafka 신호 → 정착 → incident 구성(kafka_consumer)
  → StartIncident(incident_end, close: IncidentClose, health_history, health_gaps, session: SessionContext)
  → AnalyzeIncident            service/incident_lifecycle/   타임아웃·취소, 결과 전달
  → MainAgent.analyze          agent/adapter.py              그래프 실행 → State에서 결과 투영
      list_candidate_windows → propose_analysis → task(analysis) → … → finish_incident
  → health 이력을 observations에 합침
  → synthesize_incident        그래프 밖, adapter가 호출 (구현은 incident_analysis_agent)
  → to_incident_analysis_report → HTML 렌더 → 파일 → SFTP 발행
  → IncidentOutcome → kafka_consumer가 다음 incident 여부를 판단
```

- Main 그래프·State는 incident마다 새로 만든다. 세션(연속 incident)을 잇는 것은 kafka_consumer의 코드이고 Main이 기억하지 않는다. 대기·health 확인·계속 여부도 Main의 도구가 아니다.
- 세션 맥락(`SessionContext`: 세션 id·순번·`onset`·이전 incident id·이전 보고서·이전 분석 구간·가명 대응표)과 마감 사유(`IncidentClose`)는 `model/lifecycle.py`에 있다. 수동 분석은 `session`이 없고 마감 사유는 `NOT_WAITED`라 지금 동작 그대로다.
- 요청을 Main 입력으로 바꾸는 곳은 `agent/incident_scope.py`다. `scope_lines`(kickoff에 덧붙일 범위·세션 줄), `first_windows`(첫 후보), `with_incident_health`(health 이력과 `Observations.incident_span`을 합침). 범위와 마감 사유는 `list_candidate_windows`로도 알린다. health 이력은 사건 종합 **전에** 합쳐야 종합과 타임라인이 같은 값을 본다. incident 구성 단계의 gap(`health_gaps`)은 `AnalyzeIncident._deliver`가 발행 gap에 합친다.
- **이전 incident 보고서는 참고이지 근거가 아니다.** 요약·원인·권장 조치(`previous_incident_digest`)를 Main kickoff, Analysis 위임(`LogAnalysisRequest.previous_incident_report`), 사건 종합(`synthesize_incident(previous_incident_report=)`)에 넘겨 이어서 쓰게 하지만, 이번 incident의 주장은 이번 근거(`evidence_refs`)로만 뒷받침하고 숫자는 이번 `observations`에서 읽는다. 위키와 같은 취급이다. 같은 incident 안의 구간 진단 문장을 종합에 싣지 않는 규칙(모델이 근거 없는 주장에 고정된다)은 그대로다.
- 세션의 두 번째 incident부터 `observed_start`는 이전 incident의 끝이지 장애 시작이 아니다. 시작 메시지의 "최초 관측 시각"과 사건 종합의 `onset`(before·onset·after 기준)에는 `request.onset`(세션 첫 관측)을 쓴다. 보고서 개요의 발생 시각은 이번 범위의 시작이고 "장애 최초 관측" 줄을 따로 둔다.
- **가명화 스코프는 세션 단위다.** 이전 보고서의 company/user 실명이 다음 incident 프롬프트로 가므로, 같은 세션의 incident들은 같은 별칭 표를 이어 써야 마스킹된다. `MainAgent.analyze`는 `pseudonym_scope(fresh=True, mapping=session.pseudonyms)`로 연다. 대응표 없이 새 스코프로 열면 이전 incident의 이름이 등록되지 않아 실명이 그대로 LLM으로 간다.

- 큐를 거치지 않는 수동 진입점은 `service/manual_analysis/`이고 `scripts/run_analysis.py`가 쓴다. 실제 분석 경로이므로 로컬에서는 `--dry-run`으로만 돌린다.

- 도구는 `list_candidate_windows`(읽기), `propose_analysis`(구간 승인), `finish_incident`(종료)와 DeepAgents의 `task`(위임)다.
- 승인 하나에 위임 하나다. `propose_analysis`가 `admitted_window`·`admitted_goal`을 State에 쓰고, `task`가 그것을 읽어 Analysis를 부른다. 위임 결과는 `agent/analysis_subagent.py`의 `apply_analysis_result`가 Main State에 합친다. Analysis는 Main State를 보지 않는다.

## 막는 것은 코드가 막는다

프롬프트 문장으로는 아무것도 막을 수 없다. 모든 상한은 호출 경로 위의 코드(`service/analysis_window/guardrails.py`, `agent/middleware.py`)에 있다.

- **분 예산**이 주 제약이다. 비용 동인이 호출 수가 아니라 분 수(분마다 LLM 선별)이기 때문이다. 호출 수·연속 거절·사건 처리 시간 상한은 폭주 차단용이다. 사건 처리 시간은 incident 하나의 분석·발행 시간이고 정착·incident 구성 대기는 포함하지 않는다(대기가 최대 20분까지 늘어 빼면 분석 시간이 남지 않는다). 값은 README 고정 한도 표를 본다. 같은 상수를 다른 패키지에 중복해서 두지 않는다(Analysis 쪽 상한은 `incident_analysis_agent/service/evidence_collection/limits.py`).
- 구간 길이 상한을 넘는 요청은 거절한다. 부분 중복은 아직 안 본 앞쪽 조각으로 자르고, 예산 초과는 앞쪽을 남기고 줄인다(원인은 구간 앞에서 만들어지는 경우가 많다). 이미 덮인 구간은 완전 일치가 아니라 포함 여부로 거절한다.
- 구간 차집합 계산(`window_planner.py`)을 모델에게 시키지 않는다. 첫 후보는 최초 관측 분의 5분 전부터 그 분의 끝까지다. 발생 이후 구간을 더 볼지는 `list_candidate_windows`의 정보를 보고 모델이 정한다.
- 세션의 두 번째 incident부터 첫 후보는 incident 시작 시각부터 구간 길이 상한까지다. 이전 incident들이 분석한 구간은 `prior_windows`로 받아 `analyzed_windows`와 섞지 않고, `guardrails.py`와 `window_planner.py`의 차집합·포함 판정에서 함께 뺀다. 분 예산은 쓰지 않는다.
- 상태를 바꾸는 도구(`propose_analysis`, `task`, `finish_incident`)는 한 turn에 하나만 실행한다. `DelegationGuardrailMiddleware.after_model`이 `task`를 State에 먼저 예약(`admitted_task_call_id`)하고, `wrap_tool_call`은 예약이 없는 `task`를 거절한다.

## 종료와 결과

- 종료를 확정하는 것은 `finish_incident`이고 결과는 그래프 반환 메시지가 아니라 State(`status`, `closing_reason`)에서 읽는다(`adapter._result_from`). 에이전트가 `finish_incident` 없이 끝나면 FAILED다. COMPLETED에는 구간 리포트가 하나 이상 필요하다.
- 사건 종합(`synthesize_incident`)은 구간 리포트가 없거나 CANCELLED면 건너뛴다. 종합이 실패하면 마지막 구간 리포트로 대신하지 않고 FAILED로 낸다.
- 타임아웃이어도 분석 스레드는 끝날 때까지 기다리고, 이미 확보한 결과는 실패 상태와 함께 전달한다(client 정리와 동시성 제한이 깨지지 않게).
- 구간 리포트마다 검증 결과가 따로 있다. 최종 종합 판정이 바로잡았다면 앞선 구간의 불일치는 실패로 세지 않는다. 워크플로·분석이 실제로 실패한 경우는 그대로 실패다.

## 보고서 발행

`service/report_delivery/`는 projection(DTO → 화면용 값) → rendering(`html/`, `text/`) → publication(`file/`, `sftp/`) 순서다.

- 건수·최대값은 analysis의 `service/observation/`이 센 `observations`를 그대로 옮긴다. projection에서 새로 세거나 모델 문장에서 읽지 않는다. 문장 생성은 `cross_links_text.py`처럼 HTML과 평문 renderer가 공유하는 함수에 둔다.
- 평문 폴백(`rendering/text/report_text.py`)은 HTML과 섹션 구성을 따로 구현하므로 섹션을 바꾸면 둘 다 고친다.
- 타임라인의 health 신호(`incident_timeline._health_signals`)는 분석 구간이 아니라 incident 범위 안의 점만 쓴다. 분석 구간마다 조회한 분석 시점 health는 마감 뒤라 여기에 섞이지 않는다.
- 보고서 개요의 incident 범위·마감 사유·세션 순번·이전 incident·장애 최초 관측 줄은 `projection/incident_scope_rows.py`의 `scope_rows`가 만들고 HTML과 평문이 같이 쓴다(`IncidentAnalysisReport.scope_rows`). "장시간 미복구, 자동 추적 중단"은 발행 전에 알 수 있는 조건(`tracking_stopped`: 세션의 마지막 순번이 green 확인 없이 마감)으로 표시한다. 발행 뒤 health를 다시 보는 것은 세션을 끝낼지만 정하고 이미 나간 보고서를 고치지 않는다.
- 리포트 UI 후속 점검: 쿼리 로그 근거를 `Evidence.record_key`로 찾은 `QueryLogEntry` DTO로 인용하는 작업은 `report_content.py`에 일부 들어가 있다. HTML과 평문 폴백이 같은 인용 함수(`projection/evidence_citation.py`의 `cite`·`citations`)를 쓰는지 확인한다. 평문 폴백에는 HTML에 있는 클러스터 이벤트 줄(`cluster_event_line`)과 지표 공백 줄(`metric_gap_line`)이 없다. 마스터 로그 실패가 요약 배너와 gap 줄에 중복 표시된다. 코드로 다시 확인한 뒤 착수한다.
- 미룬 과제: 발생 이후 구간(밀려서 생긴 증상)을 Main Agent가 덜 고르게 억제하는 것.

## 핵심 파일

| 파일 | 역할 |
|---|---|
| `agent/adapter.py` | `IncidentAnalyzer` 포트 구현. 그래프 조립·실행, 결과 투영, 사건 종합 호출 |
| `agent/incident_scope.py` | incident 범위·세션 맥락을 kickoff·첫 후보·관측값으로 바꿈 |
| `agent/state.py` | `MainAgentState`(이 에이전트의 유일한 mutable State) |
| `model/lifecycle.py` | `StartIncident`·`IncidentAnalysisRequest`·`SessionContext`·`IncidentClose` 경계 계약 |
| `agent/tools.py`, `agent/middleware.py` | 도구와 위임 guardrail |
| `service/analysis_window/` | 예산·승인·구간 계산 |
| `service/incident_lifecycle/analyze_incident.py` | 타임아웃·취소·발행 |
