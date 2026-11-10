# incident_orchestrator_agent (Main Agent)

## 목표

사건(incident) 하나의 수명을 맡는다. 어느 구간을 분석할지 고르고, 승인된 구간을 Analysis SubAgent에 위임하고, 언제 끝낼지 정하고, 끝난 뒤 보고서를 만들어 발행한다. **근거 수집·리포트 작성·검증은 하지 않는다**(`incident_analysis_agent` 소관).

## 흐름

```text
Kafka 신호 → 정착 대기 → StartIncident
  → AnalyzeIncident            service/incident_lifecycle/   타임아웃·취소, 결과 전달
  → MainAgent.analyze          agent/adapter.py              그래프 실행 → State에서 결과 투영
      list_candidate_windows → propose_analysis → task(analysis) → … → finish_incident
  → synthesize_incident        그래프 밖, adapter가 호출 (구현은 incident_analysis_agent)
  → to_incident_analysis_report → HTML 렌더 → 파일 → SFTP 발행
```

- 큐를 거치지 않는 수동 진입점은 `service/manual_analysis/`이고 `scripts/run_analysis.py`가 쓴다. 실제 분석 경로이므로 로컬에서는 `--dry-run`으로만 돌린다.

- 도구는 `list_candidate_windows`(읽기), `propose_analysis`(구간 승인), `finish_incident`(종료)와 DeepAgents의 `task`(위임)다.
- 승인 하나에 위임 하나다. `propose_analysis`가 `admitted_window`·`admitted_goal`을 State에 쓰고, `task`가 그것을 읽어 Analysis를 부른다. 위임 결과는 `agent/analysis_subagent.py`의 `apply_analysis_result`가 Main State에 합친다. Analysis는 Main State를 보지 않는다.

## 막는 것은 코드가 막는다

프롬프트 문장으로는 아무것도 막을 수 없다. 모든 상한은 호출 경로 위의 코드(`service/analysis_window/guardrails.py`, `agent/middleware.py`)에 있다.

- **분 예산**이 주 제약이다. 비용 동인이 호출 수가 아니라 분 수(분마다 LLM 선별)이기 때문이다. 호출 수·연속 거절·사건 처리 시간 상한은 폭주 차단용이다. 사건 처리 시간은 정착 대기를 포함한 전체이고 분석에는 그 나머지가 남는다. 값은 README 고정 한도 표를 본다. 같은 상수를 다른 패키지에 중복해서 두지 않는다(Analysis 쪽 상한은 `incident_analysis_agent/service/evidence_collection/limits.py`).
- 구간 길이 상한을 넘는 요청은 거절한다. 부분 중복은 아직 안 본 앞쪽 조각으로 자르고, 예산 초과는 앞쪽을 남기고 줄인다(원인은 구간 앞에서 만들어지는 경우가 많다). 이미 덮인 구간은 완전 일치가 아니라 포함 여부로 거절한다.
- 구간 차집합 계산(`window_planner.py`)을 모델에게 시키지 않는다. 첫 후보는 최초 관측 분의 5분 전부터 그 분의 끝까지다. 발생 이후 구간을 더 볼지는 `list_candidate_windows`의 정보를 보고 모델이 정한다.
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
- 리포트 UI 후속 점검: 쿼리 로그 근거를 `Evidence.record_key`로 찾은 `QueryLogEntry` DTO로 인용하는 작업은 `report_content.py`에 일부 들어가 있다. HTML과 평문 폴백이 같은 인용 함수(`projection/evidence_citation.py`의 `cite`·`citations`)를 쓰는지 확인한다. 평문 폴백에는 HTML에 있는 클러스터 이벤트 줄(`cluster_event_line`)과 지표 공백 줄(`metric_gap_line`)이 없다. 마스터 로그 실패가 요약 배너와 gap 줄에 중복 표시된다. 코드로 다시 확인한 뒤 착수한다.
- 미룬 과제: 발생 이후 구간(밀려서 생긴 증상)을 Main Agent가 덜 고르게 억제하는 것.

## 핵심 파일

| 파일 | 역할 |
|---|---|
| `agent/adapter.py` | `IncidentAnalyzer` 포트 구현. 그래프 조립·실행, 결과 투영, 사건 종합 호출 |
| `agent/state.py` | `MainAgentState`(이 에이전트의 유일한 mutable State) |
| `agent/tools.py`, `agent/middleware.py` | 도구와 위임 guardrail |
| `service/analysis_window/` | 예산·승인·구간 계산 |
| `service/incident_lifecycle/analyze_incident.py` | 타임아웃·취소·발행 |
