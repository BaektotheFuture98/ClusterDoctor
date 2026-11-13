# incident_analysis_agent (Analysis SubAgent)

## 목표

Main Agent가 승인한 **분석 구간 하나**를 조사한다. 근거를 모으고, 리포트를 쓰고, 원문 DTO와 대조해 검증한 결과(`WindowAnalysisResult`)를 돌려준다. 추가로 사건 전체의 최종 서술을 만드는 `synthesize_incident`도 여기에 있다(Main의 `adapter`가 그래프 밖에서 부른다). 구간 선택·예산·종료는 하지 않는다(`incident_orchestrator_agent` 소관).

## 구간 분석 흐름

```text
tools (agent/tools.py)
  collect_evidence      구간 근거 수집. 실제 수집은 1회, 재호출은 cached
  write_report          초안 작성. 최대 2회
  report_insufficient   부족 사유와 추가 구간 제안(실행 승인이 아님. 실제 분석 여부는 Main이 정함)
after_agent (agent/subagent.py finalize_update)   리포트가 있을 때만
  구조 검사 + 근거 대조(GroundingValidator) 최대 3라운드
    ANALYSIS_MISMATCH 있음 → 재분석(초안만 다시 씀). 1회를 이미 썼으면 수정 없이 종료
    그 밖(구조 검사·REPORT_MISMATCH) → 표현 수정(revise)
project_result → WindowAnalysisResult
```

- 검증은 모델이 도구로 호출하지 않는다. 리포트가 있으면 `AnalysisValidationMiddleware.after_agent`가 돈다.
- 재분석은 근거를 다시 수집하지 않는다. 수집은 구간만 보고 목표를 보지 않아서 다시 모아도 같은 원문이고, 이미 모은 근거·관측에 불일치 사유를 목표로 붙여 초안만 다시 쓴다. 재분석 실패 시 기존 산출물을 유지한다.
- 상태: 수집 실패·실행 실패·근거 없는 degraded는 FAILED, 추가 구간 제안이 있으면 NEED_MORE_CONTEXT, 아니면 리포트 유무로 COMPLETED/FAILED.
- 검증 상태 해석과 검증·수정·재분석 횟수는 루트 `CLAUDE.md` 데이터 원칙을 따른다.

## 근거 수집 (`service/evidence_collection/collector.py`)

- ClickHouse 조회는 분 단위로 쪼개 병렬로(최대 5분) 부른다. 분마다 slowlog·query log·node metric 세 쿼리가 따로 나간다.
- slowlog·query log·마스터 로그(`collector.py`)와 데이터 노드 SSH 로그(`node_investigation.py`)는 `workflow/minute_analysis/`의 Map-Reduce LLM 선별(분 버킷 map → 전체 reduce → Evidence[])을 거친다. 선별은 원인을 추정하지 않는다. 프롬프트는 네 소스 공용이고 소스별 기준은 각 `datasource`의 `AnalysisSpec`에 있다. map은 1분만 보므로 구간 비교가 필요한 기준(급변, 몰림, 구간 내내 반복)은 `what_stands_out_in_window`에 두어 reduce에만 싣는다. node_log의 `event_type`은 타임라인 묶음 키이고 그 "N건"은 선별된 근거 수다. 노드 지표는 임계 규칙(LLM 없음)이고 `node_metric_gaps`로 지표가 구간 중간에 끊기거나 늦게 시작한 노드를 드러낸다.
- 마스터 로그는 구간당 쿼리 하나다(`node_role='master'`이고 WARN/ERROR이거나 `MASTER_EVENT_LOGGERS`의 logger, 시간 오름차순 300건 상한). 같은 줄이 LLM 분 선별과 코드 집계(`cluster_events.py`, `node_mentions.py`)에 모두 쓰인다. 보고서에는 `builder.py`의 `MASTER_LOG_REPORT_MAX = 120`건으로 줄여 싣는다.
- 데이터 노드 SSH 로그는 마스터 로그에 언급된 노드 ID에 대해서만 `NodeResolver`로 찾아 수집한다. 시간대(`NODE_LOG_TIMEZONE`) 분 접두와 심각도·이탈·GC 패턴으로 먼저 거른다.
- ES 클러스터 health는 구간마다 현재 값을 조회한다(사건 시점의 상태가 아니다). yellow/red일 때만 `explain_allocation`과 함께 근거로 남긴다. 할당 실패 이유는 여기서만 나오므로 유지한다. 사건 당시의 health 이력은 kafka_consumer가 incident 구성 중에 조회해 넘기고 Main adapter가 observations에 합친다. 두 값을 섞어 쓰지 않는다. 이력을 접는 함수(`model/health_point.py`의 `fold_health`)는 양쪽이 같이 쓴다.
- 소스 하나가 실패해도 나머지는 유지하고 gap으로 남긴다.
- 상한 값은 README 고정 한도 표를 본다. `MAX_EVIDENCE_PER_SOURCE`·`MAX_EVIDENCE_TOTAL`은 구간마다 수집이 끝날 때 `preserve_required_evidence`(`analysis_context.py`)가 근거를 자르는 상한이다. 상위 쿼리 실행 근거(`required_query_evidence`)를 먼저 확보한 뒤 나머지를 채운다. `MAX_EVIDENCE_PER_SOURCE`는 Map-Reduce 선별의 소스당 상한(`minute_analysis/spec.py`)으로도 쓴다. `MAX_RAW_LOG_CHARS`는 `build_analysis_context`가 LLM에 싣는 분석 컨텍스트(리포트 작성·수정, 근거 대조, 사건 종합, 위키 조사)의 기본 상한이고 노드 조사 원문에도 쓴다.
- 수집 출처는 코드가 채운다(`EvidenceProvenance`). Evidence ID는 호출 local allocator가 발급한다.

## 사건 종합 (`service/report_generation/incident_synthesis.py`)

- 순서: 위키 조사 → `draft_incident` → 검증·수정 최대 3라운드(`incident_final=True`, 사건 수준 수정 프롬프트와 `IncidentDraftReport` schema) → `keep_known_wiki_refs`.
- 위키 조사 실패·미설정이면 빈 결과로 위키 없이 종합한다. 위키 조사 에이전트는 `wiki_investigation_agent` 소관이다.
- 검증 권고(advisory)는 재수정에만 쓰고 최종 판정과 `verification_issues`에는 넣지 않는다. 대조한 주장이 0건인데 주장이 있으면 `UNCHECKED`다.
- 재수정 응답은 위키 조사 결과를 보지 않고 `wiki_ref`를 다시 쓸 수 있어 최종 리포트도 에이전트가 연 패턴 id로 한 번 더 제한한다.
- 사건 종합 실패를 마지막 구간 리포트로 대체하지 않는다.
- 세션의 두 번째 incident부터 이전 incident 보고서(요약·원인·권장 조치)를 구간 분석과 종합 프롬프트에 참고로 싣는다. 이어서 쓰는 맥락일 뿐 이번 관측이 아니므로 근거 대조(`GroundingValidator`)의 대상 근거에 넣지 않는다. 이전 보고서만으로 뒷받침되는 주장은 근거 없음으로 걸러져야 한다.

## 알아둘 점

- `agent/runtime/`은 프로세스 공용 LLM 인프라다(루트 `CLAUDE.md` 구조 절 참고). 분리 전까지 분석 전용 코드를 넣지 않는다. 프로세스 전체 LLM 동시 호출은 `litellm_client.MAX_LLM_CONCURRENCY`로 묶인다.
- 보고서의 숫자·건수는 `service/observation/`이 코드로 센 값이고 모델은 해석만 쓴다. `observations`를 구간끼리 합칠 때(`merge_observations`)는 (timestamp, node, line)으로 중복을 제거하고 상한을 다시 적용하지 않는다.
- 후속 점검: 마스터 로그 보고서 상한 120건이 노드 이탈 집계 입력까지(`correlation.py`가 잘린 `master_events`로 `cluster_events.py`의 `parse_cluster_events`를 부른다) 자른다. 집계는 자르기 전 `master_logs` 기준이어야 하는지 확인한다. 보고서에 노드 이탈이 안 나올 때의 의심 순서: 120건 절단 → 이탈 줄이 분석 구간 밖 → logger 표기가 `MASTER_EVENT_LOGGERS`와 정확히 일치하지 않음 → `node_role` 값 → 300건 조회 상한. 운영 ClickHouse에서 확인하며 로컬에서 실제 분석을 돌리지 않는다.
- 미룬 과제: `analysis_context.py`의 `slow_executions` 상위 10개가 발생 이후의 후행 쿼리로 차는 문제.

## 핵심 파일

| 파일 | 역할 |
|---|---|
| `agent/subagent.py` | `AnalysisAgent`, 검증 `finalize_update`, `project_result` |
| `agent/tools.py` | 도구 3개와 `collect_update`·`report_update` |
| `agent/state.py` | `AnalysisAgentState` |
| `agent/dependencies.py` | `AnalysisSeams`(수집·LLM·위키 등 주입 묶음) |
| `service/evidence_collection/collector.py` | 소스별 수집 |
| `service/observation/` | 코드가 세는 관측값(`builder`, `cluster_events`, `correlation`) |
| `service/report_generation/` | 리포트 작성·사건 종합·프롬프트 |
| `service/validation/` | 구조 검사(`consistency`)와 근거 대조(`grounding`) |
| `workflow/minute_analysis/` | 분 단위 Map-Reduce 선별 그래프 |
