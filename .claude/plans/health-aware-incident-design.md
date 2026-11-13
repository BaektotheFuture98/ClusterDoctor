# 클러스터 health 기반 incident 세션

## 목적

ES `_cluster/health`를 보며 incident 범위를 정한다. 클러스터가 red·yellow인 동안 들어온 신호를 한 incident로 묶고, 오래 이어지면 20분 단위 incident로 나누어 이전 보고서를 넘기며 이어서 분석한다.

얻는 것:
- red·yellow 동안의 늦은 신호가 맥락 없는 별도 사건으로 다시 분석되지 않는다.
- 긴 장애도 incident 하나가 무한정 길어지지 않고, 다음 incident가 이전 보고서를 이어받는다.
- 보고서 타임라인에 incident 당시의 health 변화가 보인다.

하지 않는 것:
- Main Agent에 health·대기 도구를 두지 않는다. 대기와 마감·계속 판단은 코드 규칙이다(`84f0aa1`에서 Main의 `sleep`·`cluster_health` 도구를 걷어낸 이유와 같다).
- 대기·확인과 분석을 동시에 돌리지 않는다. 분석 중에는 health를 조회하지 않는다.
- 수동 분석(`manual_analysis`)은 세션을 만들지 않고 health도 기다리지 않는다.
- 프로세스 재시작 뒤 세션 복원.

## 흐름

```text
Kafka → 큐 → 유입 정착 대기(기존, ≤5분)
  → incident 구성: 30초마다
       큐에 신호 있음 → 가져와 이 incident에 합침
       큐 비었음      → health 조회 → green이면 마감 / red·yellow면 계속
       범위가 20분에 닿음 → 상태와 상관없이 마감
       조회 실패      → 그 시점 마감, gap
  → StartIncident → AnalyzeIncident.handle → 발행 → IncidentOutcome
  → 큐·health 확인
       큐 비었고 green → 세션 끝
       아니면 다음 incident: 시작 = 이전 incident 끝, 이전 보고서 전달, 다시 incident 구성
       세션 incident 3개째가 끝나도 green이 아니면 "장시간 미복구, 자동 추적 중단"
```

- 분석이 20분보다 오래 걸렸으면 다음 incident는 이미 상한을 넘었으므로 즉시 [시작, 시작+20분]으로 마감한다. 그 뒤 incident도 같다. 긴 미복구가 20분 단위로 나뉜다.
- 세션 상한 뒤 큐에 남은 신호는 새 세션이 된다.

## 상한

| 상한 | 값 | 비고 |
|---|---|---|
| 유입 정착 대기 (기존) | 30초 간격, 최대 300초 | 변경 없음 |
| incident 범위 (신규) | 최대 20분, 30초 간격 확인 | 시작 = 최초 관측(첫 incident) 또는 이전 incident 끝 |
| 세션당 incident (신규) | 3개 | |
| 사건 처리 시간 (변경) | incident당 30분 | 정착·incident 구성 대기를 빼지 않는다. 지금은 정착 대기를 30분에서 뺀다 |
| 분석 구간 길이·분 예산 (기존) | 10분·60분 | incident마다 |

## 구성 요소

| 단위 | 위치 | 하는 일 |
|---|---|---|
| `incident_session.py` (신규) | `kafka_consumer/trigger_settling/service/` | 마감 판정(순수 함수), 다음 incident 판단, 상수, 다음 incident 범위 계산 |
| `ProblemLogProcessor` (변경) | 기존 | 정착 뒤 incident 구성 루프, 세션 루프, `StartIncident` 조립, outcome에서 이전 보고서 옮기기 |
| health probe (주입) | `main.py` | `ElasticsearchClusterAdapter.health()`를 `asyncio.to_thread`로 감싸 processor에 넘김. ES `request_timeout`이 걸려 있다 |
| `fold_health` (이동) | `incident_analysis_agent/model/health_point.py` | `ObservationBuilder.record_health`의 접기 로직을 순수 함수로 옮겨 수집기와 kafka_consumer가 같이 쓴다. `HealthPoint` docstring의 옛 "2단계 대기 루프" 설명도 고친다 |

## 데이터 모델

`incident_orchestrator_agent/model/lifecycle.py`

```python
class IncidentClose(StrEnum):
    GREEN = "green 확인"
    SPAN_LIMIT = "범위 상한"
    HEALTH_UNKNOWN = "health 확인 불가"
    NOT_WAITED = "대기 안 함"          # 수동 분석

@dataclass(frozen=True)
class SessionContext:
    session_id: str
    sequence: int                                  # 1부터
    previous_report: LogAnalysisReport | None      # 직전 incident의 사건 종합 리포트
    previous_windows: tuple[TimeRange, ...]        # 세션에서 이미 분석한 구간 전체
    last_allowed: bool = False                     # 세션 상한의 마지막 순번
```

- `StartIncident`·`IncidentAnalysisRequest`에 `incident_end: datetime`, `close: IncidentClose`, `health_history: tuple[HealthPoint, ...]`, `health_gaps: tuple[str, ...]`, `session: SessionContext | None`을 더한다. 기본값을 두어 수동 경로는 호출부만 그대로다.
- `settling_wait_seconds`는 처리 시간 계산에서 쓰지 않는다(필드는 보고·로그용으로 남기거나 지운다. 구현 계획에서 정한다).
- `previous_windows`는 `outcome.diagnostics.observations.requested`에서 코드가 옮긴다.
- "장시간 미복구, 자동 추적 중단"은 발행 전에 알 수 있는 값으로 정한다: `last_allowed`이고 마감 사유가 green이 아니면(`SPAN_LIMIT`·`HEALTH_UNKNOWN`) 그 incident 보고서에 표시한다. 발행 뒤 green을 다시 확인하는 것은 세션을 끝낼지만 정하고 보고서를 고치지 않는다.

## Main·Analysis·종합

- Main 그래프·State는 incident마다 새로 만든다. 세션을 잇는 것은 kafka_consumer 코드다.
- 세션의 두 번째 incident부터 `observed_start`는 이전 incident의 끝이다. 장애 시작 시각은 `SessionContext.onset`(세션 첫 incident의 최초 관측)에 따로 두고, Main 시작 메시지의 "최초 관측 시각"과 사건 종합의 before·onset·after 기준에는 그것을 쓴다. 보고서 개요의 "발생 및 인지 일시"는 이번 incident 범위의 시작이고, 개요에 "장애 최초 관측" 줄을 따로 둔다.
- kickoff(`adapter._KICKOFF`)에 incident 범위, 마감 사유, 세션 순번을 더한다. 두 번째부터는 이전 보고서의 요약·원인·권장 조치와 이전에 분석한 구간을 더한다.
- 초기 후보: 첫 incident는 지금처럼 `initial_windows(observed_start)`. 두 번째부터는 `[incident 시작, +구간 길이 상한]`.
- `MainAgentState`에 `prior_windows`를 더해 `analyzed_windows`와 섞지 않고 `guardrails.py`(`subtract_spans`·`is_covered`)와 `window_planner.plan_new_windows`에서 함께 뺀다. 분 예산은 쓰지 않는다.
- `list_candidate_windows`에 incident 범위와 마감 사유 줄을 더한다.
- Analysis 위임과 사건 종합(초안·교정) 프롬프트에 이전 보고서를 "참고(이번 관측 아님)"로 싣는다. 근거 대조(`GroundingValidator`) 대상 근거에는 넣지 않는다. 숫자는 이번 `observations`에서 읽는다.
- `adapter.analyze`가 `synthesize_incident` **전에** `health_history`를 observations의 `health`에 합친다.
- `AnalyzeIncident._deliver`가 `health_gaps`를 발행 gap에 합친다. 처리 시간 상한은 `settling_wait_seconds`를 빼지 않는다.

### 가명화

이전 보고서는 실명으로 복원된 문장이다. incident마다 새 스코프(`@pseudonym_scope(fresh=True)`)로 열면 이전 incident의 company/user가 이번 스코프에 등록되지 않아 실명이 그대로 LLM으로 간다. **같은 세션의 incident는 같은 별칭 표를 이어 쓴다**(세션 id로 스코프를 잇는다). 수동 분석과 세션 첫 incident는 지금처럼 새 스코프다. 세션이 끝나면 별칭 표를 버린다.

## 보고서

- 개요(`report_layout._overview`)에 행을 더한다: incident 범위, 마감 사유, 세션 순번과 이전 incident. 평문 폴백도 같이 고친다.
- `_health_signals`(`incident_timeline.py`)의 필터를 분석 구간에서 incident 범위로 넓힌다. 분석 시점 health(`collector._collect_cluster_health`)는 마감 뒤라 섞이지 않는다.
- 분석 시점 health 조회와 `explain_allocation` 근거는 유지한다. 할당 실패 이유는 거기서만 나온다.

## 오류 처리

| 상황 | 처리 | 표시 |
|---|---|---|
| incident 구성 중 health 조회 실패 (예외, 타임아웃, payload 형식) | 그 시점 마감, `HEALTH_UNKNOWN` | gap "health 확인 불가로 incident 마감" |
| 발행 뒤 health 조회 실패 | 세션 끝 (큐에 신호가 있으면 새 세션) | 로그 |
| `handle()` 예외 (outcome 없음) | 이전 보고서 없이 다음 incident 판단을 계속 | 다음 incident의 이전 보고서 "없음(분석 실패)" |
| 대기 중 `close()` | 정착 대기와 같이 취소, 세션 이어 가지 않음 | — |
| 수동 분석 | `NOT_WAITED`, 세션 없음 | — |

## 테스트

`tests/unit/<패키지>/` 아래 소스 구조를 따른다.

- `incident_session.py`: 큐에 신호 → 계속 / 큐 비고 green → 마감 / red·yellow → 계속 / 20분 → 마감 / 조회 실패 → 마감. 다음 incident 판단(비었고 green → 끝, 아니면 계속, 세 번째 뒤 → 중단). 분석이 20분보다 길 때 다음 범위가 [시작, +20분].
- `fold_health`: 같은 상태면 `until`만, 바뀌면 새 점.
- `ProblemLogProcessor`(가짜 probe·가짜 analyze, 짧은 주기): 신호 → green이면 incident 1개 / red 지속이면 incident 3개와 각 `StartIncident`의 순번·이전 보고서·이전 구간 / 분석 중 쌓인 신호가 다음 incident에 합쳐짐 / 세션 상한 뒤 큐 신호가 새 세션 / 대기 중 `close()`.
- Main: kickoff에 범위·마감 사유·순번·이전 보고서 / `prior_windows`가 planner·guardrails에서 빠지고 분 예산은 그대로 / `list_candidate_windows` 줄.
- 가명화: 같은 세션의 두 incident에서 같은 company가 같은 별칭으로 가려짐 / 세션이 다르면 별칭 표가 이어지지 않음.
- adapter: 종합 전에 이력이 합쳐짐(종합 함수는 가짜). 종합 프롬프트에 이전 보고서가 참고로 실리고 근거 목록에는 없음.
- `AnalyzeIncident`: 처리 시간 상한이 대기 시간을 빼지 않음.
- 리포트: HTML·평문 개요 행 / `_health_signals`가 incident 범위 기준 / 수동 경로 출력 불변.

검증: `uv run pytest`, `uv run python -m compileall -q src`, asyncio 변경이라 Python 3.11 venv에서도 테스트한다. 실제 분석 경로는 로컬에서 돌리지 않는다.

## 문서

README와 패키지 CLAUDE.md(kafka_consumer, incident_orchestrator_agent, incident_analysis_agent)는 이 설계로 먼저 고쳤다. 구현과 같은 브랜치에 커밋한다.
