# kafka_consumer

## 목표

Kafka의 문제성 로그 신호를 받아, 유입이 멎을 때까지 기다리고(정착) 클러스터 health를 보며 incident 범위를 정한 뒤 `StartIncident`로 만들어 `AnalyzeIncident`에 넘긴다. green이 아니면 이전 incident의 보고서를 넘기며 다음 incident를 이어서 만든다(세션). 분석·보고서는 하지 않는다(`incident_orchestrator_agent` 소관). `aiokafka`는 `consumer/kafka/`에만 둔다.

## 흐름

```text
consumer/kafka/consumer.py           메시지 → ProblemLogSignal(발생 시각만)
  → trigger_settling/service/problem_log_processor.py   큐 적재, 정착 대기, incident 세션 루프
      inflow.py InflowTracker          유입 양 끝(first_seen, last_seen)과 정착 판정
      incident_session.py              incident 마감 규칙, 다음 incident 판단, 상수
  → incident 구성: 30초마다 큐 확인 → 없으면 health → green이면 마감 (최대 20분)
  → AnalyzeIncident.handle(StartIncident)   → IncidentOutcome
  → 큐·health 확인 → 비었고 green이면 세션 끝, 아니면 다음 incident(이전 보고서 전달)
```

## 지켜야 하는 것

- **메시지에서는 발생 시각만 읽는다.** 상세 분석은 ClickHouse를 조회해서 하므로 다른 필드를 채우지 않는다. 파싱에 실패해도 consumer를 죽이지 않고 수신 시각으로 폴백한다.
- **시각은 항상 timezone-aware다.** 오프셋 없는 문자열은 KST가 아니라 UTC로 읽는다. naive가 섞이면 정렬이 TypeError로 터지고, 로컬 시간대를 가정하면 구간이 9시간 어긋난다.
- 기준 시각(`base_time`): 발생 시각이 수신 시각보다 미래(clock skew)이거나 파이프라인 지연이 30분을 넘으면 수신 시각을 쓴다. 고른 기준 문자열은 리포트의 "사용한 시각 기준"으로 그대로 간다. 미래 시각 신호는 현재 시각으로 눌러 쓴다.
- 정착: 확인 간격마다 큐를 비워 보고, 연속 2번 0건이면 정착이다(커넥터 폴링 때문에 1번 0건은 유입 중에도 나온다). 간격·대기 상한·큐 크기는 README 고정 한도 표를 본다.
- 대기와 마감·계속 판단을 프롬프트가 아니라 런타임 코드가 맡는다. 얼마나 기다릴지는 예산이고, "큐가 비었고 green이면 끝"은 판단이 아니라 규칙이다. Main Agent에 health·대기 도구를 두지 않는 이유다(초기 설계에서 Main이 `sleep`·`cluster_health` 도구로 기다리던 것을 `84f0aa1`에서 걷어냈다).
- **incident 구성**: 정착 뒤에도 같은 간격으로 큐를 확인해 새 신호는 이 incident에 합친다. 큐가 비었을 때 ES `_cluster/health`를 조회해 green이면 마감한다. red·yellow는 계속 기다린다. 범위가 상한(20분)에 닿으면 상태와 상관없이 마감한다. 조회 실패는 그 시점에 마감하고 gap으로 넘긴다. 조회 결과는 `health_point.fold_health`(수집기와 같은 함수)로 접어 health 이력으로 넘긴다.
- 정착·incident 구성 대기는 사건 처리 시간 상한에 넣지 않는다. 상한은 incident 하나의 분석·발행 시간이다.
- **세션**: 발행 뒤 큐와 health를 다시 확인해, 큐가 비었고 green이면 세션을 끝낸다. 아니면 이전 incident가 끝난 시각부터 다음 incident를 같은 규칙으로 구성한다. 분석이 상한보다 오래 걸렸으면 다음 incident는 즉시 상한 범위로 마감된다(긴 미복구를 20분 단위로 나눈다). 세션당 incident 수에 상한이 있고, 닿으면 "장시간 미복구, 자동 추적 중단"으로 끝낸다. 그 뒤 큐에 남은 신호는 새 세션이 된다.
- 다음 incident의 `StartIncident`에는 `SessionContext`(세션 id·순번·세션 첫 관측 `onset`·이전 incident id·보고서·분석 구간·가명 대응표)를 담는다. 이전 보고서와 분석 구간은 `IncidentOutcome`에서 코드가 옮긴다(`SessionContext.following`). LLM이 기억하게 하지 않는다. 두 번째 incident부터 `observed_start`는 이전 incident의 끝이고, 장애 시작은 `onset`에 남는다.
- health 조회 함수(`cluster_health`)를 주입하지 않으면 정착 직후 incident 하나로 끝난다(세션·대기 없음). 기존 정착 테스트가 이 경로다. 운영에서는 `main.py`가 ES client를 `asyncio.to_thread`로 감싸 넘긴다.
- **처리는 순차다.** 대기·확인과 분석을 동시에 돌리지 않는다. 분석 중 들어온 신호는 큐(넘치면 버리고 경고)에 쌓였다가 다음 incident에 합쳐진다. 분석 중에는 health를 조회하지 않으므로 그 시간의 이력은 비어 있다.
- 수동 분석은 이 패키지를 거치지 않으므로 세션을 만들지 않고 health도 기다리지 않는다.
- 종료(`close`): 정착·incident 구성 대기 중이면 취소하고, 분석 중이면 끝날 때까지 기다린 뒤 큐를 버린다. 세션은 이어 가지 않는다.
- Kafka: `auto_offset_reset="latest"`, 자동 커밋이다. 프로세스가 내려가 있던 동안의 신호는 다시 받지 않는다. 연결 확인이 `kafka_failure_timeout_seconds` 동안 연속 실패하면 `KafkaUnavailableError`로 프로세스를 exit code 1로 끝낸다(`main.py`).

## 핵심 파일

| 파일 | 역할 |
|---|---|
| `consumer/kafka/consumer.py` | `KafkaConsumerAdapter`, 연결 감시, 메시지 파싱 |
| `trigger_settling/service/problem_log_processor.py` | 큐, 정착 대기, incident 세션 루프, 종료 |
| `trigger_settling/service/inflow.py` | 기준 시각, `InflowTracker` |
| `trigger_settling/service/incident_session.py` | incident 마감 규칙, 다음 incident 판단, 범위·세션 상한 |
