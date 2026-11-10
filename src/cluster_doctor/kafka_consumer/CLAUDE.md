# kafka_consumer

## 목표

Kafka의 문제성 로그 신호를 받아, 유입이 멎을 때까지 기다린 뒤(정착) 관측 구간을 `StartIncident`로 만들어 `AnalyzeIncident`에 넘긴다. 분석·보고서는 하지 않는다(`incident_orchestrator_agent` 소관). `aiokafka`는 `consumer/kafka/`에만 둔다.

## 흐름

```text
consumer/kafka/consumer.py           메시지 → ProblemLogSignal(발생 시각만)
  → trigger_settling/service/problem_log_processor.py   큐 적재, 정착 대기, Incident 생성
      inflow.py InflowTracker          유입 양 끝(first_seen, last_seen)과 정착 판정
  → AnalyzeIncident.handle(StartIncident)
```

## 지켜야 하는 것

- **메시지에서는 발생 시각만 읽는다.** 상세 분석은 ClickHouse를 조회해서 하므로 다른 필드를 채우지 않는다. 파싱에 실패해도 consumer를 죽이지 않고 수신 시각으로 폴백한다.
- **시각은 항상 timezone-aware다.** 오프셋 없는 문자열은 KST가 아니라 UTC로 읽는다. naive가 섞이면 정렬이 TypeError로 터지고, 로컬 시간대를 가정하면 구간이 9시간 어긋난다.
- 기준 시각(`base_time`): 발생 시각이 수신 시각보다 미래(clock skew)이거나 파이프라인 지연이 30분을 넘으면 수신 시각을 쓴다. 고른 기준 문자열은 리포트의 "사용한 시각 기준"으로 그대로 간다. 미래 시각 신호는 현재 시각으로 눌러 쓴다.
- 정착: 확인 간격마다 큐를 비워 보고, 연속 2번 0건이면 정착이다(커넥터 폴링 때문에 1번 0건은 유입 중에도 나온다). 간격·대기 상한·큐 크기는 README 고정 한도 표를 본다. 이 대기 시간은 사건 처리 시간 상한에서 빠진다.
- 대기를 프롬프트가 아니라 런타임 코드가 맡는다. 얼마나 기다릴지는 판단이 아니라 예산이다.
- **처리는 순차다.** 사건 하나의 분석이 끝나야 다음 신호의 정착 판단을 시작한다. 분석 중 들어온 신호는 큐(넘치면 버리고 경고)에 쌓였다가 다음 사건이 된다. 그래서 같은 장애의 늦은 신호가 별도 사건으로 한 번 더 분석될 수 있다.
- 종료(`close`): 정착 대기 중이면 취소하고, 분석 중이면 끝날 때까지 기다린 뒤 큐를 버린다.
- Kafka: `auto_offset_reset="latest"`, 자동 커밋이다. 프로세스가 내려가 있던 동안의 신호는 다시 받지 않는다. 연결 확인이 `kafka_failure_timeout_seconds` 동안 연속 실패하면 `KafkaUnavailableError`로 프로세스를 exit code 1로 끝낸다(`main.py`).

## 핵심 파일

| 파일 | 역할 |
|---|---|
| `consumer/kafka/consumer.py` | `KafkaConsumerAdapter`, 연결 감시, 메시지 파싱 |
| `trigger_settling/service/problem_log_processor.py` | 큐·정착 루프·Incident 생성·종료 |
| `trigger_settling/service/inflow.py` | 기준 시각, `InflowTracker` |
