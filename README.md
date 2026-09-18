# ClusterGuard

ClusterGuard는 Elasticsearch 클러스터를 스스로 진단한다. Kafka에서 ES 슬로우 로그를
실시간으로 받아 유입이 멎기를 기다린 뒤, 에이전트 둘이 나눠서 일한다 — **Supervisor**가
어느 시간대를 볼지 정하고, **진단 SubAgent**가 그 구간 안에서 조사·원인 분석·리포트
작성·검증을 끝낸다. 결과는 HTML 파일 한 장으로 남는다.

요청에 응답하는 서비스가 아니라 계속 떠 있는 컨슈머다. 아무도 호출하지 않고, 슬로우 로그가
도착하는 것에 반응한다.

> 예전에는 호출자가 시간 구간을 넘기는 `POST /api/v1/diagnosis` HTTP API가 있었다. 그
> 계층은 없앴다. 이제 에이전트가 트리거의 타임스탬프에서 구간을 스스로 고른다.
> 엔드포인트를 찾고 있다면, `DiagnosisService`와 `single`/`graph` `ANALYSIS_MODE` 스위치와
> 함께 제거됐다.

## 동작 방식

```
ES slowlog 발생
  → Filebeat                                    (실측 +16초)
  → ES 데이터스트림 logs-elasticsearch.slowlog-default
  → Kafka Elasticsearch Source Connector
  → Kafka slowlog 토픽 ─┬→ ClusterGuard consume  (트리거)
                        └→ ClickHouse slowlog_v2 (실측 합계 +31초)
```

커넥터는 Elasticsearch에서 읽어 Kafka로 쓴다. 두 갈래가 같은 커넥터 출력에서 나오므로,
ClusterGuard에 메시지가 닿을 무렵이면 ClickHouse에도 대체로 들어와 있다 — 배치 윈도가
그만큼은 충분히 벌어 준다.

트리거된 뒤:

1. **묶기** — `SlowlogTriggerService`가 `MICRO_BATCH_SECONDS` 동안 도착분을 모았다가
   Incident **하나**를 연다. 슬로우 로그는 몰려서 오므로 건건이 진단하면 같은 사고를
   수십 번 분석하게 된다.
2. **유입이 멎기를 기다린다** — `IncidentOrchestrator`가 큐를 15초 간격으로 비우며 보고,
   **연속 두 번** 신규 0건을 본 뒤에야 다음으로 간다. 한 번으로는 부족하다 — 커넥터의
   폴링 간격 때문에 사고 도중에도 잠깐 잠잠해 보이는 구간이 생긴다. 누적 상한 5분.
3. **Scope 결정** — Supervisor가 `IncidentState`(이미 본 구간, 남은 예산, 직전 응답)를 보고
   `REQUEST_ANALYSIS` / `COMPLETE_INCIDENT` / `FAIL` / `CANCEL` 중 하나를 고른다.
4. **분석** — 진단 SubAgent가 그 구간에 대해 datasource별 워크플로를 돌려 Evidence를 모으고,
   그것들을 한자리에 놓고 원인을 추론해 리포트를 쓴 뒤, 근거와 리포트가 맞는지 검증한다.
5. **더 볼까** — SubAgent가 구간 **밖**의 시간이 필요하다고 답하면 `NEED_MORE_CONTEXT`와
   함께 범위를 제안한다. 승인은 Supervisor가 한다. 3번으로 돌아간다.
6. **리포트** — `HtmlFileNotifier`가 Incident 한 건당 HTML 파일 하나를 `REPORT_DIR`에 쓴다.

### 두 에이전트가 보는 것이 다르다

```
Supervisor        Incident의 Scope와 Lifecycle만.  구조화 호출 1회로 Decision 하나
진단 SubAgent     구간 하나 안의 조사·RCA·리포트·검증 전부
```

**Supervisor는 raw 로그를 보지 않는다.** 둘 사이를 오가는 것은 구조화된
`LogAnalysisRequest`/`LogAnalysisResponse`와 참조뿐이고, 실체(Evidence·원문·리포트)는
`ArtifactStore`에 있다. Supervisor의 프롬프트는 사이클마다 상태 스냅샷 한 장으로 새로
그려지므로, 분석을 몇 번 하든 컨텍스트가 불어나지 않는다.

**tool loop가 아니다.** Supervisor가 결정할 것은 행동 하나와 그 이유뿐이라 tool이 필요
없고, 루프와 상한은 `IncidentOrchestrator`가 갖는다. 모델에게 루프를 맡기면 상한이 프롬프트
문장이 되고, 프롬프트 문장은 강제가 아니다.

**한도는 프롬프트가 아니라 런타임 코드가 강제한다.** 예산 단위는 호출 수가 아니라 **분**이다
— 조회가 분 단위로 쪼개지고 비어 있지 않은 분마다 LLM이 돈다. 호출 수로 세면 1분 창과 10분
창이 같은 예산을 먹어, 1분짜리 공백 하나를 메우는 데 예산의 6분의 1이 날아간다.

### 왜 분 단위로 쪼개는가

datasource마다 같은 절차를 돈다. 판단 기준만 `TriageSpec`으로 갈아 끼운다.

```
Raw Logs → 1분 Chunk → Map(분마다 LLM) → Reduce(구간 전체) → Evidence[]
```

구간 전체를 프롬프트 하나에 담으면 결국 표본을 뽑아야 하고, 표본 추출은 **가장 오래된
로그부터** 잃는다. 분으로 쪼개면 각 버킷이 상한 아래에 들어가므로 모든 줄이 모델에 닿는다.

**모델은 줄 번호만 돌려준다.** Map은 `#3`처럼 번호가 붙은 줄을 보고 남길 번호와 짧은 이유를
고르고, Reduce는 그 후보들만 다시 보고 최종 번호를 고른다. 시각·노드·원문은 코드가
`RawRecord`에서 옮긴다 — 모델이 옮겨 적으면 틀리고, 이 저장소는 그것을 실측으로 두 번
확인했다("관측값과 판단을 나눈다" 참고). 없는 번호를 고르면 버린다.

**Reduce는 근본 원인을 정하지 않는다.** 여기서 원인을 확정하면 그 결론이 한 소스만 보고
내려진 것이 되고, 뒤 단계는 이미 내려진 결론을 확인하는 절차로 퇴화한다. 원인은 모든
datasource의 Evidence가 모인 뒤 Cross-source 단계가 처음으로 묻는다.

비어 있는 분은 건너뛰고 비용이 들지 않는다. 한 분이 실패해도(레이트 리밋, 필터링된 응답)
진단이 가라앉지 않는다. 그 분은 `[분석 실패]`로 표시되고 실패한 시각이
`unresolved_gaps`로 Supervisor에게 올라가므로, 빈 구간이 감춰지지 않는다. Reduce 호출 자체가
실패하면 Map 결과를 그대로 남기되 "걸러지지 않았다"는 사실을 함께 남긴다 — 걸러지지 않은
근거가, 근거가 없는 것보다 낫다.

노드 메트릭만 이 그래프를 타지 않는다. 임계값을 넘은 최고점을 코드가 규칙으로 고른다.
`heap=92%`는 판단이 아니라 측정이고, 측정을 모델에게 읽히면 옮겨 적다 틀린다.

### 노드 로그는 두 경로로 온다

마스터 노드 로그는 ClickHouse에 있고 데이터 노드 로그는 없다. 그래서 길이 둘이다.

| | 소스 | 언제 | 이유 |
|---|---|---|---|
| 마스터 노드 | ClickHouse 노드 로그 테이블 | 구간마다 항상 | SSH가 없고, IP를 얻는 `GET /_nodes` 왕복이 없고, severity 정규식 대신 `level` 컬럼을 쓴다 |
| 데이터 노드 | 노드로 SSH | **후보가 나왔을 때만** | 그 로그는 어디에도 적재되지 않는다 |

둘을 잇는 것은 마스터 로그 자체다.

```
Master Evidence → ProblemNodeCandidate → NodeResolver → SSH → Node Triage
                       (LLM)              (조회)      (조회)
```

**조건부인 것이 요점이다.** 후보가 없으면 SSH에 붙지 않는다. 접속 하나가 ES 왕복 + 새 세션 +
파일 grep이고, 근거 없이 그것을 치르면 추측에 비용을 쓰는 것이다. 후보는 마스터 Evidence의
`[id]`를 근거로 인용해야 하고, 인용이 없거나 없는 id를 대면 그 후보는 버린다.

**Resolver와 Fetcher는 LLM이 아니다.** 노드 주소는 추론할 것이 아니라 조회할 것이고, SSH
명령은 모델이 정하지 않는다 — 어느 쪽도 모델이 틀렸을 때 되돌릴 방법이 없다. 조립되는
명령은 `grep`/`tail` allowlist와 경로 문자 검사를 통과해야 나간다.

마스터 로그는 구간마다 자동으로 딸려 온다 — 마스터 이벤트와 분 단위 슬로우 로그 급증을
**같은 Evidence 목록에** 놓는 경로이고, "상관관계를 설명하라"는 요구에 필요한 것이 그것이다.

**마스터 로그는 레벨이 아니라 로거로 고른다.** 이 기능이 드러내려는 이벤트 — 샤드 재배치,
node-left, 리더 선출, 할당 실패 — 를 Elasticsearch는 **INFO**로 남기므로 `WARN,ERROR`
필터는 정확히 중요한 것을 버린다. 그렇다고 INFO를 다 열면 더 나쁘다. 실제 테이블에서 재
보니 INFO 10줄 중 9줄이 ML 유지보수, 만료 데이터 삭제, 매핑 변경 잡음이었고, 사고 중에는
샤드별 INFO 줄이 행 상한을 삼켰다. 그래서 질의는
`level IN (WARN,ERROR) OR logger IN (<클러스터 이벤트 로거들>)`이다 — 커버리지는 올라가고
분량은 내려간다.

측정으로만 찾을 수 있었던 두 가지: 저장된 `logger` 값에 뒤쪽 공백 패딩이 붙어 있어
(`"o.e.t.TransportService    "`) 비교에 `trimBoth(logger)`를 쓴다는 것, 그리고 그 값이 전체
클래스명이 아니라 축약형(`o.e.c.r.a.AllocationService`)이라는 것.

### 슬로우 로그는 발생 시각으로 읽는다

`slowlog_v2`에는 타임스탬프가 둘이다. `ch_ingested_at`(ClickHouse가 행을 저장한 시각)과
`_source.@timestamp`(ES가 실제로 느린 쿼리를 기록한 시각). 질의는 후자로 필터한다. 둘의
실측 지연은 23~41초이고, 지연이 분 경계를 넘으면 **트리거를 일으킨 바로 그 슬로우 로그**가
빠지기에 충분하다 — 같은 1분 구간이 어느 컬럼으로 필터하느냐에 따라 다른 행을 돌려준다
(실측 4행 대 2행).

## 리포트

Incident 한 건당 HTML 파일 하나를 `HtmlFileNotifier`가 쓴다. 한 Incident가 구간을 여러 번
분석했으면 관측값은 누적되고 리포트는 마지막 것이 실린다.

```
reports/report-20260916-083051.html
```

### 관측값과 판단을 나눈다

리포트는 문자열이 아니라 객체(`DiagnosisReport`)다. 그 안이 둘로 갈려 있다.

```
DiagnosisReport
├─ observations   코드가 센 것.  모델을 거치지 않는다
└─ narrative      모델이 쓴 판단. 관측값을 하나도 담지 않는다
```

**나눈 이유는 실측이다.** 예전에는 모델이 쓴 평문을 notifier가 정규식으로 훑어 리포트를
만들었고, 그 경로가 두 번 틀렸다.

1. `slowlog=264` — 그 구간의 실제 slowlog는 **0건**이고 264는 `es_query_log` 건수였다.
   코드는 소스별 건수를 정확히 세어 넘기는데, 타임라인 줄 형식에 소스 칸이 하나뿐이라
   모델이 비어 있지 않은 숫자를 그 칸에 넣었다.
2. `took=미확인` — "문제 쿼리 후보"가 요구하는 필드가 `SlowlogEntry`에만 있는데 그 구간
   slowlog가 0건이라, 모델이 `es_query_log` 항목을 고르고 칸을 못 채웠다.

둘 다 뿌리가 같다. **코드가 정확히 아는 값을 모델이 옮겨 적게 시켰다.** 이제 숫자는 코드가
세고 모델은 판단만 쓴다. 느린 요청 후보도 코드가 `[C1] [C2] …`로 수치와 함께 제시하고,
모델은 **id와 고른 이유만** 돌려준다 — `미확인`과 전사 오류가 구조적으로 불가능해진다.

섹션은 최대 열 개이고, 빈 섹션은 건너뛴다(번호는 코드가 센다).

```
관측값 →  1 인시던트 개요          유입 시각, 분석 구간, 시각 기준, 대기 시간, 코드 판정 심각도
         2 분 단위 타임라인        소스별 건수 · took/runtime/heap 최대 · rejected
         3 클러스터 상태 이력
         4 노드별 구간 최대값
         5 마스터 노드 로그        사건별로 묶고 대표 줄은 원문 그대로
         6 느린 요청 후보          [C1] … 수치는 코드, 선정 이유는 모델
판단  →  7 결론
         8 발견된 문제점           severity 배지 + 근거
         9 근본 원인               근거 / 반박 근거 / 확인하지 못한 것
        10 권장 조치
```

**관측값은 묶어서 싣는다.** 첫 실행 리포트가 17,526자였는데 마스터 로그 24줄이 그중
72%(12,685자)였고, 그 24줄 중 20줄이 같은 `follower_check` 타임아웃으로 대상 노드 이름만
달랐다. 사건별로 묶으면 "40초 동안 19대에 대해 20건"이 한눈에 들어온다. 노드도 마찬가지다 —
107대 중 rejected가 0이 아닌 것이 하나도 없으면 목록 대신 세 줄로 요약한다. 리포트가
5,596자가 됐다 — 읽을 내용은 늘고 분량은 3분의 1이다.

**대표 줄은 원문 그대로 남긴다.** 근거로 인용하려면 원문이어야 한다. 잘라낸 건수는
`… 외 N건`으로 드러낸다. 묶이지 않은 줄(로거를 뽑을 수 없는 스택 트레이스 등)은 예외로
여러 줄을 싣는다 — 서로 다른 사건이라 대표 하나로 줄이면 나머지가 통째로 사라진다.

### 모든 주장이 근거 참조를 달고 다닌다

원인 분석 단계에 도착하는 것은 원문이 아니라 줄어든 Evidence 목록이고, 각 줄 앞에 `[id]`가
붙어 있다. 모델은 그 id로 인용한다.

```
Evidence
├─ evidence_id / event_time / source / node   코드가 채운다
├─ message                                    원문 그대로
├─ raw_ref                                    원문을 다시 꺼낼 참조
└─ selection_reason                           왜 남겼는가
```

리포트의 타임라인·발견된 문제·원인 후보가 전부 `evidence_refs`를 든다. 그래야 검증이
가능해진다 — 근거 참조가 없는 주장은 "검증할 수 없다"가 아니라 **"근거가 없다"**로 판정된다.

### 검증은 별도 에이전트가 아니다

검증에 필요한 것은 판단이 아니라 대조다. 같은 모델에게 자기 출력을 채점시키면 거의
통과한다. 그래서 규칙 여덟 개가 전부 **구조화된 필드**에서 나온다.

| 검사 | 잡는 것 |
|---|---|
| 없는 근거 인용 | 존재하지 않는 id. 근거가 있는 것처럼 보이게 만든다 |
| 근거 없는 주장 | `evidence_refs`가 빈 finding / 원인 / 타임라인 |
| 제시되지 않은 후보 | 목록 밖의 `C9`. 코드가 수치를 조인할 수 없다 |
| 시각 불일치 | 타임라인이 쓴 시각 ≠ 인용한 근거의 시각 (허용오차 60초) |
| 노드 불일치 | 아는 노드 이름을 지목했는데 그 노드의 근거를 인용하지 않았다 |
| 순서 어긋남 | 타임라인이 시간순이 아니다 |
| 과도한 확신 | `confidence=High`인데 근거 1건. 근거가 얇은데 "~때문이다" |
| 인과 역전 | 원인으로 든 근거가 결과보다 늦다 |

딱 한 군데 본문을 읽는 곳이 노드 이름인데, 그것도 **이미 아는 이름의 집합**하고만 대조한다.
본문에서 이름을 뽑으려 들면 임의의 단어를 노드로 오인하고, 그 오탐이 수정 횟수를 태운다.

불일치가 나오면 지적을 **전부 한 번에** 실어 다시 쓰게 한다. 하나씩 알려 주면 허용된 횟수
안에 끝나지 않는다. 수정은 최대 1회이고, 상한에 닿으면 남은 불일치를 리포트에 **기록한 채**
내보낸다 — 지적을 지우면 운영자가 검증을 통과한 리포트로 읽는다.

### 모델이 판단을 남기지 못해도 리포트는 나간다

구조화 출력은 실패 갈래를 늘린다. 응답이 JSON이 아니거나 필드가 어긋나면 빈 초안으로
떨어지는데, 그때도 코드가 모은 관측값은 온전하다.

| | 상황 | 결과 |
|---|---|---|
| 정상 | 초안이 읽히고 검증 통과 | `verification_status=PASSED` |
| 불일치 | 수정 상한까지 갔는데 남음 | `MISMATCH`. 지적을 배너로 싣고 재트리거하지 않는다 |
| 형식 이탈 | 초안을 읽지 못함 | 빈 초안 → 검증이 "근거를 인용하지 않았다"로 잡고 `gaps`에 남는다 |
| 조회 실패 | Evidence가 하나도 없음 | `status=FAILED`. 관측값만으로 리포트 |

**어느 경우든 관측값 섹션은 항상 그려진다.** 근거가 하나도 없으면 원인 분석을 아예 부르지
않는다 — 빈 목록을 주고 "원인을 찾으라"는 호출은 비용만 들고, 그 답은 근거 없는 산문이 된다.

**형식이 맞아도 판단이 빌 수 있다.** 같은 구간을 세 번 돌렸더니 모델이 채운 판단 필드가
9개 중 8개 → 3개 → 0개로 들쭉날쭉했다(프롬프트에 "반드시 채운다"를 넣은 뒤에도). 검증은
이것을 잡을 수 없다 — 주장이 없으면 대조할 것도 없다. 그래서 등급을 바꾸는 대신 `gaps`에
사실을 남기고, notifier가 배너로 그린다.

### 심각도는 둘이다

모델이 `severity`를 채우지 않는 일이 반복됐다. 9/16 04:22 구간 실측에서 노드 이탈과
GREEN→YELLOW 전환이 분류 없이 리포트에 실렸다. 기본값을 `"Info"`로 되돌리는 것은 이미
실패한 길이다 — 25초 지연과 노드 19대 타임아웃이 전부 `Info`로 나왔었다. 빈칸을 그럴듯한
값으로 채우는 것은 관측값 쪽에서 이미 당한 실패다.

그래서 **코드가 관측값에서 따로 낸다.** 개요에 한 줄로 실리고, "코드 판정"이라고 못 박아
모델 판단과 구분한다.

```
관측된 이상 신호: Warning (코드 판정) — 마스터 로그 ERROR 2건, 마스터 로그 WARN 6건
```

| 등급 | 조건 |
|---|---|
| `Critical` | `rejected > 0` — 요청이 실제로 거절됐다. 사용자가 받은 오류다 |
| `Warning` | 마스터 로그 ERROR / 분석하지 못한 분 |
| `Info` | 마스터 로그 WARN |
| (없음) | 위 어느 것도 아님 |

판정 규칙은 **전부 구조화된 필드**에서 온다. 모델이 쓴 문장은 읽지 않는다 — 읽기 시작하면
모델 산문 파싱이 되고, 그것이 이 저장소가 두 번 당한 실패다.

**클러스터 상태는 분석 구간 안의 관측만 센다.** `cluster_health`는 ES 실시간 API라 과거를
모르고, 과거 사고를 분석하면 그 값은 진단을 돌린 시각의 상태다(리포트도 그렇게 경고한다).
밖의 값으로 등급을 매기면 사고와 무관한 시각의 green이 "정상" 판정을 만든다.

모델의 `severity`는 비면 `(모델이 분류하지 않음)`으로 그린다. 코드가 대신 채우지 않는다 —
그 필드는 판단이고, 측정에서 따라 나오는 등급은 따로 자리가 있다. 둘이 어긋나는 것이 오히려
읽을 거리다.

### 리포트는 항상 전달된다

Incident 실행은 문자열이 아니라 `IncidentOutcome`을 돌려주고, 그것이 예전 설계가 뭉쳐
놓았던 두 결정을 갈라놓는다.

| | 무엇이 정하는가 |
|---|---|
| 리포트를 전달하는가 | **언제나 전달한다.** |
| 재트리거하는가 | `analysis_failed`만 |
| 무엇이 빠졌는가 | `gaps` — 배너로 그리고 본문에 섞지 않는다 |

뭉쳐 두었더니 실제 진단을 잃었다. 실패한 실행이 예외를 올리면 `notify`를 건너뛰어 리포트가
사라졌고, 운영자는 `logs/app.log`를 뒤져야 무언가를 알 수 있었다. 더 나쁜 것은 **보조**
조사의 실패도 같은 일을 했다는 것이다 — SSH 접속 하나가 거절되면 슬로우 로그 분석이 멀쩡히
끝난 진단이 통째로 버려졌다.

지금은 "진단이 성립했는가"로 가른다.

- 조회가 통째로 실패(`status=FAILED`)하거나 검증이 불일치로 끝남(`MISMATCH`) →
  `analysis_failed`. 리포트는 그래도 나가되 "결론을 신뢰할 수 없다"는 붉은 배너가 붙고,
  재트리거하지 않는다.
- 노드 로그 수집 실패, 분석 예산 소진, 일부 분 버킷 실패 → `gaps` 항목. 리포트는
  정상적으로 나가고 재트리거도 **한다**. 분석 자체는 성공했기 때문이다.

`gaps`는 모델이 아니라 코드가 기록한다. 프롬프트로 "수집하지 못한 것을 밝혀라"라고 시키는
것으로는 부족하다 — 모델이 어길 수 있고, 그러면 "노드 로그를 못 읽었다"와 "읽었는데 특이
사항이 없다"가 리포트에서 똑같아 보인다.

### HTML

모든 텍스트가 이스케이프된다. 리포트는 ES 쿼리 원문과 로그 줄을 그대로 인용하므로 최종
사용자의 검색어와 `<`·`&`가 페이지에 닿는다. 파일은 자기완결적이라(웹 폰트 없음, CDN 없음)
망 분리 환경에서도 열리고, 밝은/어두운 팔레트와 인쇄 스타일을 함께 담는다. 맨 아래
`<details>`에 평문 전문이 들어간다.

저장에 실패해도 진단을 잃지 않는다 — 전문을 로그로 남긴다. 인코딩할 수 없는 문자는
(provider 응답에 섞여 오는 짝 없는 서로게이트 등) 입구에서 치환한다. 그러지 않으면 파일
쓰기와 폴백 로그가 **동시에** 무너진다.

`reports/`는 git에서 제외한다. 운영 인덱스명, 쿼리 원문, company/user 식별자가 실린다.

## 요구 사항

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/)
- 슬로우 로그 토픽을 나르는 **Kafka**
- 슬로우 로그 / 쿼리 로그 / 노드 메트릭 / 노드 로그 테이블을 가진 **ClickHouse**
- **Elasticsearch** (구간마다 클러스터 상태를 먼저 읽는다)
- LLM provider **하나**의 API 키 — Gemini 또는 NVIDIA NIM, `LLM_PROVIDER`로 고른다
- 선택: ES 노드의 **SSH** 자격 증명. 없으면 데이터 노드 로그를 못 쓰고 그 갈래는 건너뛴다
- 최초 기동에만 `openaipublic.blob.core.windows.net` 접근 — "배포 메모" 참고

## 설정

환경 변수 또는 프로젝트 루트의 `.env`에서
`src/cluster_doctor/infrastructure/config/settings.py`가 읽는다. `.env.example`을 `.env`로
복사해 실제 값을 채운다 — 실제 키를 커밋하지 않는다.

필수 값이 없으면 **기동 시점에** 시끄럽게 실패한다. 첫 슬로우 로그가 올 때까지 미루지
않는다. 오류는 *어느* 설정이 잘못됐는지만 말하고 값은 절대 말하지 않는다 — 설정 실패가
`CLICKHOUSE_PASSWORD`를 로그 수집기에 써 넣는 일이 없도록.

| 변수 | 필수 | 기본값 | 비고 |
|---|---|---|---|
| `LLM_PROVIDER` | 아니오 | `gemini` | `gemini` 또는 `nvidia_nim`. 모르는 값은 첫 호출의 `KeyError`가 아니라 기동 시점에 거부된다. |
| `GEMINI_API_KEY` | 선택 시 | — | litellm에 파라미터로 넘긴다. URL에 절대 싣지 않는다. |
| `GEMINI_MODEL` | 아니오 | `gemini-3.5-flash-lite` | |
| `NVIDIA_API_KEY` | 선택 시 | — | |
| `NVIDIA_MODEL` | 아니오 | `google/gemma-4-31b-it` | litellm 로컬 비용 맵에 항목이 없어 비용 추적이 0으로 읽힌다. `reasoning_effort`를 지원하지 않는다 — "섹션 나눈 CoT" 참고. |
| `CLICKHOUSE_URL` | 예 | — | 예: `jdbc:clickhouse://host:8123/packetbeat`. `jdbc:` 접두어는 생략 가능. **경로의 데이터베이스가 아래 네 테이블을 가진 것이어야 한다** — 다른 데이터베이스의 동명 테이블도 조회에 성공하고 낡은 행을 돌려주며, 어디에도 오류가 남지 않는다. |
| `CLICKHOUSE_USER` | 아니오 | `default` | |
| `CLICKHOUSE_PASSWORD` | 아니오 | `` (빈 값) | |
| `CLICKHOUSE_SLOWLOG_TABLE` | 아니오 | `slowlog_v2` | |
| `CLICKHOUSE_LOG_TABLE` | 아니오 | `log` | |
| `CLICKHOUSE_NODE_METRIC_TABLE` | 아니오 | `es_node_metric` | |
| `CLICKHOUSE_NODE_LOG_TABLE` | 아니오 | `loki_logs` | 현재 적재 범위는 **마스터 노드 로그뿐**이다. |
| `ES_HOST` | 예 | — | 쉼표로 구분한 호스트 목록. 비면 기동 시점에 거부된다. |
| `ES_PORT` | 아니오 | `9200` | |
| `ES_USER` | 아니오 | `` (빈 값) | 비면 basic auth를 아예 쓰지 않는다. |
| `ES_PASSWORD` | 아니오 | `` (빈 값) | |
| `SSH_USER` | 아니오 | `` (빈 값) | 데이터 노드 로그에만 쓴다. |
| `SSH_PASSWORD` | 아니오 | `` (빈 값) | |
| `SSH_PORT` | 아니오 | `22` | |
| `KAFKA_BOOTSTRAP_SERVERS` | 아니오 | `localhost:9092` | |
| `KAFKA_TOPIC` | 아니오 | `slowlog` | |
| `KAFKA_GROUP_ID` | 아니오 | `clusterdoctor` | 오프셋은 `(group, topic, partition)`으로 관리되므로 토픽만 바꾸는 것은 안전하다 — 새 토픽에는 커밋된 오프셋이 없고 `auto_offset_reset=latest`가 끝에서 시작한다. |
| `MICRO_BATCH_SECONDS` | 아니오 | `10` | Incident를 열기 전에 도착분을 모으는 시간. |
| `CLUSTER_NAME` | 아니오 | `elasticsearch` | 리포트와 Incident에 실리는 이름. |
| `NODE_HEAP_WARN_PERCENT` | 아니오 | `85` | 이 값을 넘은 노드 메트릭만 근거가 된다. 64GB heap에서 85%는 평상시일 수 있고, 그때 기본값을 그대로 쓰면 배경 소음이 근거 목록을 채운다. |
| `NODE_QUEUE_WARN` | 아니오 | `100` | search/write 큐. 기본 크기가 1000이므로 100이면 이미 밀리고 있다는 뜻이다. |
| `REPORT_DIR` | 아니오 | `reports` | 진단 HTML을 쓰는 곳. Incident 한 건당 파일 하나. 상대 경로는 작업 디렉터리 기준. |
| `LITELLM_LOCAL_MODEL_COST_MAP` | 아니오 | `True` | `Settings`가 아니라 litellm이 직접 읽는다. `litellm_client.py`가 import 시점에 설정하며, litellm이 GitHub에서 비용 데이터를 받아 오는 것을 막는다. |

`.env.example`이 `Settings`가 무시하는 값을 문서화하지 않는지 테스트가 확인한다. 그 검사가
있는 이유는 설정 네 개(`FLUSH_INTERVAL_SECONDS`, `FLUSH_MAX_SIZE`, `LOOKBACK_*`)가 한때
문서에는 있는데 읽히지는 않았기 때문이다 — 운영자가 값을 넣어도 하드코딩된 기본값이
쓰였고 아무 경고도 없었다.

### 고정 한도

환경 변수가 아니라 컴파일 시점 상수다. 몇몇은 조용히 작업을 버리거나 거절할 수 있으므로,
로그를 읽는 운영자를 위해 여기 적는다.

런타임 상한은 대부분 `application/service/guardrails.py` 한 곳에 있다. Agent가 우회할 수
있는 자리에 두지 않는다.

| 한도 | 값 | 정의 위치 | 비고 |
|---|---|---|---|
| 분석 윈도 | 10분 | `domain/model/time_range.py` (`MAX_TIME_RANGE_DURATION`) | ClickHouse 팬아웃과 호출당 LLM 비용을 묶는다. 더 긴 제안은 거절하지 않고 **쪼갠다** — 상한의 근거는 한 번의 조회 비용이지 "그 시간대를 보면 안 된다"가 아니다. |
| Incident당 분석한 분 | 60분 | `guardrails.py` (`MAX_ANALYZED_MINUTES`) | **주 예산.** 남은 예산보다 긴 요청은 앞쪽만 잘라 분석한다. 통째로 거절하면 남은 예산을 못 쓴 채 끝난다. |
| Incident당 분석 호출 | 12회 | 같은 파일 (`MAX_ANALYSIS_CALLS`) | 2차 안전장치. 1분짜리 요청을 수십 번 하는 폭주는 분 예산만으로는 늦게 잡힌다. |
| Supervisor 사이클 | 16회, 연속 거절 3회 | 같은 파일 (`MAX_SUPERVISOR_CYCLES`, `MAX_REJECTED_DECISIONS`) | 거절된 사이클은 분석을 하지 않아 예산을 늘리지 않는다. 모델이 같은 요청을 되풀이하면 거절 상한에서 끊는다. |
| 같은 구간 재요청 | 거절 | 같은 파일 (`check_not_duplicate`) | 완전히 같은 구간만이 아니라 **덮인 구간**을 막는다. 14:00~14:10을 본 뒤 14:02~14:05는 새로 얻는 것이 없다. 부분 중복은 아직 안 본 부분만 잘라 통과시킨다. |
| 리포트 수정 | 1회 | 같은 파일 (`MAX_REPORT_REVISIONS`) | 넘으면 남은 불일치를 리포트에 기록한 채 끝낸다. 모델이 지적을 이해하지 못하면 검증과 수정이 서로를 부르며 끝나지 않는다. |
| 유입 대기 | 1회 60초, 누적 300초 | 같은 파일 (`MAX_SINGLE_WAIT_SECONDS`, `MAX_TOTAL_WAIT_SECONDS`) | 기다리는 동안 분석은 시작되지 않았으므로 큐만 늘어난다. |
| Incident 실행 시간 | 30분 | 같은 파일 (`INCIDENT_TIMEOUT_SECONDS`) | 대기·조회·LLM이 각자 상한을 가져도 그 곱은 묶이지 않는다. |
| Evidence 수 | 소스당 25, 전체 80 | 같은 파일 (`MAX_EVIDENCE_PER_SOURCE`, `MAX_EVIDENCE_TOTAL`) | Reduce가 아무것도 걸러 내지 못하면 Cross-source 프롬프트가 원문 크기로 돌아간다. 잘린 사실은 `WARNING`으로 남는다. |
| 한 번에 조사할 노드 | 2대 | `.../diagnosis/node_investigation.py` (`MAX_NODES_PER_ANALYSIS`) | 후보가 많다는 것은 대개 클러스터 전체가 흔들렸다는 뜻이고, 그때는 마스터 로그 쪽이 더 말해 준다. |
| 연속 재트리거 | 3회 | `application/service/slowlog_trigger_service.py` (`_MAX_CONSECUTIVE_RETRIGGERS`) | 성공한 실행만 재트리거하고, 매번 `MICRO_BATCH_SECONDS`를 먼저 기다린다. |
| 분 구간·소스당 행 수 | 10,000 | `.../clickhouse/clickhouse_log_adapter.py` (`_MAX_ROWS_PER_SEGMENT_PER_SOURCE`) | 1분 구간마다 소스별 질의에 `LIMIT`으로 걸린다. `ORDER BY`가 없으므로 상한에 닿으면 ClickHouse가 임의의 부분집합을 돌려주고, 보고되는 건수도 잘린 값이다. 상한에 닿은 질의마다 소스와 구간을 적은 `WARNING`이 남는다. |
| 쿼리 로그 줄당 키워드 | 5개 | `.../agent/common/log_format.py` (`_MAX_KEYWORDS_SHOWN`) | 어떤 쿼리는 키워드가 200개가 넘어 프롬프트 한 줄이 2,000자를 넘는다. 나머지는 `외 N개`로 요약하고, 도메인 객체는 전부 들고 있다. |
| 구간당 마스터 로그 줄 | 300 | `.../workflows/datasource/master_log.py` (`MASTER_LOG_MAX_LINES`) | 로거 화이트리스트가 이미 걸러 주고, Triage가 한 번 더 줄인다. |
| 리포트에 싣는 마스터 로그 줄 | 120 | `.../diagnosis/run_state.py` (`MASTER_LOG_REPORT_MAX`) | 잘린 사실은 `"N건 중 M건"`으로 머리글에 드러난다. |
| 노드 로그 조회 행 | 기본 300, 하드 캡 2,000 | `application/port/outbound/log_repository.py` (`DEFAULT_NODE_LOG_LIMIT`, `MAX_NODE_LOG_LIMIT`) | 클램프가 포트에 있어 호출부와 어댑터가 같은 유효값을 본다. `ORDER BY timestamp`라 **가장 이른** 행이 남는다. 사고는 시작이 끝보다 중요하다. |
| SSH 노드 로그 줄 | 호출당 300, 서버에서 `tail -n 2000` | `.../ssh/node_log_fetcher.py` | ClickHouse 경로와 반대 편향 — `tail`은 **가장 늦은** 줄을 남긴다. |
| SSH 명령 | `grep`/`tail`만 | 같은 파일 (`_ALLOWED_COMMANDS`) | 조립된 명령의 파이프라인 각 단계와 파일 경로 문자를 검사한다. ES 응답도 노드 설정도 이 프로세스가 통제하지 않는 입력이다. |
| SSH 타임아웃 | 접속 10초, 명령 30초 | `guardrails.py` (`SSH_*_TIMEOUT_SECONDS`) | |
| 프롬프트에 싣는 원문 | 60,000자 | `guardrails.py` (`MAX_RAW_LOG_CHARS`) | 잘린 사실을 본문에 적는다. 조용히 자르면 검증이 "인용이 원문에 없다"고 잘못 말한다. |
| LLM 출력 토큰 | Map 1,024 · Reduce 2,048 · RCA 8,192 · Decision 2,048 | `triage/nodes.py`, `diagnosis/agent.py`, `supervisor/agent.py` | |
| LLM 요청 타임아웃 | 120초 | `.../agent/common/litellm_client.py` (`_REQUEST_TIMEOUT_SECONDS`) | |
| ClickHouse 송수신 타임아웃 | 30초 | `infrastructure/config/dependencies.py` | 없으면 멈춘 ClickHouse가 워커 스레드를 무한정 붙잡을 수 있다. |

### 재시도를 끈 것은 의도다

LLM 호출은 전부 `litellm.completion(num_retries=0)` 하나를 지난다. 재시도가 꺼져 있다.

이 경로의 실패는 압도적으로 429 — 레이트 리밋이다. 같은 거대한 프롬프트를 다시 보내도
성공할 수 없다. 실패한 것은 요청이 아니라 **할당량**이기 때문이고, 소비만 배로 는다.
Gemini 무료 등급의 **분당 입력 토큰 250,000**에 대고 재 보면, 5분 구간 한 번이 513,122
입력 토큰이고 재시도를 포함하면 2,052,488 — 그 분 할당량의 821%가 된다. 게다가 증폭이 두
번 곱해진다. 분마다 호출이 나가고, 성공한 실행은 최대 세 번까지 재트리거된다.

> 위 숫자는 Gemini 것이다. NVIDIA NIM의 한도는 다르고 여기 문서화하지 않았다.

`LlmApiError`는 상태 코드만 들고 있으므로(응답 본문과 요청 URL에는 API 키가 들어갈 수 있다)
429만으로는 *어느* 한도에 걸렸는지 알 수 없다. 그래서 클라이언트가 화이트리스트에 있는
레이트 리밋 헤더만 로그로 남기고 예외를 올린다.

```
WARNING  LLM provider=nvidia_nim rejected the request with status=429;
         retry-after=40 x-ratelimit-limit-tokens=250000 x-ratelimit-remaining-tokens=0
```

`x-ratelimit-*-tokens`는 프롬프트가 너무 크다는 뜻이고, `x-ratelimit-*-requests`는 호출이
너무 붙어 있다는 뜻이다. 예외에서 그 밖의 것은 아무것도 로그에 남기지 않는다 — 본문도,
URL도, `str(exc)`도.

재시도를 끈 대가는 일시적인 5xx도 재시도되지 않는다는 것이다. 감당할 만하다. 실패한 분은
실행을 무너뜨리지 않고 `[분석 실패]`로 내려앉기 때문이다.

### 추론은 프롬프트로 유도한다

`google/gemma-4-31b-it`은 `reasoning_effort`를 지원하지 않아 thinking budget으로 추론을
올릴 수 없다. 대신 Cross-source 프롬프트가 순서를 지시한다 — Observation → Timeline →
Hypothesis → Supporting → Counter → Conclusion.

`counter_evidence_refs`가 **필드로** 있는 것이 그 지시를 강제하는 부분이다. 반증을 쓸 자리를
만들어 두지 않으면 모델은 반증을 쓰지 않고, 그러면 근거가 얇은 결론과 두꺼운 결론이
리포트에서 똑같아 보인다.

이것을 호출 두 번으로 나누는 것(추론용 한 번, 작성용 한 번)은 기각했다. 입력 토큰이 두
배가 되고, 그 두 배가 재트리거로 다시 곱해진다.

## 설치

```
uv sync
```

## 실행

```
uv run python -m cluster_doctor.main
```

Kafka 컨슈머가 뜨고 블록된다. HTTP 포트는 없다. 로그는 stderr와 `logs/app.log`로, 진단
리포트는 `REPORT_DIR`에 HTML로 나간다.

실제 슬로우 로그를 기다리지 않고 돌려 보려면 합성 메시지를 발행한다.

```
uv run python scripts/produce_test_message.py --at "2026-09-16T04:22:00" --count 3
```

Kafka도 본 프로세스도 없이 **특정 시간대만** 진단해 보려면 독립 실행 스크립트를 쓴다.
`--dry-run`은 어느 구간이 잡히는지만 보여 주고 아무것도 호출하지 않는다.

```
uv run python scripts/run_diagnosis.py --at "2026-09-16T04:22:00" --span 6m --dry-run
uv run python scripts/run_diagnosis.py --at "2026-09-16T04:22:00" --span 6m
```

실행이 끝나면 리포트 경로와 함께 호출별 입력 토큰이 찍힌다 — 오케스트레이터의 입력 토큰
증가가 곧 컨텍스트 누적이다.

## 테스트

```
uv run pytest -q
```

479개, 약 5초. 실제 LLM·Kafka·ClickHouse·Elasticsearch·SSH를 건드리는 테스트는 없다.

## 데이터 소스

구간 하나를 분석할 때 ClickHouse 테이블 셋을 읽어 하나의 타임라인으로 합친다. 네 번째는
노드 로그를 담고 따로 조회된다.

| 테이블 | 시각 컬럼 | 타입 | 담는 것 |
|---|---|---|---|
| `slowlog_v2` | `_source.@timestamp` | `DateTime64(9)` (JSON 하위 컬럼) | ES 슬로우 로그 임계값을 넘은 쿼리: 인덱스, 노드, `took`, 히트 수, 샤드 수, 쿼리 원문, service/project/company/user를 담은 `x-opaque-id` 헤더 |
| `log` | `reg_date` | `DateTime('Asia/Seoul')` | 모든 ES 쿼리 실행: 호스트, 실행 시간, 성공 여부, 명령, 키워드, company, user |
| `es_node_metric` | `reg_date` | `DateTime('Asia/Seoul')` | 노드별 CPU, 메모리, JVM heap, search/write 큐와 거절 건수 |
| `loki_logs` | `timestamp` | `DateTime64(9, 'Asia/Seoul')` | 노드 로그 줄: `node`, `node_role`, `level`, `detected_level`, `logger`, `filename`, `host`, `line`. **현재는 마스터 노드만.** |

**시간대는 전부 KST다.** ClickHouse 서버의 `timezone()`이 `Asia/Seoul`이고, `slowlog_v2`의
컬럼은 지정하지 않음으로써 그것을 물려받는다. 전부 timezone-aware KST 값을 돌려주므로
병합과 정렬이 안전하다.

`loki_logs`와 `slowlog_v2`는 상시 적재되는 로그가 아니라 **사건 기반** 로그다. 평시 0건이
정상이고 사고 때 몰린다.

### 노드 메트릭 읽는 법

`os_mem`은 `GET _nodes/stats`의 `os.mem.used_percent` — **페이지 캐시를 포함한 OS
메모리**다. Elasticsearch는 남는 RAM을 파일시스템 캐시로 일부러 쓰므로 95~99%는 정상이지
증상이 아니다. 리포트가 한 번 그것을 "메모리 사용률이 95%~99%로 매우 높게 유지됨"이라고
짚었고, 그래서 지금은 `os_mem(캐시포함)=98%`로 그리고 프롬프트 세 곳이 명시한다. 진짜 메모리
압박은 `jvm_heap`과 GC 경고 또는 `search_rejected`가 함께 나타나는 것이다.

`_source`는 문서 전체가 아니라 이름 붙인 JSON 하위 컬럼만 선택한다 — 전체 문서는 행당
2,668자이고 그중 진단에 쓰이는 것은 약 27%다. 나머지(`host.mac`, `agent.ephemeral_id`,
`host.os.kernel`, …)는 아무것도 아닌 데 쓰는 프롬프트 토큰이다.

## 배포 메모

실제 설치에 대고 측정한 것이지 문서에서 유추한 것이 아니다.

### 망 분리 환경의 최초 기동

`import litellm`이 tiktoken `cl100k_base` 인코딩을 `openaipublic.blob.core.windows.net`에서
받아 온다. **그 요청에는 예외 처리가 없다.** 캐시가 비어 있고 그 호스트로 가는 경로가
없으면 import가 약 19.5초 멈췄다가 처리되지 않은 `ProxyError`로 죽는다 — 앱이 아예 뜨지
않고, 실패 모습이 설정 문제와 전혀 닮지 않았다.

컨테이너 이미지를 만든다면 빌드 시점에 캐시를 데운다.

    RUN python -c "import litellm"

그러지 않으면 최초 기동에 그 호스트 접근이 필요하다. 캐시는 디스크에 남으므로 이후
기동에는 필요 없다.

`LITELLM_LOCAL_MODEL_COST_MAP=True`는 비용 데이터를 받으려는 **별개의** GitHub 호출을
없앤다. tiktoken 쪽에는 도움이 되지 않는다.

### 이미지 줄이기 (선택)

litellm이 이 서비스가 건드리지 않는 약 39MB를 함께 담는다.

    RUN rm -rf /path/to/site-packages/litellm/proxy/_experimental/out \
               /path/to/site-packages/litellm/proxy/swagger \
               /path/to/site-packages/litellm/rust_bridge/_native.pyd || true

`|| true`는 의도적이다. litellm이 구조를 바꾸면 이 단계는 **빌드를 깨는** 대신 조용히
아무 일도 하지 않아야 한다.

`litellm/proxy/`의 나머지는 지우지 않는다. 관리자 전용 도구처럼 보이지만 실제
`completion()` 호출이 그쪽을 지나간다.

디스크만 아낀다. 그 파일들은 import 경로에 없으므로 기동 시간은 그대로다.

## 알려진 한계

- **무료 등급의 토큰 할당량은 여전히 닿을 수 있다.** 바쁜 5분 구간이 입력 토큰 약
  513,000을 쓰는데 상한은 분당 250,000이다. 재시도와 재트리거 폭주는 막았지만 프롬프트
  크기 자체는 그대로다. 실측 기준 토큰의 약 80%가 분 단위/종합 파이프라인에서 나가고
  오케스트레이터는 나머지다 — 줄일 여지는 파이프라인 쪽에 있고, 남은 지렛대는
  `es_query_log`를 실행 시간으로 거르는 것과, Evidence 수가 아니라 실제 토큰으로 프롬프트를
  자르는 것이다.
- **모델이 판단 필드를 비우는 빈도가 일정하지 않다.** 같은 구간을 세 번 돌렸을 때 9개 중
  8개 → 3개 → 0개가 찼다. 프롬프트를 세 번 고쳤고 그때마다 일시적으로만 나아졌다. 지금
  구조는 모델이 안 써도 리포트가 무너지지 않고, 안 썼다는 사실이 배너로 도달한다. 근본
  해결은 아니다.
- **유입 정착 확인이 분석 전에 큐를 비운다.** 그 뒤 실행이 심하게 죽으면 비워 낸 항목은
  사라지고 재트리거도 걸리지 않는다. 사고는 다음 슬로우 로그를 기다린다.
- **마스터 로그 0건과 "아직 적재되지 않음"은 구별되지 않는다.** SSH 폴백은 ClickHouse
  질의가 *실패*했을 때만 작동한다. 로거 화이트리스트가 걸린 상태에서 0건은 건강한 구간의
  정상 결과이고, 0건마다 폴백하면 분석할 때마다 ES 왕복과 새 SSH 접속을 치르게 된다.
  대가는 적재 공백이 "마스터 쪽 이벤트 없음"으로 읽힌다는 것이다.
- **데이터 노드 로그는 SSH 설정에 달려 있고, 망 구성에도 달려 있다.** `SSH_USER`가 비어도
  기동 시점에 아무 경고가 없고, 에이전트가 그 단계에 닿아야 빈 곳이 드러난다. 더해서 ES
  `_nodes` API는 **클러스터 내부 주소**를 알려 주므로, 실행 호스트가 그 망 밖에 있으면 SSH가
  반드시 실패한다(실측: 노드 108대 전부 `192.168.x.x`). 지금은 연결 타임아웃 10초를 그대로
  기다린 뒤 `gaps`로 남는다.
