# ClusterGuard

ClusterGuard는 Elasticsearch 클러스터를 스스로 진단한다. Kafka에서 ES 슬로우 로그를
실시간으로 받아 유입이 멎기를 기다린 뒤, LLM 에이전트가 무엇을 조사할지 직접 정한다 —
클러스터 상태를 확인하고, 해당 구간의 ClickHouse 행을 끌어와 분 단위로 분석하고, 단서가
가리키는 노드의 로그를 읽는다. 결과는 HTML 파일 한 장으로 남는다.

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
   에이전트를 **한 번** 띄운다. 슬로우 로그는 몰려서 오므로 건건이 진단하면 같은 사고를
   수십 번 분석하게 된다.
2. **유입이 멎기를 기다린다** — 에이전트가 `sleep(30)`과 `check_new_slowlogs()`를 번갈아
   부르고, **연속 두 번** 신규 0건을 본 뒤에야 다음으로 간다. 한 번으로는 부족하다 —
   커넥터의 폴링 간격 때문에 사고 도중에도 잠깐 잠잠해 보이는 구간이 생긴다. 상한 5분.
3. **분석** — `analyze_logs(start, end)`가 그 구간에 대해 ClickHouse 세 소스를 조회하고,
   1분 버킷으로 쪼개 비어 있지 않은 버킷마다 LLM을 한 번씩 부른 뒤, 마지막으로 한 번 더
   불러 버킷들을 리포트로 종합한다. 같은 호출이 그 구간의 마스터 노드 로그도 함께 끌어와,
   종합 단계가 분 단위 급증과 나란히 놓고 볼 수 있게 한다.
4. **추가 조사** — 클러스터가 green이 아니거나 특정 노드가 지목됐을 때만. 할당 실패 설명,
   노드 로그("노드 로그는 두 경로로 온다" 참고).
5. **리포트** — `HtmlFileNotifier`가 진단 한 건당 HTML 파일 하나를 `REPORT_DIR`에 쓴다.

에이전트가 할 수 있는 일은 tool 일곱 개가 전부다: `analyze_logs`, `check_new_slowlogs`,
`sleep`, `cluster_health`, `explain_unassigned_shards`, `search_node_logs`, `get_node_logs`.
파일시스템 접근은 아예 막혀 있다.

**한도는 프롬프트가 아니라 tool이 강제한다.** 프롬프트로 "`analyze_logs`는 여섯 번까지"라고
말해도 모델은 무시할 수 있고, deepagents의 `recursion_limit`은 9,999라 프레임워크도 막아
주지 않는다. tool이 저마다 자기 예산을 자른다.

### 왜 분 단위로 쪼개는가

구간 전체를 프롬프트 하나에 담으면 결국 표본을 뽑아야 하고, 표본 추출은 **가장 오래된
로그부터** 잃는다. 분으로 쪼개면 각 버킷이 상한 아래에 들어가므로 모든 줄이 모델에 닿는다.
종합 단계는 분별 요약과 각 분이 고른 근거 줄만 보고, 원본 로그는 다시 보지 않는다 — 다시
보면 잘라내기가 되살아난다.

비어 있는 분은 건너뛰고 비용이 들지 않는다. 한 분이 실패해도(레이트 리밋, 필터링된 응답)
진단이 가라앉지 않는다. 그 분은 `[분석 실패]`로 표시되고 종합 프롬프트에 그 사실이
전달되므로, 빈 구간이 감춰지지 않고 리포트에 드러난다. **모든** 분이 실패하면 "이상
없음"처럼 읽히는 것을 돌려주는 대신 실행이 실패한다.

### 노드 로그는 두 경로로 온다

마스터 노드 로그는 ClickHouse에 있고 데이터 노드 로그는 없다. 그래서 에이전트가 두 길을
쓰고, 프롬프트가 이 순서로 안내한다.

| | 소스 | tool | 이유 |
|---|---|---|---|
| 마스터 노드 | ClickHouse 노드 로그 테이블 | `search_node_logs` | SSH가 없고, IP를 얻는 `GET /_nodes` 왕복이 없고, severity 정규식 대신 `level` 컬럼을 쓴다 |
| 데이터 노드 | 노드로 SSH | `get_node_logs` | 그 로그는 어디에도 적재되지 않는다 |

둘을 잇는 것은 마스터 로그 자체다. 클러스터 수준 이벤트가 문제가 된 노드 이름을 찍어 주고,
`get_node_logs(node_id)`가 그 이름을 `GET /_nodes/<id>`로 풀어(IP, 로그 디렉터리, 클러스터
이름) SSH 연결까지 직접 연다. tool 호출 한 번이면 되고 별도 조회가 없다.

`analyze_logs`도 자기 구간의 마스터 로그를 자동으로 끌어와 종합 단계에 넘긴다 — 마스터
이벤트와 분 단위 슬로우 로그 급증을 **같은 모델 앞에** 놓는 유일한 경로이고, "상관관계를
설명하라"는 요구에 필요한 것이 그것이다.

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

진단 한 건당 HTML 파일 하나를 `HtmlFileNotifier`가 쓴다.

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

### 모델이 판단을 남기지 못해도 리포트는 나간다

구조화 출력은 실패 갈래를 늘린다. langchain은 이 모델(ChatLiteLLM + nvidia_nim)에 대해
native json_schema를 쓰지 않고 `ToolStrategy`로 떨어지는데, 그것은 스키마가 **평범한 tool
하나로 바인딩될 뿐**이라는 뜻이다. 모델이 그 tool을 부르지 않고 평문으로 끝낼 수 있다.

그래서 사다리를 명시한다.

| | 상황 | 결과 |
|---|---|---|
| 1단 | 구조화 성공 | `narrative` 채움. 정상 |
| 2단 | 모델이 평문으로만 끝냄 | 평문을 싣고 `gaps`에 기록. 분석 실패는 아니다 |
| 3단 | 모델이 아무것도 안 남김 | 관측값만으로 리포트. `analysis_failed=True` |
| 4단 | 관측값마저 없음 | 그때만 예외 |

**어느 단이든 관측값 섹션은 항상 그려진다.** 2단에서도 관측값이 하나도 없으면 — 즉
`analyze_logs`가 한 번도 성공하지 않았으면 — 평문은 싣되 분석 실패로 표시한다. 근거가
하나도 없는 판단이 정상 진단으로 나가면 안 된다.

**형식이 맞아도 판단이 빌 수 있다.** 같은 구간을 세 번 돌렸더니 모델이 채운 판단 필드가
9개 중 8개 → 3개 → 0개로 들쭉날쭉했다(프롬프트에 "반드시 채운다"를 넣은 뒤에도). 사다리는
이것을 잡을 수 없다 — 구조화는 성공했고 관측값도 온전하니 1단이 맞다. 그래서 등급을 바꾸는
대신 `gaps`에 사실을 남기고, notifier가 배너로 그린다.

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

`analyze`는 문자열이 아니라 `DiagnosisResult`를 돌려주고, 그것이 예전 설계가 뭉쳐 놓았던 두
결정을 갈라놓는다.

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

- `analyze_logs` 실패 → `analysis_failed`. 리포트는 그래도 나가되 "결론을 신뢰할 수 없다"는
  붉은 배너가 붙고, 재트리거하지 않는다.
- 노드 로그 수집 실패, `analyze_logs` 호출 상한 도달, 일부 분 버킷 실패 → `gaps` 항목.
  리포트는 정상적으로 나가고 재트리거도 **한다**. 분석 자체는 성공했기 때문이다.

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
- **Elasticsearch** (에이전트의 첫 단계가 `cluster_health()`다)
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
| `MICRO_BATCH_SECONDS` | 아니오 | `10` | 에이전트를 띄우기 전에 도착분을 모으는 시간. |
| `REPORT_DIR` | 아니오 | `reports` | 진단 HTML을 쓰는 곳. 진단 한 건당 파일 하나. 상대 경로는 작업 디렉터리 기준. |
| `LITELLM_LOCAL_MODEL_COST_MAP` | 아니오 | `True` | `Settings`가 아니라 litellm이 직접 읽는다. `litellm_client.py`가 import 시점에 설정하며, litellm이 GitHub에서 비용 데이터를 받아 오는 것을 막는다. |

`.env.example`이 `Settings`가 무시하는 값을 문서화하지 않는지 테스트가 확인한다. 그 검사가
있는 이유는 설정 네 개(`FLUSH_INTERVAL_SECONDS`, `FLUSH_MAX_SIZE`, `LOOKBACK_*`)가 한때
문서에는 있는데 읽히지는 않았기 때문이다 — 운영자가 값을 넣어도 하드코딩된 기본값이
쓰였고 아무 경고도 없었다.

### 고정 한도

환경 변수가 아니라 컴파일 시점 상수다. 몇몇은 조용히 작업을 버리거나 거절할 수 있으므로,
로그를 읽는 운영자를 위해 여기 적는다.

| 한도 | 값 | 정의 위치 | 비고 |
|---|---|---|---|
| 분석 윈도 | 10분 | `domain/model/time_range.py` (`MAX_TIME_RANGE_DURATION`) | `analyze_logs`가 더 긴 구간을 거절하고 좁히라고 알린다. ClickHouse 팬아웃과 호출당 LLM 비용을 묶는다. |
| 실행당 `analyze_logs` 호출 | 6회 | `.../deepagent/tools.py` (`_MAX_ANALYZE_CALLS`) | 가장 비싼 tool — 한 번 부르면 구간의 분 수만큼 LLM 요청이 나간다. 상한을 넘으면 아무것도 조회하지 않고 거절 문자열을 돌려준다. |
| 같은 구간 재요청 | 거절 | 같은 파일 | 이미 분석한 `(start, end)`를 다시 부르면 조회하지 않고 거절한다. 반환값이 같으므로 새로 얻는 것이 없고 호출 예산만 태운다(실측으로 같은 구간이 연달아 두 번 요청됐다). |
| 에이전트 대기 예산 | `sleep` 당 60초, 누적 300초 | 같은 파일 (`_MAX_SLEEP_SECONDS`, `_MAX_WAIT_SECONDS`) | 기다리는 동안 분석은 시작되지 않았으므로 큐만 늘어난다. 누적 상한을 넘으면 `sleep`이 기다리지 않고 즉시 반환한다. |
| 연속 재트리거 | 3회 | `application/service/slowlog_trigger_service.py` (`_MAX_CONSECUTIVE_RETRIGGERS`) | 성공한 실행만 재트리거하고, 매번 `MICRO_BATCH_SECONDS`를 먼저 기다린다. |
| 분 구간·소스당 행 수 | 10,000 | `.../clickhouse/clickhouse_log_adapter.py` (`_MAX_ROWS_PER_SEGMENT_PER_SOURCE`) | 1분 구간마다 소스별 질의에 `LIMIT`으로 걸린다. `ORDER BY`가 없으므로 상한에 닿으면 ClickHouse가 임의의 부분집합을 돌려주고, LLM에 보고되는 건수도 잘린 값이다. 상한에 닿은 질의마다 소스와 구간을 적은 `WARNING`이 남는다. |
| 쿼리 로그 줄당 키워드 | 5개 | `.../langgraph/prompts.py` (`_MAX_KEYWORDS_SHOWN`) | 어떤 쿼리는 키워드가 200개가 넘어 프롬프트 한 줄이 2,000자를 넘는다. 나머지는 `외 N개`로 요약하고, 도메인 객체는 전부 들고 있다. |
| `analyze_logs` 호출당 마스터 로그 줄 | 80 | `.../deepagent/tools.py` (`_MASTER_LOG_MAX_LINES`) | 호출마다 종합 프롬프트에 실리므로 최대 6배가 된다. 로거 화이트리스트가 이미 걸러 주므로 좁게 잡는다. |
| 리포트에 싣는 마스터 로그 줄 | 120 | `.../deepagent/analyzer.py` (`_MASTER_LOG_REPORT_MAX`) | 잘린 사실은 `"N건 중 M건"`으로 머리글에 드러난다. |
| `search_node_logs` 호출당 노드 로그 행 | 기본 300, 하드 캡 2,000 | `application/port/outbound/log_repository.py` (`DEFAULT_NODE_LOG_LIMIT`, `MAX_NODE_LOG_LIMIT`) | 클램프가 포트에 있어 tool과 어댑터가 같은 유효값을 본다 — 아니면 5,000을 요청했을 때 2,000행이 오는데 tool은 잘렸다고 보고하지 않는다. `ORDER BY timestamp`라 **가장 이른** 행이 남는다. 사고는 시작이 끝보다 중요하다. |
| SSH 노드 로그 줄 | 호출당 300, 서버에서 `tail -n 2000` | `.../ssh/node_log_fetcher.py` | ClickHouse 경로와 반대 편향 — `tail`은 **가장 늦은** 줄을 남긴다. |
| LLM 출력 토큰 | 분당 1,024, 종합 8,192 | `.../langgraph/nodes.py` | |
| LLM 요청 타임아웃 | 120초 | `.../llm/litellm_client.py` (`_REQUEST_TIMEOUT_SECONDS`) | |
| ClickHouse 송수신 타임아웃 | 30초 | `infrastructure/config/dependencies.py` | 없으면 멈춘 ClickHouse가 워커 스레드를 무한정 붙잡을 수 있다. |

### 재시도를 끈 것은 의도다

LLM 두 계층 모두 재시도가 꺼져 있다 — litellm은 `num_retries=0`, `ChatLiteLLM`은
`max_retries=0`. (오케스트레이터가 `ChatGoogleGenerativeAI`로 돌 때는 `1`이었다. 그
라이브러리는 `0`을 "Google SDK 기본값을 쓰라"로 읽기 때문이다. `ChatLiteLLM`에는 그런
특수 해석이 없으므로 `0`이 곧 재시도 없음이다.)

이 경로의 실패는 압도적으로 429 — 레이트 리밋이다. 같은 거대한 프롬프트를 다시 보내도
성공할 수 없다. 실패한 것은 요청이 아니라 **할당량**이기 때문이고, 소비만 배로 는다.
Gemini 무료 등급의 **분당 입력 토큰 250,000**에 대고 재 보면, 5분 구간 한 번이 513,122
입력 토큰이고 재시도를 포함하면 2,052,488 — 그 분 할당량의 821%가 된다. 게다가 증폭이 두
번 곱해진다. 오케스트레이터는 tool 루프이고, 성공한 실행은 최대 세 번까지 재트리거된다.

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

### 섹션 나눈 CoT

`google/gemma-4-31b-it`은 `reasoning_effort`를 지원하지 않아 thinking budget으로 추론을
올릴 수 없다. 대신 종합 프롬프트가 블록 두 개를 요구하고 — `===추론===` 다음
`===리포트===` — `_strip_reasoning`이 두 번째만 남긴다. 운영자는 리포트를 보고 작업 과정은
보지 않는다. 구분자가 없으면 응답 전체를 쓰되 경고를 남긴다. 추론이 섞인 리포트가 빈
리포트보다 낫다.

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

353개, 약 7초. 실제 LLM·Kafka·ClickHouse·Elasticsearch·SSH를 건드리는 테스트는 없다.

## 데이터 소스

`analyze_logs`가 ClickHouse 테이블 셋을 읽어 하나의 타임라인으로 합친다. 네 번째는 노드
로그를 담고 따로 조회된다.

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
  `es_query_log`를 실행 시간으로 거르는 것이다.
- **모델이 판단 필드를 비우는 빈도가 일정하지 않다.** 같은 구간을 세 번 돌렸을 때 9개 중
  8개 → 3개 → 0개가 찼다. 프롬프트를 세 번 고쳤고 그때마다 일시적으로만 나아졌다. 지금
  구조는 모델이 안 써도 리포트가 무너지지 않고, 안 썼다는 사실이 배너로 도달한다. 근본
  해결은 아니다.
- **`check_new_slowlogs`는 분석이 시작되기 전에 큐를 비운다.** 그 뒤 실행이 심하게 죽으면
  비워 낸 항목은 사라지고 재트리거도 걸리지 않는다. 사고는 다음 슬로우 로그를 기다린다.
- **마스터 로그 0건과 "아직 적재되지 않음"은 구별되지 않는다.** `analyze_logs` 안의 SSH
  폴백은 ClickHouse 질의가 *실패*했을 때만 작동한다. 로거 화이트리스트가 걸린 상태에서
  0건은 건강한 구간의 정상 결과이고, 0건마다 폴백하면 최대 여섯 번의 호출마다 ES 왕복과 새
  SSH 접속을 치르게 된다. 대가는 적재 공백이 "마스터 쪽 이벤트 없음"으로 읽힌다는 것이다.
- **데이터 노드 로그는 SSH 설정에 달려 있고, 망 구성에도 달려 있다.** `SSH_USER`가 비어도
  기동 시점에 아무 경고가 없고, 에이전트가 그 단계에 닿아야 빈 곳이 드러난다. 더해서 ES
  `_nodes` API는 **클러스터 내부 주소**를 알려 주므로, 실행 호스트가 그 망 밖에 있으면 SSH가
  반드시 실패한다(실측: 노드 108대 전부 `192.168.x.x`). 지금은 연결 타임아웃 10초를 그대로
  기다린 뒤 `gaps`로 남는다.
