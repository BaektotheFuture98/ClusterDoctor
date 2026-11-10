# ClusterGuard

Elasticsearch 클러스터의 문제성 로그 신호(Kafka)를 받아 ClickHouse·ES·SSH 근거를 모으고, LLM으로 분석·검증해 HTML 보고서를 만드는 진단 서비스다. 동작 흐름, 설정 변수, 고정 한도, 보고서 형식은 [README.md](README.md)가 기준이다. 여기에는 README와 코드로는 알기 어려운 규칙만 둔다.

## 구조

`src/cluster_doctor/` 아래 패키지와 문서다. 패키지 문서는 그 폴더의 파일을 읽을 때 자동으로 로드되며 목표·흐름·불변식·후속 작업을 담는다. 이 파일에는 여러 패키지에 걸치는 규칙만 둔다.

| 패키지 | 역할 | 문서 |
|---|---|---|
| `main.py` | 조립(Composition Root) | |
| `bootstrap/` | `settings.py`, `resources.py`(공유 client 수명) | |
| `kafka_consumer/` | Kafka 수신, 유입 정착 대기 | [CLAUDE.md](src/cluster_doctor/kafka_consumer/CLAUDE.md) |
| `incident_orchestrator_agent/` | Main Agent: 구간 선택·사건 수명·보고서 발행 | [CLAUDE.md](src/cluster_doctor/incident_orchestrator_agent/CLAUDE.md) |
| `incident_analysis_agent/` | Analysis SubAgent: 근거 수집·분석·검증·사건 종합 | [CLAUDE.md](src/cluster_doctor/incident_analysis_agent/CLAUDE.md) |
| `wiki_investigation_agent/` | 장애 패턴 위키(MCP) 조사 에이전트 | [CLAUDE.md](src/cluster_doctor/wiki_investigation_agent/CLAUDE.md) |

- 폴더는 구현 기술이 아니라 **역할과 owner**를 따른다. `model/`은 owner 안의 여러 component가 공유하는 데이터, State는 실행 owner 가까이(`agent/state.py`), LLM 응답 schema는 그 service/workflow 가까이에 둔다.
- 저장소 관례: 도구 루프 에이전트는 `agent/`, 고정 그래프는 `workflow/`, 직접 호출은 `service/`.
- Orchestrator가 Analysis를 호출하고 Analysis는 Orchestrator에 의존하지 않는다. `aiokafka`는 `kafka_consumer/consumer/kafka`에만 둔다.
- 프로세스 공용 LLM 인프라가 두 패키지에 흩어져 있다. `incident_analysis_agent/agent/runtime/`(litellm 클라이언트, 재시도, 가명화, 호출 로그, harness, tool_batch)은 orchestrator·위키 에이전트가 거꾸로 참조하고, `incident_orchestrator_agent/agent/chat_model.py`(Main·Analysis·위키 에이전트가 함께 쓰는 chat model)는 orchestrator 안에 있다. 위키 에이전트와 분석 쪽도 서로 참조하는 순환이 있다. 최상위 `cluster_doctor/llm/`로 분리하는 것이 후속 과제다.
- `chat_model.py`는 `litellm_client`와 자체 재시도 0회(429 증폭 방지, 재시도는 `RateLimitRetryMiddleware`), 요청 타임아웃, 샘플링 인자 미전송을 같게 유지한다. 값을 옮겨 적지 않고 import해서 쓴다.

## 설계 원칙

- **State**: 실행 owner마다 mutable source of truth 하나(`MainAgentState`, `AnalysisAgentState`, `MinuteAnalysisState`). Agent State 안에 다른 mutable State를 넣지 않는다. 도구는 `ToolRuntime.state`를 읽고 `Command(update=...)`로 값 교체와 matching `ToolMessage`를 반환한다. tuple/frozen snapshot을 쓰고 in-place 변경하지 않는다.
- 한 AI turn에서는 첫 상태 변경 도구만 실행하고 나머지는 오류 `ToolMessage`로 응답한다. Main의 readonly 조회만 병렬 가능하다.
- 실행 객체·model·seams·공유 client는 기동 시 한 번 만든다. Main graph·State는 사건마다, Analysis graph·State는 구간마다 새로 만든다. 공유 어댑터는 사건·State를 보관하지 않는다.
- lifecycle은 Agent State를 공유하지 않는다. 경계는 `StartIncident → IncidentAnalysisRequest → MainAgentState → IncidentAnalysisResult → IncidentOutcome/publication`이다. timeout이어도 이미 확보한 결과는 버리지 않고 실패 상태와 함께 전달한다.
- BaseModel은 LLM·비신뢰 입력 검증에, 내부 immutable 값은 frozen dataclass에, reducer가 필요한 mapping State는 TypedDict에 쓴다.
- Evidence ID는 수집 호출의 local allocator가 `E-{incident_id}-{number}`로 발급하고 결과 DTO로 Main에 돌려준다. 외부 mutable State를 바꾸는 callback은 없다.
- 소스 하나가 실패해도 나머지는 유지하고 gap으로 남긴다. 조회 실패와 정상 0건을 구분해서 표시한다.

## 데이터 원칙

- **원문 = DTO다.** 흐름은 "테이블 → DTO → 보고서 → DTO 대조 검증"이다. DTO를 다시 직렬화한 `raw` 사본이나 `SELECT *`·`url` 전체 같은 원본 덩어리를 따로 저장·전달하지 않는다. 필요한 컬럼만 조회해 DTO 필드로 파싱하고, 검증·식별에 원문이 필요하면 DTO 필드나 `record_key`로 해결한다. (키워드 5개 제한 같은 축소 정책이 raw 사본으로 무력화되고 토큰만 불어나기 때문이다.)
- 수집 출처(`EvidenceProvenance`: 호스트, 파일, DB·테이블, 조회 구간, 수집 시각)는 코드가 채운다. LLM이 쓰지 않는다.
- 보고서의 건수·최대값·쿼리 후보는 코드가 센 `observations`에서 읽는다. 모델은 해석 문장(`summary`, `findings`, `root_causes`, `recommendations` 등)만 쓰고 그 주장에는 근거 참조(`evidence_refs`)가 붙는다.
- LLM 선별 단계(map/reduce)는 원인을 추정하지 않는다. 원인 판단은 모든 소스의 근거가 모인 뒤 구간 리포트 단계에서 한다.
- 검증 결과 해석: `PASSED`는 사실이 입증됐다는 뜻이 아니라 명확한 모순을 찾지 못했다는 뜻이다. `NOT_VERIFIED`는 `PASSED`가 아니다. 검증은 최대 3회, 표현 수정 최대 2회, 재분석 최대 1회다. 재분석이 실패하면 기존 산출물을 유지하고 사건 종합이 실패하면 마지막 구간 리포트로 대체하지 않는다.
- 위키는 지식 근거이지 이번 사건의 관측이 아니다. 증상이 같다는 이유만으로 원인을 확정하지 않는다("~로 보인다" 후보). 위키로는 색인 조회와 패턴 id만 보내고 우리 관측 데이터는 보내지 않는다.

## 코드 작성 규칙

- 주석·docstring에 수정 이력("예전에는 X였다")이나 대화 맥락을 쓰지 않는다. 커밋 메시지에 쓴다. 남기는 기준은 "현재 코드의 근거인가"다. 되돌리면 깨진다는 경고, 실측값, 뻔한 대안을 안 쓴 이유는 남긴다. 저장소에 `예전에는`이 남아 있어도 그 방식을 따라 하지 않는다.
- 주변 코드의 주석 밀도·이름·관용을 따른다.

## 실행과 검증

- 테스트: `uv run pytest`. collection I/O와 LLM 호출은 테스트에서 대체한다. 실제 외부 연결이나 사고 원인의 정확성을 보장하지 않는다. `uv run python -m compileall -q src`도 쓴다. 테스트는 `tests/unit/<패키지>/` 아래에 소스 패키지 구조를 따라 둔다.
- 서버 venv는 **Python 3.11**이고 pyproject·로컬은 3.13이다. asyncio·타입 등 버전에 민감한 변경은 3.11에서도 확인한다(`uv venv --python 3.11`).
- **로컬에서 실제 분석 경로를 실행하지 않는다**(`scripts/run_analysis.py` 등, `--dry-run` 제외). 로컬 `.env`가 운영 ClickHouse·ES·Gemini를 가리키고, `cluster_doctor.main`이 import하는 litellm이 import 시점에 repo 루트 `.env`를 환경변수로 올린다(cwd와 무관). "설정이 없어 실패할 것"이라고 가정하지 않는다. 일부만 확인할 때는 해당 함수만 import하거나 단위 테스트로 한다.
- `reports/report-*.html`은 외부 서버에서 같은 코드로 돌린 결과를 옮겨 온 것이다. 로컬 `logs/app.log`에 그 실행의 로그가 없다. 리포트가 이상하면 HTML 렌더 결과와 코드 경로로 추적하고, 서버 로그가 필요하면 grep할 문자열을 지정해 사용자에게 요청한다. 로컬 재현은 LLM 없이 스키마·함수 단위로 한다.
- 로그 디렉터리 변수는 `CLUSTER_DOCTOR_LOG_DIR`다. 서버 셸에 Kafka용 `LOG_DIR`이 export돼 있고 OS 환경변수가 `.env`보다 우선하므로, 같은 방식으로 셸 환경변수가 `.env`를 덮어쓰는 다른 설정이 있을 수 있다.
- `.env`의 값(특히 `MONITORING_WIKI_URL`)은 출력하지 않는다.

## 문서 작성 규칙

- 고정 한도의 값은 README의 고정 한도 표가 기준이다. 패키지 문서에는 상한이 왜 그런지를 적고, 규칙 설명에 꼭 필요한 숫자만 둔다.
- 작업 계획 문서는 `docs/superpowers/plans/`(gitignored, 로컬 전용)에 있다.
