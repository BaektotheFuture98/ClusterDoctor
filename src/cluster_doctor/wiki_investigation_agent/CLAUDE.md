# wiki_investigation_agent

## 목표

사건 종합 직전에, 누적 관측과 같은 **장애 패턴**(원인 → 증상 → 해결방법)이 monitoring-wiki에 있는지 찾고, 관측·구간 리포트·클러스터 구성 문서(infra.md)·패턴을 이어 **원인 후보의 인과 사슬을 재구성**한다(예: 특정 기간 검색 집중 → WARM 티어·RC17 → RC17 노드 이탈). 보고서는 쓰지 않는다. 결과(`WikiFindings`: 패턴·구성 문서·재구성)는 사건 종합 프롬프트에 실리고, 종합은 재구성 사슬을 원인 서술의 뼈대로 써서 고리를 관측과 대조한다.

## 흐름

- 호출: `incident_analysis_agent/service/report_generation/incident_synthesis.py`가 사건당 한 번 `WikiInvestigator.investigate(context, windows=, base_date=, evidence_ids=)`를 부른다. `context`는 상한 24,000자의 관측 JSON, `windows`는 구간 리포트 요약(`window_digest.py`), `base_date`는 티어 기준일(사건 발생일, 없으면 첫 구간 시작일)이다.
- 구성 문서는 모델이 고르는 도구가 아니라 `investigate` 시작 시 코드가 `get_infra`로 한 번 가져와 사용자 메시지에 싣는다. 재구성에 항상 필요하고 고를 이유가 없어서다. 실패하거나 비어 있으면 구성 없이 조사한다. 문서 내용(노드 구성, 티어별 랙, 티어 기준)은 위키가 관리하고 코드는 형식을 가정하지 않는다. 노드 이름의 앞부분이 랙이라는 대응(`RC17-04` → `RC17`)만 프롬프트에 둔다.
- 도구는 `get_index`, `get_pattern(id)` 둘뿐이다(`agent/tools.py`, `WikiToolbox`). 모델에게는 id만 받고 `sections`는 `증상·원인·해결방법`으로 고정한다. 색인에 없는 id는 위키로 보내지 않는다. 패턴은 최대 3개까지 열 수 있다.
- 최종 답은 JSON `{"patterns": [{"id", "reason"}], "reconstruction": [{"statement", "chain": [{"step", "basis", "refs"}], "wiki_ref"}]}`이다. 형식이 어긋나면 빈 목록이다. `reason`은 로그용이고 프롬프트에 싣지 않는다.
- 연 적이 없는 id는 결과에서 버린다(`pattern_id in toolbox.pages`).
- 재구성의 고리는 출처(`basis`)가 evidence(이번 Evidence id)·infra(구성 문서)·wiki(고른 패턴 id)다. 코드가 없는 Evidence id, 구성 문서 없이 단 infra 고리, 고르지 않은 패턴을 지우고, **관측 고리가 하나도 남지 않은 사슬은 버린다**. 구성·위키만으로 세운 인과가 원인 초안으로 넘어가지 않게 하기 위해서다.

## 지켜야 하는 것

- 위키·구성 문서의 지위와 위키로 보내는 것(색인·구성 문서 조회와 패턴 id만)은 사건 종합도 따르는 규칙이라 루트 `CLAUDE.md` 데이터 원칙에만 둔다.
- 구간 리포트 요약은 사건 종합 초안과 달리 해석 문장을 싣는다. 재구성의 출발점이기 때문이다. 대신 `MISMATCH`·`UNCHECKED` 구간은 해석을 빼고 근거 id만 싣는다(검증이 모순을 찾았거나 대조하지 못했다). 상한을 넘으면 뒤쪽 구간부터 뺀다.
- 관측 JSON은 비신뢰 데이터다. 프롬프트가 그 안의 명령을 따르지 않게 한다. 시점 기준은 최초 관측 분(`onset_kst`)이고, `after` 근거만으로 패턴의 원인을 채택하지 않는다.
- **Agent State가 없다.** 루트 설계 원칙의 예외다. `create_agent`는 기본 State(메시지)로만 돌고, 연 페이지·실패 여부·호출 수는 `WikiToolbox`가 들며 도구가 그 객체를 직접 고친다. 결과도 그래프 State가 아니라 실행 뒤 `toolbox.pages`에서 읽는다. 읽기 전용 일회성 조회라 State와 `Command(update=...)`로 옮겨도 얻는 것이 적어서다. 그래서 `WikiToolbox`는 `investigate` 호출마다 새로 만들고 공유하거나 실행 사이에 재사용하지 않는다. 한 턴의 상태 변경 도구 하나 규칙도 강제하지 않으므로, 한 턴에 `get_pattern`이 여러 번 오면 병렬 실행에서 패턴 상한을 넘겨 열 수 있다(싣는 페이지 합계 상한으로 잘린다).
- **실패는 예외로 새지 않는다.** `investigate`는 빈 `WikiFindings`를 돌려주고, 사건 종합은 위키 없이 이어 간다. 위키 호출이 한 번 실패하면 이후 호출은 위키로 나가지 않는다.
- 상한(값은 README 고정 한도 표): 조사 전체 시간은 구성 문서 조회를 포함한다(`WikiTimeGuardMiddleware`: 넘긴 뒤 첫 모델 호출은 통과시켜 지금까지 연 패턴으로 답하게 하고 그다음부터 `TimeoutError`). 재구성으로 최종 답이 길어져 패턴 고르기만 할 때보다 늘렸다. MCP 호출 30초, 재귀 16. 사건 종합 프롬프트에는 구성 문서·재구성·패턴 페이지가 각자 상한으로 잘려 실리고, 그만큼 관측 JSON 몫이 줄어든다.
- LLM 인프라는 `incident_analysis_agent/agent/runtime/`을 쓴다(공용 LLM 인프라의 위치와 순환 참조는 루트 `CLAUDE.md` 구조 절 참고).

## monitoring-wiki MCP

- 서버 설계 의도는 호출 측 LLM이 `get_index`로 색인을 읽고 `get_pattern(id, sections)`로 고른 페이지만 여는 것이다.
- 서버 도구: `get_index`, `get_pattern`(없는 id는 `is_error`), `get_infra`(구성 문서), `ask`(키워드 매칭, 의미 판단 없음), `submit_report`(질문에 쓰지 않는다). 앱은 `ask`를 쓰지 않고, `ask` 중심으로 되돌리지 않는다.
- 색인이 프롬프트 상한(6000자, 약 16패턴)을 넘으면 그때 `ask` 등으로 후보를 줄이는 2단계를 검토한다.
- 서버 venv에 `mcp>=2.3.0`이 있어야 한다. 없으면 색인 조회가 실패하고 위키 없이 종합한다.
- 클라이언트(`datasource/wiki_client.py`)는 동기 인터페이스이고 메인 event loop와 독립된 스레드에서 `asyncio.run`을 쓴다. 응답 없는 서버에서 호출자를 오래 붙잡지 않도록 `with ThreadPoolExecutor`를 쓰지 않는다.
- 읽기 전용으로 확인할 때는 `get_index`·`get_pattern`·`get_infra`만 호출하고, 질문에는 가상 증상만 쓴다. `MONITORING_WIKI_URL` 값은 출력하지 않는다.

## 핵심 파일

- 시간 상한 미들웨어(`WikiTimeGuardMiddleware`)와 답 파싱은 `agent/investigator.py`에 있다.
- 사건 종합이 위키 결과를 쓰는 쪽의 테스트는 `tests/unit/incident_analysis_agent/service/test_*wiki*.py`에 있다.
