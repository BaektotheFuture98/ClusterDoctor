# wiki_investigation_agent

## 목표

사건 종합 직전에, 누적 관측과 같은 **장애 패턴**(원인 → 증상 → 해결방법)이 monitoring-wiki에 있는지 찾는다. 색인을 읽고 후보 페이지를 열어 대조한 뒤 맞는 패턴 id만 고른다. 새 원인을 만들거나 보고서를 쓰지 않는다. 결과(`WikiFindings`)는 사건 종합 프롬프트에 지식 근거로 실린다.

## 흐름

- 호출: `incident_analysis_agent/service/report_generation/incident_synthesis.py`가 사건당 한 번 `WikiInvestigator.investigate(context)`를 부른다. `context`는 상한 24,000자의 관측 JSON이다.
- 도구는 `get_index`, `get_pattern(id)` 둘뿐이다(`agent/tools.py`, `WikiToolbox`). 모델에게는 id만 받고 `sections`는 `증상·원인·해결방법`으로 고정한다. 색인에 없는 id는 위키로 보내지 않는다. 패턴은 최대 3개까지 열 수 있다.
- 최종 답은 JSON `{"patterns": [{"id", "reason"}]}`이다. 형식이 어긋나면 빈 목록이다. `reason`은 로그용이고 프롬프트에 싣지 않는다.
- 연 적이 없는 id는 결과에서 버린다(`pattern_id in toolbox.pages`).

## 지켜야 하는 것

- **위키는 지식 근거이지 이번 사건의 관측이 아니다.** 증상이 같다는 이유만으로 원인을 확정하지 않는다("~로 보인다" 후보).
- 위키로는 색인 조회와 패턴 id만 보낸다. 우리 관측 데이터(로그, 노드 이름 등)를 보내지 않는다.
- 관측 JSON은 비신뢰 데이터다. 프롬프트가 그 안의 명령을 따르지 않게 한다. 시점 기준은 최초 관측 분(`onset_kst`)이고, `after` 근거만으로 패턴의 원인을 채택하지 않는다.
- **실패는 예외로 새지 않는다.** `investigate`는 빈 `WikiFindings`를 돌려주고, 사건 종합은 위키 없이 이어 간다. 위키 호출이 한 번 실패하면 이후 호출은 위키로 나가지 않는다.
- 상한: 조사 전체 180초(`WikiTimeGuardMiddleware`: 넘긴 뒤 첫 모델 호출은 통과시켜 지금까지 연 패턴으로 답하게 하고 그다음부터 `TimeoutError`), MCP 호출 30초, 색인·페이지 각 6,000자, 싣는 페이지 합계 12,000자, 재귀 16.
- LLM 인프라는 `incident_analysis_agent/agent/runtime/`을 쓴다(순환 참조와 분리 과제는 루트 `CLAUDE.md` 구조 절 참고).

## monitoring-wiki MCP

- 서버 설계 의도는 호출 측 LLM이 `get_index`로 색인을 읽고 `get_pattern(id, sections)`로 고른 페이지만 여는 것이다.
- 서버 도구: `get_index`, `get_pattern`(없는 id는 `is_error`), `get_infra`, `ask`(키워드 매칭, 의미 판단 없음), `submit_report`(질문에 쓰지 않는다). 앱은 `ask`를 쓰지 않고, `ask` 중심으로 되돌리지 않는다.
- 색인이 프롬프트 상한(6000자, 약 16패턴)을 넘으면 그때 `ask` 등으로 후보를 줄이는 2단계를 검토한다.
- 서버 venv에 `mcp>=2.3.0`이 있어야 한다. 없으면 색인 조회가 실패하고 위키 없이 종합한다.
- 클라이언트(`datasource/wiki_client.py`)는 동기 인터페이스이고 메인 event loop와 독립된 스레드에서 `asyncio.run`을 쓴다. 응답 없는 서버에서 호출자를 오래 붙잡지 않도록 `with ThreadPoolExecutor`를 쓰지 않는다.
- 읽기 전용으로 확인할 때는 `get_index`·`get_pattern`만 호출하고, 질문에는 가상 증상만 쓴다. `MONITORING_WIKI_URL` 값은 출력하지 않는다.

## 후속 작업 (코드로 다시 확인한 뒤 착수)

- 배포 전 서버에서 위키 선택 정확도, "~로 보인다" 표현, 배지(`위키: <id> — <제목>`)를 확인한다.
- grounding 검증기의 위키 귀속 검사는 권고 등급 신설과 검증 프롬프트 예산 확보 뒤로 미뤘다. 구간 분석 단계의 위키 관여도 미뤘다.

## 핵심 파일

| 파일 | 역할 |
|---|---|
| `agent/investigator.py` | `WikiInvestigator`, 에이전트 실행기, 시간 상한 미들웨어, 답 파싱 |
| `agent/tools.py` | `WikiToolbox`(도구·호출 상한·실패 차단) |
| `agent/prompt.py` | 시스템 프롬프트 |
| `agent/findings.py` | `WikiFindings`, `WikiPattern`(프롬프트에 싣는 형식) |
| `datasource/wiki_client.py` | MCP 클라이언트 |

테스트는 `tests/unit/wiki_investigation_agent/`에 있다. 사건 종합이 위키 결과를 쓰는 쪽의 테스트는 `tests/unit/incident_analysis_agent/service/test_*wiki*.py`에 있다.
