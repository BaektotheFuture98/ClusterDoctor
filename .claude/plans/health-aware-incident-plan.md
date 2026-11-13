# 클러스터 health 기반 incident 세션 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 정착 뒤 30초마다 큐와 ES `_cluster/health`를 확인해 incident 범위(최대 20분)를 정하고, green이 아니면 이전 보고서를 넘기며 다음 incident를 이어 만드는 세션(최대 3개)을 구현한다.

**Architecture:** 대기·마감·계속 판단은 `kafka_consumer`의 코드 규칙(`incident_session.py`)과 `ProblemLogProcessor` 세션 루프가 맡는다. `StartIncident`가 범위·마감 사유·health 이력·`SessionContext`(이전 보고서, 이전 분석 구간, 세션 가명화 대응표)를 orchestrator로 나른다. Main Agent는 incident마다 새로 만들고 대기·health 도구를 갖지 않는다.

**Tech Stack:** Python 3.13(서버 3.11), asyncio, LangGraph/DeepAgents, pydantic, pytest(`uv run pytest`).

**설계 문서:** `.claude/plans/health-aware-incident-design.md`

## Global Constraints

- 테스트: `uv run pytest`. 컴파일 확인: `uv run python -m compileall -q src`. asyncio 변경이 있으므로 마지막에 Python 3.11 venv에서도 테스트한다(`uv venv --python 3.11`).
- **로컬에서 실제 분석 경로를 실행하지 않는다**(`scripts/run_analysis.py` 등, `--dry-run` 제외). `.env`가 운영 ClickHouse·ES·Gemini를 가리킨다. LLM·ES는 테스트에서 가짜로 대체한다.
- 알려진 기존 실패: `tests/unit/incident_orchestrator_agent/service/test_report_summary.py::test_demo_is_identified_once`(Windows 인코딩). 이것 외의 실패는 고친다.
- 주석·docstring에 수정 이력("예전에는 X")을 쓰지 않는다. 현재 코드의 근거만 쓴다. 주변 코드의 주석 밀도·이름·관용을 따른다.
- 테스트는 `tests/unit/<패키지>/` 아래 소스 구조를 따라 둔다.
- 시각은 항상 timezone-aware.
- 상수 값은 한 곳에만 둔다. incident 범위 상한·세션 상한은 `kafka_consumer/trigger_settling/service/incident_session.py`에만 있다.
- 커밋 메시지 끝: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. 커밋은 사용자 승인 뒤에만 한다(실행 전에 확인).
- README·패키지 CLAUDE.md는 이 설계로 이미 고쳐 두었다(커밋 안 됨). 구현 이름이 문서와 다르면 문서가 아니라 구현을 문서에 맞춘다.

---

### Task 0: 브랜치 준비

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: 브랜치 생성**

```bash
git switch -c feat/health-aware-incident
```

- [ ] **Step 2: 이미 끝난 프롬프트 정비 변경을 먼저 커밋** (문서는 Task 10에서 함께 커밋)

```bash
git add src/cluster_doctor/incident_analysis_agent/workflow/minute_analysis/spec.py \
        src/cluster_doctor/incident_analysis_agent/workflow/minute_analysis/prompt.py \
        src/cluster_doctor/incident_analysis_agent/datasource/clickhouse/slowlog.py \
        src/cluster_doctor/incident_analysis_agent/datasource/clickhouse/query_log.py \
        src/cluster_doctor/incident_analysis_agent/datasource/clickhouse/master_log.py \
        src/cluster_doctor/incident_analysis_agent/datasource/ssh/node_log.py \
        tests/unit/incident_analysis_agent/workflow/test_minute_prompt.py
git commit -m "feat: 분 선별 기준을 분 단위와 구간 단위로 나눈다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 3: 개인 설정이 올라가지 않게 `.gitignore`에 추가**

`.gitignore` 끝에 한 줄 추가:

```
.claude/settings.local.json
```

Run: `git status --short` → `.claude/settings.local.json`이 목록에 없어야 한다.

---

### Task 1: health 접기 함수를 도메인으로 옮긴다

**Files:**
- Modify: `src/cluster_doctor/incident_analysis_agent/model/health_point.py`
- Modify: `src/cluster_doctor/incident_analysis_agent/service/observation/builder.py:234-259`
- Test: `tests/unit/incident_analysis_agent/model/test_health_point.py`

**Interfaces:**
- Produces: `fold_health(points: tuple[HealthPoint, ...], payload: dict, now: datetime) -> tuple[HealthPoint, ...]`, `is_green(payload: dict) -> bool`

- [ ] **Step 1: 실패하는 테스트**

```python
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import (
    HealthPoint,
    fold_health,
    is_green,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)


def test_same_state_extends_until():
    first = fold_health((), {"status": "red", "unassigned_shards": 3}, _T)
    later = _T + timedelta(seconds=30)
    folded = fold_health(first, {"status": "red", "unassigned_shards": 3}, later)
    assert folded == (HealthPoint(at=_T, until=later, status="red", unassigned_shards=3),)


def test_changed_state_appends_point():
    first = fold_health((), {"status": "red", "unassigned_shards": 3}, _T)
    later = _T + timedelta(seconds=30)
    folded = fold_health(first, {"status": "yellow", "unassigned_shards": 1}, later)
    assert [point.status for point in folded] == ["red", "yellow"]
    assert folded[1].at == later


def test_is_green_ignores_case():
    assert is_green({"status": "GREEN"})
    assert not is_green({"status": "yellow"})
    assert not is_green({})
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/incident_analysis_agent/model/test_health_point.py -v`
Expected: FAIL (`ImportError: cannot import name 'fold_health'`)

- [ ] **Step 3: 구현** — `health_point.py` 전체를 다음으로 바꾼다.

```python
from dataclasses import dataclass, replace
from datetime import datetime


@dataclass(frozen=True)
class HealthPoint:
    """클러스터 상태가 유지된 한 구간.

    health는 incident 구성 중에는 30초마다, 분석 구간마다 한 번씩 조회된다.
    호출마다 한 줄을 쌓으면 같은 green이 열 줄 늘어서고, 그 목록은 "상태 변화를
    시간순으로"라는 리포트의 요구를 오히려 가린다. 그래서 직전과 같은 상태면 새
    항목을 만들지 않고 ``until``만 늘린다(``fold_health``).
    """

    at: datetime
    until: datetime
    status: str
    unassigned_shards: int = 0
    active_shards: int = 0
    number_of_nodes: int = 0


def fold_health(
    points: tuple[HealthPoint, ...], payload: dict, now: datetime
) -> tuple[HealthPoint, ...]:
    """health 응답 하나를 이력에 접는다.

    수집기(분석 시점)와 incident 구성(사건 당시)이 같은 규칙을 쓴다. payload가
    dict가 아니거나 숫자 칸에 숫자가 아닌 값이 오면 예외가 난다. 호출자가 조회
    실패로 다룬다.
    """
    status = str(payload.get("status", ""))
    unassigned = int(payload.get("unassigned_shards", 0) or 0)
    if points and points[-1].status == status and points[-1].unassigned_shards == unassigned:
        return (*points[:-1], replace(points[-1], until=now))
    return (
        *points,
        HealthPoint(
            at=now,
            until=now,
            status=status,
            unassigned_shards=unassigned,
            active_shards=int(payload.get("active_shards", 0) or 0),
            number_of_nodes=int(payload.get("number_of_nodes", 0) or 0),
        ),
    )


def is_green(payload: dict) -> bool:
    return str(payload.get("status", "")).lower() == "green"
```

`builder.py`의 `record_health` 본문을 교체한다(docstring 첫 줄 유지):

```python
    def record_health(self, payload: dict, now: datetime) -> None:
        """클러스터 상태를 이력에 접어 넣는다. 규칙은 ``fold_health``에 있다."""
        self.health = list(fold_health(tuple(self.health), payload, now))
```

`builder.py` import에 `fold_health` 추가:

```python
from cluster_doctor.incident_analysis_agent.model.health_point import (
    HealthPoint,
    fold_health,
)
```

`builder.py`에서 `HealthPoint`·`replace`가 다른 곳에 안 쓰이면 ruff가 알려 준다(`uvx ruff check src/cluster_doctor/incident_analysis_agent/service/observation/builder.py`). 안 쓰이는 import만 지운다.

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/incident_analysis_agent -q`
Expected: PASS (기존 수집기 테스트 포함)

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/incident_analysis_agent/model/health_point.py src/cluster_doctor/incident_analysis_agent/service/observation/builder.py tests/unit/incident_analysis_agent/model/test_health_point.py
git commit -m "refactor: health 이력 접기를 도메인 함수로 옮긴다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: 가명화 대응표를 세션 단위로 이어 쓸 수 있게 한다

**Files:**
- Modify: `src/cluster_doctor/incident_analysis_agent/agent/runtime/pseudonym.py:1-19,140-157`
- Test: `tests/unit/incident_analysis_agent/agent/runtime/test_pseudonym_session.py`

**Interfaces:**
- Produces: `pseudonym_scope(*, fresh: bool = False, mapping: Pseudonymizer | None = None)`

- [ ] **Step 1: 실패하는 테스트**

```python
from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import (
    PSEUDONYMS,
    Pseudonymizer,
    pseudonym_scope,
)


def test_given_mapping_is_reused_across_scopes():
    shared = Pseudonymizer()
    with pseudonym_scope(fresh=True, mapping=shared):
        PSEUDONYMS.register("company", "acme-corp")
        first = PSEUDONYMS.mask("acme-corp 요청")
    with pseudonym_scope(fresh=True, mapping=shared):
        second = PSEUDONYMS.mask("acme-corp 요청")
    assert first == second == "company-0001 요청"


def test_fresh_scope_without_mapping_forgets_names():
    with pseudonym_scope(fresh=True):
        PSEUDONYMS.register("company", "acme-corp")
    with pseudonym_scope(fresh=True):
        assert PSEUDONYMS.mask("acme-corp 요청") == "acme-corp 요청"
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/incident_analysis_agent/agent/runtime/test_pseudonym_session.py -v`
Expected: FAIL (`TypeError: pseudonym_scope() got an unexpected keyword argument 'mapping'`)

- [ ] **Step 3: 구현**

모듈 docstring 10행의 문장 `대응표는 Incident마다 새로 만들고 종료 시 해제한다.`만 다음으로 바꾼다. 같은 줄 뒤의 `Main/SubAgent와 병렬`과 11행 `조회·분 단위 분석은 contextvars로 같은 대응표를 공유한다.`는 그대로 둔다:

```
대응표는 incident 세션마다 새로 만든다. 같은 세션의 incident들은 이전 보고서의
이름을 같은 가명으로 가려야 하므로 대응표를 이어 쓴다(``mapping`` 인자).
```

`pseudonym_scope`를 교체한다:

```python
@contextmanager
def pseudonym_scope(
    *, fresh: bool = False, mapping: Pseudonymizer | None = None
) -> Iterator[Pseudonymizer]:
    """incident 하나 안에서 mapping을 공유하고, 성공·실패와 무관하게 해제한다.

    Main이 새 scope를 시작한다. 중첩된 분석과 단독 LLM 호출은 활성 scope를
    재사용하며, 독립적으로 호출되면 임시 scope를 만든다.
    context 복사본은 별도 카운터가 아니라 같은 thread-safe mapping을 공유한다.

    ``mapping``을 주면 그 대응표로 scope를 연다. 세션의 다음 incident는 이전
    보고서(실명으로 복원된 문장)를 프롬프트에 싣는데, 새 대응표에는 그 이름이
    등록돼 있지 않아 가려지지 않는다.
    """
    current = _current.get()
    if mapping is None and current is not None and not fresh:
        yield current
        return
    if mapping is None:
        mapping = Pseudonymizer()
    token = _current.set(mapping)
    try:
        yield mapping
    finally:
        _current.reset(token)
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/incident_analysis_agent/agent/runtime -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/incident_analysis_agent/agent/runtime/pseudonym.py tests/unit/incident_analysis_agent/agent/runtime/test_pseudonym_session.py
git commit -m "feat: 가명화 대응표를 세션 단위로 이어 쓸 수 있게 한다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: lifecycle 계약과 처리 시간

**Files:**
- Modify: `src/cluster_doctor/incident_orchestrator_agent/model/lifecycle.py`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/incident_lifecycle/analyze_incident.py:65-140`
- Test: `tests/unit/incident_orchestrator_agent/model/test_lifecycle_session.py`
- Test: `tests/unit/incident_orchestrator_agent/service/incident_lifecycle/test_analyze_incident_session.py`

**Interfaces:**
- Consumes: `HealthPoint`(Task 1), `Pseudonymizer`(Task 2)
- Produces:
  - `IncidentClose` (StrEnum: `GREEN="green 확인"`, `SPAN_LIMIT="범위 상한"`, `HEALTH_UNKNOWN="health 확인 불가"`, `NOT_WAITED="대기 안 함"`)
  - `SessionContext(session_id, sequence, last_allowed, onset=None, previous_incident_id="", previous_report=None, previous_windows=(), pseudonyms=Pseudonymizer())`, `SessionContext.following(*, incident_id, report, windows, last_allowed) -> SessionContext`
  - `IncidentAnalysisRequest.onset` (property): 세션 첫 관측 시각, 세션이 없으면 `observed_start`
  - `tracking_stopped(close: IncidentClose, session: SessionContext | None) -> bool`
  - `StartIncident`·`IncidentAnalysisRequest` 새 필드: `incident_end: datetime | None = None`, `close: IncidentClose = IncidentClose.NOT_WAITED`, `health_history: tuple[HealthPoint, ...] = ()`, `session: SessionContext | None = None`. `StartIncident`에만 `health_gaps: tuple[str, ...] = ()`, `StartIncident.to_request() -> IncidentAnalysisRequest`

- [ ] **Step 1: 실패하는 테스트 (모델)** — `test_lifecycle_session.py`

```python
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.model.incident import Incident
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentClose,
    SessionContext,
    StartIncident,
    tracking_stopped,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)


def test_following_keeps_pseudonyms_and_onset_and_accumulates_windows():
    first = SessionContext(session_id="s1", sequence=1, last_allowed=False, onset=_T)
    window = TimeRange(_T, _T + timedelta(minutes=5))
    second = first.following(incident_id="inc1", report=None, windows=(window,), last_allowed=False)
    third = second.following(incident_id="inc2", report=None, windows=(window,), last_allowed=True)
    assert third.sequence == 3
    assert third.previous_incident_id == "inc2"
    assert third.previous_windows == (window, window)
    assert third.pseudonyms is first.pseudonyms
    assert third.onset == _T
    assert third.last_allowed


def test_request_onset_is_session_onset_for_follow_on_incident():
    incident = Incident(incident_id="i", cluster="c", trigger_time=_T, kafka_receive_time=_T)
    later = _T + timedelta(minutes=20)
    session = SessionContext(session_id="s1", sequence=2, last_allowed=False, onset=_T)
    assert StartIncident(incident, later, later, 0.0, session=session).to_request().onset == _T
    assert StartIncident(incident, later, later, 0.0).to_request().onset == later


def test_tracking_stops_only_on_last_allowed_without_green():
    last = SessionContext(session_id="s1", sequence=3, last_allowed=True)
    assert tracking_stopped(IncidentClose.SPAN_LIMIT, last)
    assert tracking_stopped(IncidentClose.HEALTH_UNKNOWN, last)
    assert not tracking_stopped(IncidentClose.GREEN, last)
    assert not tracking_stopped(IncidentClose.SPAN_LIMIT, None)


def test_to_request_carries_session_fields():
    incident = Incident(incident_id="i", cluster="c", trigger_time=_T, kafka_receive_time=_T)
    session = SessionContext(session_id="s1", sequence=1, last_allowed=False)
    command = StartIncident(
        incident, _T, _T, 0.0,
        incident_end=_T + timedelta(minutes=20),
        close=IncidentClose.SPAN_LIMIT,
        session=session,
        health_gaps=("g",),
    )
    request = command.to_request()
    assert request.session is session
    assert request.close is IncidentClose.SPAN_LIMIT
    assert request.incident_end == _T + timedelta(minutes=20)
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent/model/test_lifecycle_session.py -v`
Expected: FAIL (`ImportError: cannot import name 'IncidentClose'`)

- [ ] **Step 3: 구현 (모델)** — `lifecycle.py`

import 추가:

```python
from enum import StrEnum

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import Pseudonymizer
from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
```

`IncidentAnalysisRequest` 위에 추가:

```python
class IncidentClose(StrEnum):
    """incident 범위를 마감한 이유. 보고서 개요와 Main 시작 메시지에 그대로 쓴다."""

    GREEN = "green 확인"
    SPAN_LIMIT = "범위 상한"
    HEALTH_UNKNOWN = "health 확인 불가"
    NOT_WAITED = "대기 안 함"


@dataclass(frozen=True)
class SessionContext:
    """같은 장애로 이어지는 incident들의 연결. kafka_consumer가 만들고 넘긴다.

    Main은 incident마다 새로 만들어지므로 세션을 기억하지 못한다. 이전 보고서와
    분석 구간은 코드가 outcome에서 옮겨 여기에 싣는다. ``pseudonyms``는 세션의
    incident들이 이어 쓰는 가명 대응표다(``pseudonym_scope``의 ``mapping``).
    ``onset``은 세션 첫 incident의 최초 관측 시각이다. 두 번째 incident부터
    ``observed_start``는 이전 incident의 끝이라 장애 시작이 아니다.
    """

    session_id: str
    sequence: int
    last_allowed: bool
    onset: datetime | None = None
    previous_incident_id: str = ""
    previous_report: LogAnalysisReport | None = None
    previous_windows: tuple[TimeRange, ...] = ()
    pseudonyms: Pseudonymizer = field(
        default_factory=Pseudonymizer, compare=False, repr=False
    )

    def following(
        self,
        *,
        incident_id: str,
        report: LogAnalysisReport | None,
        windows: tuple[TimeRange, ...],
        last_allowed: bool,
    ) -> SessionContext:
        return SessionContext(
            session_id=self.session_id,
            sequence=self.sequence + 1,
            last_allowed=last_allowed,
            onset=self.onset,
            previous_incident_id=incident_id,
            previous_report=report,
            previous_windows=self.previous_windows + windows,
            pseudonyms=self.pseudonyms,
        )


def tracking_stopped(close: IncidentClose, session: SessionContext | None) -> bool:
    """세션 상한의 마지막 incident가 green 확인 없이 마감됐는가.

    발행 전에 알 수 있는 값으로 정한다. 발행 뒤 health를 다시 보는 것은 세션을
    끝낼지만 정하고, 이미 나간 보고서를 고치지 않는다.
    """
    return session is not None and session.last_allowed and close is not IncidentClose.GREEN
```

`IncidentAnalysisRequest`를 교체한다:

```python
@dataclass(frozen=True)
class IncidentAnalysisRequest:
    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float
    incident_end: datetime | None = None
    close: IncidentClose = IncidentClose.NOT_WAITED
    health_history: tuple[HealthPoint, ...] = ()
    session: SessionContext | None = None

    @property
    def onset(self) -> datetime:
        """장애의 최초 관측 시각. 시작 메시지와 사건 종합의 before·onset·after 기준이다.

        세션의 두 번째 incident부터 ``observed_start``는 이전 incident의 끝이므로
        세션의 ``onset``을 쓴다.
        """
        if self.session is not None and self.session.onset is not None:
            return self.session.onset
        return self.observed_start
```

`StartIncident`를 교체한다:

```python
@dataclass(frozen=True)
class StartIncident:
    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float
    incident_end: datetime | None = None
    close: IncidentClose = IncidentClose.NOT_WAITED
    health_history: tuple[HealthPoint, ...] = ()
    health_gaps: tuple[str, ...] = ()
    session: SessionContext | None = None

    def to_request(self) -> IncidentAnalysisRequest:
        return IncidentAnalysisRequest(
            incident=self.incident,
            observed_start=self.observed_start,
            observed_end=self.observed_end,
            settling_wait_seconds=self.settling_wait_seconds,
            incident_end=self.incident_end,
            close=self.close,
            health_history=self.health_history,
            session=self.session,
        )
```

`SessionContext`가 `IncidentAnalysisRequest`보다 앞에 정의되도록 순서를 맞춘다(`from __future__ import annotations`가 있으므로 타입 힌트 순서는 상관없지만 `IncidentClose` 기본값은 정의 뒤여야 한다).

- [ ] **Step 4: 실패하는 테스트 (처리 시간·전달)** — `test_analyze_incident_session.py`

```python
import asyncio
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.model.incident import Incident, IncidentStatus
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisResult,
    IncidentClose,
    SessionContext,
    StartIncident,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import ReportPublication
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    AnalyzeIncident,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)


class _Analyzer:
    def __init__(self):
        self.requests = []

    def analyze(self, request):
        self.requests.append(request)
        return IncidentAnalysisResult(status=IncidentStatus.COMPLETED)


class _Publisher:
    def __init__(self):
        self.calls = []

    async def publish(self, report, *, gaps, analysis_failed):
        self.calls.append((report, gaps, analysis_failed))
        return ReportPublication()


def _service(timeout=60.0):
    analyzer, publisher = _Analyzer(), _Publisher()
    return AnalyzeIncident(
        incident_analyzer=analyzer, report_publisher=publisher, incident_timeout_seconds=timeout
    ), analyzer, publisher


def _command(**fields):
    incident = Incident(incident_id="i", cluster="c", trigger_time=_T, kafka_receive_time=_T)
    settling = fields.pop("settling", 0.0)
    return StartIncident(incident, _T, _T, settling, **fields)


def test_settling_wait_does_not_shrink_analysis_time():
    service, analyzer, _ = _service(timeout=60.0)
    outcome = asyncio.run(service.handle(_command(settling=120.0)))
    assert len(analyzer.requests) == 1
    assert outcome.status is IncidentStatus.COMPLETED


def test_request_carries_session_and_health():
    service, analyzer, _ = _service()
    session = SessionContext(session_id="s1", sequence=2, last_allowed=False)
    point = HealthPoint(at=_T, until=_T, status="red")
    asyncio.run(service.handle(_command(
        close=IncidentClose.SPAN_LIMIT,
        incident_end=_T + timedelta(minutes=20),
        health_history=(point,),
        session=session,
    )))
    request = analyzer.requests[0]
    assert request.session is session
    assert request.health_history == (point,)
    assert request.close is IncidentClose.SPAN_LIMIT


def test_health_gaps_are_published():
    service, _, publisher = _service()
    asyncio.run(service.handle(_command(health_gaps=("health 확인 불가로 incident를 마감했다",))))
    assert "health 확인 불가로 incident를 마감했다" in publisher.calls[0][1]
```

- [ ] **Step 5: 실패 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent/service/incident_lifecycle/test_analyze_incident_session.py -v`
Expected: `test_settling_wait_does_not_shrink_analysis_time` FAIL(분석이 안 돎), `test_health_gaps_are_published` FAIL

- [ ] **Step 6: 구현 (`analyze_incident.py`)**

`_handle`의 처음 부분을 교체한다:

```python
        incident = command.incident
        # 상한은 incident 하나의 분석·발행 시간이다. 정착·incident 구성 대기는 빼지
        # 않는다 — 대기가 최대 범위 상한(20분)까지 늘어 빼면 분석할 시간이 남지 않는다.
        deadline = Deadline(self._incident_timeout_seconds)
```

(`remaining_seconds` 변수와 그 계산을 지운다.)

`_handle`의 발행 부분에서 gap을 합친다:

```python
        gaps = tuple(dict.fromkeys((*command.health_gaps, *result.gaps)))
```

`_run_analyzer`의 요청 생성을 교체한다:

```python
        analyzer_task = asyncio.create_task(
            asyncio.to_thread(self._analyzer.analyze, command.to_request())
        )
```

`IncidentAnalysisRequest` import가 안 쓰이게 되면 지운다.

- [ ] **Step 7: 통과 확인 + 기존 테스트**

Run: `uv run pytest tests/unit/incident_orchestrator_agent -q --deselect tests/unit/incident_orchestrator_agent/service/test_report_summary.py::test_demo_is_identified_once`
Expected: PASS. 기존 테스트 중 "정착 대기가 처리 시간에서 빠진다"를 단언하는 것이 있으면(예: `settling_wait_seconds`가 상한 이상이면 분석하지 않고 FAILED) 새 규칙에 맞게 단언을 뒤집는다 — 대기 시간과 상관없이 분석이 돈다.

- [ ] **Step 8: Commit**

```bash
git add src/cluster_doctor/incident_orchestrator_agent/model/lifecycle.py src/cluster_doctor/incident_orchestrator_agent/service/incident_lifecycle/analyze_incident.py tests/unit/incident_orchestrator_agent/model/test_lifecycle_session.py tests/unit/incident_orchestrator_agent/service/incident_lifecycle/test_analyze_incident_session.py
git commit -m "feat: incident 범위·마감 사유·세션 맥락을 lifecycle 계약에 싣는다

처리 시간 상한은 incident 하나의 분석·발행 시간으로 바꾼다. 대기가
20분까지 늘어 빼면 분석 시간이 남지 않는다.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: 이전 incident의 분석 구간을 덮인 구간으로 본다

**Files:**
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/analysis_window/guardrails.py:154-191`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/analysis_window/window_planner.py:37-89`
- Test: `tests/unit/incident_orchestrator_agent/service/analysis_window/test_prior_windows.py`

**Interfaces:**
- Produces: `covered_windows(state) -> tuple[TimeRange, ...]`(guardrails), `continuation_windows(start: datetime, end: datetime) -> list[TimeRange]`(window_planner). State 키 `prior_windows`.

- [ ] **Step 1: 실패하는 테스트**

```python
from datetime import datetime, timedelta

import pytest

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    MAX_ANALYZED_MINUTES,
    admit_window,
    remaining_minutes,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    continuation_windows,
    plan_new_windows,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_W = TimeRange(_T, _T + timedelta(minutes=10))


def test_prior_windows_are_not_proposed_again():
    assert plan_new_windows([_W], {"analyzed_windows": (), "prior_windows": (_W,)}) == []


def test_prior_windows_are_rejected_by_admission():
    with pytest.raises(GuardrailViolation):
        admit_window(_W, {"prior_windows": (_W,)})


def test_prior_windows_do_not_spend_budget():
    assert remaining_minutes({"prior_windows": (_W,)}) == MAX_ANALYZED_MINUTES


def test_continuation_windows_start_at_incident_start():
    start = _T + timedelta(seconds=40)
    assert continuation_windows(start, start + timedelta(minutes=20)) == [
        TimeRange(_T, _T + timedelta(minutes=10))
    ]


def test_continuation_windows_empty_for_empty_range():
    assert continuation_windows(_T, _T) == []
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent/service/analysis_window/test_prior_windows.py -v`
Expected: FAIL (`ImportError: cannot import name 'continuation_windows'`)

- [ ] **Step 3: 구현**

`guardrails.py`의 `remaining_minutes` 위에 추가:

```python
def covered_windows(state: Mapping[str, object]) -> tuple[TimeRange, ...]:
    """이미 덮인 구간. 이번 incident가 분석한 것과 세션의 이전 incident가 분석한 것.

    State에서 둘을 섞지 않는다. ``analyzed_windows``는 이번 incident의 기록이고,
    이전 incident의 구간(``prior_windows``)은 분 예산을 쓰지 않는다.
    """
    return tuple(_value(state, "analyzed_windows", ())) + tuple(
        _value(state, "prior_windows", ())
    )
```

`admit_window`의 `subtract_spans(window, tuple(_value(state, "analyzed_windows", ())))`를 `subtract_spans(window, covered_windows(state))`로, `check_not_duplicate`의 `is_covered(window, tuple(_value(state, "analyzed_windows", ())))`를 `is_covered(window, covered_windows(state))`로 바꾼다.

`window_planner.py` import에 추가:

```python
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    covered_windows,
)
```

`plan_new_windows`의 `subtract_spans(candidate, tuple(state.get("analyzed_windows", ())))`를 `subtract_spans(candidate, covered_windows(state))`로 바꾼다.

파일 끝에 추가:

```python
def continuation_windows(start, end) -> list[TimeRange]:
    """세션의 두 번째 incident부터 쓰는 첫 후보. incident 시작 분부터 구간 길이 상한까지다.

    ``initial_windows``처럼 앞당기지 않는다. 그 앞은 이전 incident가 이미 봤다.
    """
    pieces = split_span(start.replace(second=0, microsecond=0), end)
    return pieces[:1]
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent/service/analysis_window -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/incident_orchestrator_agent/service/analysis_window/ tests/unit/incident_orchestrator_agent/service/analysis_window/test_prior_windows.py
git commit -m "feat: 세션의 이전 incident가 분석한 구간을 다시 분석하지 않는다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: 이전 보고서 요약과 Analysis·사건 종합 프롬프트

**Files:**
- Create: `src/cluster_doctor/incident_analysis_agent/service/report_generation/previous_incident.py`
- Modify: `src/cluster_doctor/incident_analysis_agent/model/analysis_contract.py:55-58`
- Modify: `src/cluster_doctor/incident_analysis_agent/service/report_generation/report_writer.py:106,122-155,191-206`
- Modify: `src/cluster_doctor/incident_analysis_agent/service/report_generation/prompts.py:239-282`
- Modify: `src/cluster_doctor/incident_analysis_agent/service/report_generation/incident_synthesis.py:35-42`
- Test: `tests/unit/incident_analysis_agent/service/report_generation/test_previous_incident.py`

**Interfaces:**
- Produces:
  - `previous_incident_digest(report: LogAnalysisReport) -> str`
  - `LogAnalysisRequest.previous_incident_report: LogAnalysisReport | None = None`
  - `ReportWriter._prior_summary(prior_report, previous_incident=None) -> str`
  - `build_incident_synthesis_messages(..., previous_incident: str = "")`
  - `ReportWriter.draft_incident(..., previous_incident: LogAnalysisReport | None = None)`
  - `synthesize_incident(..., previous_incident_report=None)`

**주의 (기존 결정과의 충돌):** `draft_incident`는 "이전 구간의 진단 문장은 최종 모델을 근거 없는 주장에 고정시킨다"는 이유로 구간 리포트의 진단 문장을 종합에 싣지 않는다. 이번 설계는 사용자 요구("이어서 작성 혹은 참고")에 따라 **이전 incident**의 요약을 싣는다. 고정 효과를 줄이려고 (1) 섹션 제목에 "참고용, 이번 사건의 관측이 아니다"를 박고, (2) 이전 보고서를 근거 대조 대상에 넣지 않아 이전 보고서만으로 뒷받침되는 주장은 기존 검증이 걸러 내게 둔다. 같은 incident 안의 구간 진단 문장을 싣지 않는 기존 규칙은 그대로다.

- [ ] **Step 1: 실패하는 테스트**

```python
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportRecommendation,
    RootCause,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.previous_incident import (
    previous_incident_digest,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
    build_incident_synthesis_messages,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_REPORT = LogAnalysisReport(
    incident_id="i1",
    analyzed_from=_T,
    analyzed_to=_T + timedelta(minutes=20),
    summary="노드 이탈로 샤드가 재배치됐다.",
    root_causes=(RootCause(statement="data-3 GC 정지"),),
    recommendations=(ReportRecommendation(text="heap 확인"),),
)


def test_digest_has_range_summary_causes_and_actions():
    digest = previous_incident_digest(_REPORT)
    assert "10:00~10:20" in digest
    assert "노드 이탈로 샤드가 재배치됐다." in digest
    assert "원인: data-3 GC 정지" in digest
    assert "조치: heap 확인" in digest


def test_window_prior_summary_includes_previous_incident():
    text = ReportWriter._prior_summary(None, _REPORT)
    assert text.startswith("이전 incident:")


def test_synthesis_marks_previous_incident_as_reference():
    messages = build_incident_synthesis_messages(
        cluster="c", analyzed_from=_T, analyzed_to=_T + timedelta(minutes=20),
        previous_windows_json="{}", analysis_context="{}", previous_incident="요약",
    )
    user = messages[1]["content"]
    assert "--- 이전 incident 보고서 요약 (참고용, 이번 사건의 관측이 아니다) ---" in user
    assert user.index("이전 incident") < user.index("누적 관측값")


def test_synthesis_without_previous_incident_has_no_section():
    messages = build_incident_synthesis_messages(
        cluster="c", analyzed_from=_T, analyzed_to=_T + timedelta(minutes=20),
        previous_windows_json="{}", analysis_context="{}",
    )
    assert "이전 incident" not in messages[1]["content"]
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/incident_analysis_agent/service/report_generation/test_previous_incident.py -v`
Expected: FAIL (`ModuleNotFoundError: ...previous_incident`)

- [ ] **Step 3: 구현**

`previous_incident.py` 생성:

```python
"""세션의 이전 incident 보고서를 다음 incident 프롬프트에 싣는 요약.

이전 보고서는 이어서 쓰기 위한 맥락이지 이번 incident의 관측이 아니다. 근거
대조 대상에 넣지 않고, 프롬프트에는 "참고용"이라고 표시한 자리에만 싣는다.
"""

from __future__ import annotations

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport

# 원인·조치를 몇 개까지 싣는가. 전문을 실으면 incident마다 리포트가 하나씩 더 붙는다.
_MAX_ITEMS = 3


def previous_incident_digest(report: LogAnalysisReport) -> str:
    start = report.analyzed_from.astimezone(KST)
    end = report.analyzed_to.astimezone(KST)
    lines = [f"[{start:%H:%M}~{end:%H:%M}] {report.summary}"]
    lines += [f"- 원인: {cause.statement}" for cause in report.root_causes[:_MAX_ITEMS]]
    lines += [f"- 조치: {item.text}" for item in report.recommendations[:_MAX_ITEMS]]
    return "\n".join(lines)
```

`analysis_contract.py`의 `LogAnalysisRequest`에 `prior_candidate_ids` 아래 추가:

```python
    # 세션의 이전 incident 사건 종합 리포트. 같은 incident의 앞선 구간 리포트
    # (``prior_report``)와 달리 다른 incident의 결론이라 참고로만 싣는다.
    previous_incident_report: LogAnalysisReport | None = None
```

`report_writer.py`:
- import 추가: `from cluster_doctor.incident_analysis_agent.service.report_generation.previous_incident import previous_incident_digest`
- 106행을 `prior_summary=self._prior_summary(request.prior_report, request.previous_incident_report),`로.
- `_prior_summary`를 교체:

```python
    @staticmethod
    def _prior_summary(
        prior_report: LogAnalysisReport | None,
        previous_incident: LogAnalysisReport | None = None,
    ) -> str:
        """같은 Incident의 앞선 리포트 요약과 세션의 이전 incident 요약.

        **전문을 싣지 않는다.** 앞선 분석의 결론 한 문단이면 이번 구간을 어떤
        물음으로 볼지 정하는 데 충분하고, 전문을 실으면 호출마다 리포트가 하나씩
        더 붙어 Context가 분석 횟수만큼 불어난다.
        """
        parts: list[str] = []
        if previous_incident is not None:
            parts.append("이전 incident: " + previous_incident_digest(previous_incident))
        if prior_report is not None:
            questions = "; ".join(prior_report.unresolved_questions[:3])
            parts.append(
                f"[{prior_report.analyzed_from:%H:%M}~{prior_report.analyzed_to:%H:%M}] "
                f"{prior_report.summary}"
                + (f" 미해결: {questions}" if questions else "")
            )
        return "\n".join(parts)
```

- `draft_incident` 시그니처에 `previous_incident: LogAnalysisReport | None = None`을 `wiki` 뒤에 추가하고, `build_incident_synthesis_messages(...)` 호출에 `previous_incident=previous_incident_digest(previous_incident) if previous_incident else "",`를 추가한다. 교정(`build_incident_review_messages`)에는 싣지 않는다.

`prompts.py`:
- `_WIKI_FINDINGS_SECTION_TITLE` 아래 추가:

```python
_PREVIOUS_INCIDENT_SECTION_TITLE = (
    "--- 이전 incident 보고서 요약 (참고용, 이번 사건의 관측이 아니다) ---"
)
```

- `build_incident_synthesis_messages` 시그니처에 `previous_incident: str = "",`를 `onset` 뒤에 추가하고, `context` 목록의 `*_onset_lines(onset),` 다음에 넣는다:

```python
        *([_PREVIOUS_INCIDENT_SECTION_TITLE, previous_incident] if previous_incident else []),
```

`incident_synthesis.py`의 `synthesize_incident` 시그니처에 `previous_incident_report=None`을 추가하고 `draft_incident` 호출에 `previous_incident=previous_incident_report,`를 넘긴다. docstring에 한 줄 추가: `` ``previous_incident_report``는 세션의 이전 incident 리포트다. 초안 입력에만 참고로 싣고 근거 대조에는 넣지 않는다. ``

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/incident_analysis_agent -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/incident_analysis_agent/ tests/unit/incident_analysis_agent/service/report_generation/test_previous_incident.py
git commit -m "feat: 세션의 이전 incident 요약을 구간 분석과 사건 종합에 참고로 싣는다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Main 시작 메시지·후보·health 병합·세션 가명화

**Files:**
- Create: `src/cluster_doctor/incident_orchestrator_agent/agent/incident_scope.py`
- Modify: `src/cluster_doctor/incident_analysis_agent/model/observations.py:146-163,228-247`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/agent/state.py`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/agent/tools.py:206-207`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/agent/adapter.py:97-182`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/agent/analysis_subagent.py:92-113`
- Test: `tests/unit/incident_orchestrator_agent/agent/test_incident_scope.py`

**Interfaces:**
- Consumes: Task 3 (`IncidentAnalysisRequest`, `SessionContext`, `IncidentClose`), Task 4 (`continuation_windows`), Task 5 (`previous_incident_digest`, `previous_incident_report`, `synthesize_incident(previous_incident_report=)`), Task 2 (`pseudonym_scope(mapping=)`)
- Produces:
  - `Observations.incident_span: tuple[datetime, datetime] | None = None`
  - `scope_lines(request) -> list[str]`, `first_windows(request) -> tuple[TimeRange, ...]`, `with_incident_health(observations, request) -> Observations` (incident_scope.py)
  - State 키 `prior_windows`, `incident_end`, `incident_close`; `initial_main_agent_state(..., prior_windows=(), incident_end=None, incident_close="")`
  - `AnalysisSubagentAdapter.bind(incident, previous_incident_report=None)`

- [ ] **Step 1: 실패하는 테스트**

```python
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.agent.incident_scope import (
    first_windows,
    scope_lines,
    with_incident_health,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import Incident
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
    IncidentClose,
    SessionContext,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_END = _T + timedelta(minutes=20)
_INCIDENT = Incident(incident_id="i2", cluster="c", trigger_time=_T, kafka_receive_time=_T)
_PREV = LogAnalysisReport(
    incident_id="i1", analyzed_from=_T - timedelta(minutes=20), analyzed_to=_T,
    summary="이전 결론 문장",
)


def _request(session=None, **fields):
    return IncidentAnalysisRequest(
        incident=_INCIDENT, observed_start=_T, observed_end=_T, settling_wait_seconds=0.0,
        incident_end=fields.pop("incident_end", _END),
        close=fields.pop("close", IncidentClose.SPAN_LIMIT),
        session=session, **fields,
    )


def test_scope_lines_without_session_show_range_and_close():
    lines = scope_lines(_request())
    assert lines == ["incident 범위: 2026-11-13T10:00:00 ~ 2026-11-13T10:20:00 (마감: 범위 상한)"]


def test_scope_lines_for_follow_on_incident_include_previous_report():
    window = TimeRange(_T - timedelta(minutes=10), _T)
    session = SessionContext(
        session_id="s1", sequence=2, last_allowed=False, previous_incident_id="i1",
        previous_report=_PREV, previous_windows=(window,),
    )
    text = "\n".join(scope_lines(_request(session)))
    assert "세션 s1의 2번째 incident" in text
    assert "이전 incident i1" in text
    assert "참고용, 이번 incident의 근거가 아니다" in text
    assert "이전 결론 문장" in text


def test_scope_lines_without_incident_end_are_empty():
    assert scope_lines(_request(incident_end=None, close=IncidentClose.NOT_WAITED)) == []


def test_first_windows_follow_on_starts_at_incident_start():
    session = SessionContext(session_id="s1", sequence=2, last_allowed=False)
    assert first_windows(_request(session)) == (TimeRange(_T, _T + timedelta(minutes=10)),)


def test_first_windows_first_incident_leads_onset():
    session = SessionContext(session_id="s1", sequence=1, last_allowed=False)
    assert first_windows(_request(session))[0].start == _T - timedelta(minutes=5)


def test_with_incident_health_prepends_history_and_sets_span():
    settled = HealthPoint(at=_T, until=_T, status="red")
    analysed = HealthPoint(at=_END + timedelta(minutes=5), until=_END + timedelta(minutes=5), status="green")
    merged = with_incident_health(Observations(health=(analysed,)), _request(health_history=(settled,)))
    assert merged.health == (settled, analysed)
    assert merged.incident_span == (_T, _END)


def test_with_incident_health_keeps_manual_observations():
    observations = Observations()
    assert with_incident_health(observations, _request(incident_end=None)) is observations
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent/agent/test_incident_scope.py -v`
Expected: FAIL (`ModuleNotFoundError: ...incident_scope`)

- [ ] **Step 3: 구현**

`observations.py`: `Observations`의 `source_statuses` 아래 추가:

```python
    # incident 범위(시작 ~ 마감). 타임라인의 health 신호가 이 범위 안의 점만 쓴다.
    # 수동 분석에는 없다.
    incident_span: tuple[datetime, datetime] | None = None
```

`merge_observations`의 `Observations(...)` 인자에 추가:

```python
        incident_span=current.incident_span or new.incident_span,
```

docstring 표에 한 줄 추가: `      incident_span   먼저 정해진 값을 유지한다.`

`incident_scope.py` 생성:

```python
"""incident 범위·세션 맥락을 Main 입력으로 옮긴다.

Main은 incident마다 새로 만들어지고 세션을 기억하지 못한다. 범위·마감 사유와
이전 incident 정보는 kafka_consumer가 ``IncidentAnalysisRequest``에 실어 보내고,
여기서 시작 메시지·첫 후보·관측값으로 바뀐다.
"""

from __future__ import annotations

from dataclasses import replace

from cluster_doctor.incident_analysis_agent.model.kst import format_kst
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.report_generation.previous_incident import (
    previous_incident_digest,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    continuation_windows,
    initial_windows,
)


def scope_lines(request: IncidentAnalysisRequest) -> list[str]:
    """시작 메시지에 덧붙일 incident 범위와 세션 맥락."""
    if request.incident_end is None:
        return []
    lines = [
        f"incident 범위: {format_kst(request.observed_start)} ~ "
        f"{format_kst(request.incident_end)} (마감: {request.close.value})"
    ]
    session = request.session
    if session is None or session.sequence == 1:
        return lines
    lines.append(
        f"세션 {session.session_id}의 {session.sequence}번째 incident다. "
        f"이전 incident {session.previous_incident_id}에서 이어진다."
    )
    if session.previous_windows:
        spans = ", ".join(
            f"{format_kst(w.start)}~{format_kst(w.end)}" for w in session.previous_windows
        )
        lines.append(f"이전 incident가 분석한 구간(다시 분석할 수 없다): {spans}")
    if session.previous_report is not None:
        lines += [
            "이전 incident 보고서 (참고용, 이번 incident의 근거가 아니다):",
            previous_incident_digest(session.previous_report),
        ]
    else:
        lines.append("이전 incident 보고서: 없음(분석 실패)")
    return lines


def first_windows(request: IncidentAnalysisRequest) -> tuple[TimeRange, ...]:
    """첫 후보 구간. 세션의 두 번째 incident부터는 앞당기지 않는다."""
    session = request.session
    if session is not None and session.sequence > 1 and request.incident_end is not None:
        return tuple(continuation_windows(request.observed_start, request.incident_end))
    return tuple(initial_windows(request.observed_start))


def with_incident_health(
    observations: Observations, request: IncidentAnalysisRequest
) -> Observations:
    """incident 구성 중의 health 이력과 범위를 관측값에 합친다.

    사건 종합 **전에** 불러야 종합과 타임라인이 같은 값을 본다. 이력을 앞에
    두는 이유: 분석 구간마다 조회한 분석 시점 값보다 시각이 이르다.
    """
    if request.incident_end is None:
        return observations
    return replace(
        observations,
        health=request.health_history + observations.health,
        incident_span=(request.observed_start, request.incident_end),
    )
```

`state.py`:
- `MainAgentState`에 추가:

```python
    prior_windows: NotRequired[Annotated[tuple[TimeRange, ...], last_write_wins]]
    incident_end: NotRequired[Annotated[datetime | None, last_write_wins]]
    incident_close: NotRequired[Annotated[str, last_write_wins]]
```

- `initial_main_agent_state`에 키워드 인자 `prior_windows: tuple[TimeRange, ...] = ()`, `incident_end: datetime | None = None`, `incident_close: str = ""`를 추가하고 반환 dict에 `"prior_windows": prior_windows, "incident_end": incident_end, "incident_close": incident_close,`를 넣는다.

`tools.py`의 `list_candidate_windows`에서 `observed_end` 줄 삽입 뒤에 추가:

```python
        if state.get("incident_end") is not None:
            lines.insert(
                0,
                f"incident 범위 끝: {format_kst(state['incident_end'])} "
                f"(마감: {state.get('incident_close', '')})",
            )
```

`analysis_subagent.py`의 `bind`:

```python
    def bind(
        self, incident: Incident, previous_incident_report: LogAnalysisReport | None = None
    ) -> CompiledSubAgent:
```

`LogAnalysisRequest(...)`에 `previous_incident_report=previous_incident_report,`를 넣고 import `from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, LogAnalysisStatus`로 바꾼다.

`adapter.py`:
- import 추가:

```python
from cluster_doctor.incident_orchestrator_agent.agent.incident_scope import (
    first_windows,
    scope_lines,
    with_incident_health,
)
```

  `initial_windows` import는 지운다.
- `@pseudonym_scope(fresh=True)` 데코레이터를 지우고 `analyze`를 둘로 나눈다:

```python
    def analyze(self, request: IncidentAnalysisRequest) -> IncidentAnalysisResult:
        """Incident를 분석하며 그래프·최종 종합 실행 오류는 실패 결과로 반환한다.

        호출부와 mutable State를 공유하지 않는다. 예외가 나도 마지막
        커밋된 snapshot의 근거를 보존한다. 최종 리포트는 누적 근거로
        종합하며, 종합 실패 시 구간 리포트를 대신 반환하지 않는다.

        세션의 incident는 세션 대응표로 가명화한다. 이전 보고서의 이름이 이번
        프롬프트에도 실리기 때문이다.
        """
        session = request.session
        with pseudonym_scope(
            fresh=True, mapping=session.pseudonyms if session is not None else None
        ):
            return self._analyze(request)

    def _analyze(self, request: IncidentAnalysisRequest) -> IncidentAnalysisResult:
        incident = request.incident
        kickoff = _KICKOFF.format(
            cluster=incident.cluster,
            trigger=format_kst(incident.trigger_time),
            # 세션의 두 번째 incident부터는 이번 범위의 시작이 아니라 장애의 최초 관측이다.
            # 이번 범위는 scope_lines가 따로 알린다.
            observed_start=format_kst(request.onset),
            observed_end=format_kst(request.observed_end),
        )
        extra = scope_lines(request)
        session = request.session
        final_state = {
            "messages": [HumanMessage(content="\n".join([kickoff, *extra]))],
            **initial_main_agent_state(
                incident_id=incident.incident_id,
                observed_end=request.observed_end,
                pending_windows=first_windows(request),
                total_wait_seconds=request.settling_wait_seconds,
                prior_windows=session.previous_windows if session is not None else (),
                incident_end=request.incident_end,
                incident_close=request.close.value if request.incident_end else "",
            ),
        }
        previous_report = session.previous_report if session is not None else None
        try:
            graph = self._compile(incident, previous_report)
            # (기존 stream 루프 그대로)
```

  기존 `analyze` 본문의 나머지(`for snapshot in graph.stream(...)` 이하)는 `_analyze`로 그대로 옮긴다. 그 안에서:
  - `result = self._result_from(final_state)` 바로 다음 줄에 `result = replace(result, observations=with_incident_health(result.observations, request))`
  - `synthesize_incident(...)` 호출에 `previous_incident_report=previous_report,`를 추가하고, `onset=request.observed_start`를 `onset=request.onset`으로 바꾼다. 사건 종합의 before·onset·after 표시는 장애 시작 기준이어야 한다 — 두 번째 incident부터 `observed_start`를 쓰면 이어지는 장애의 증상이 "onset"으로 표시된다.
- `_compile` 시그니처를 `def _compile(self, incident: Incident, previous_report=None):`로 바꾸고 `self._analysis_subagent.bind(incident, previous_report)`로 부른다.

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent tests/unit/incident_analysis_agent -q --deselect tests/unit/incident_orchestrator_agent/service/test_report_summary.py::test_demo_is_identified_once`
Expected: PASS. 기존 테스트가 `bind(incident)`·`_compile(incident)`를 가짜로 바꿔 끼우고 있으면 새 인자를 기본값으로 받게 가짜를 맞춘다.

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/incident_orchestrator_agent/agent/ src/cluster_doctor/incident_analysis_agent/model/observations.py tests/unit/incident_orchestrator_agent/agent/test_incident_scope.py
git commit -m "feat: Main에 incident 범위와 세션 맥락을 넘기고 health 이력을 종합 전에 합친다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: 보고서 개요와 타임라인

**Files:**
- Create: `src/cluster_doctor/incident_orchestrator_agent/service/report_delivery/projection/incident_scope_rows.py`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/model/incident_report.py:84-103`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/report_delivery/projection/output_mapping.py:28-61`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/report_delivery/projection/incident_timeline.py:789-802`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/report_delivery/rendering/html/report_layout.py:47-75`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/report_delivery/rendering/text/report_text.py:45`
- Modify: `src/cluster_doctor/incident_orchestrator_agent/service/incident_lifecycle/analyze_incident.py` (`_deliver` 호출)
- Test: `tests/unit/incident_orchestrator_agent/service/report_delivery/test_incident_scope_rows.py`

**Interfaces:**
- Consumes: Task 3 (`IncidentClose`, `SessionContext`, `tracking_stopped`), Task 6 (`Observations.incident_span`)
- Produces: `scope_rows(*, observed_start, incident_end, close, session) -> tuple[tuple[str, str], ...]`, `IncidentAnalysisReport.scope_rows`, `to_incident_analysis_report(..., scope_rows=())`

- [ ] **Step 1: 실패하는 테스트**

```python
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentClose, SessionContext
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_scope_rows import (
    scope_rows,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import (
    _health_signals,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import (
    _overview,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_END = _T + timedelta(minutes=20)


def test_rows_for_manual_analysis_are_empty():
    assert scope_rows(observed_start=_T, incident_end=None, close=IncidentClose.NOT_WAITED, session=None) == ()


def test_rows_show_close_session_and_stop():
    session = SessionContext(
        session_id="s1", sequence=3, last_allowed=True, previous_incident_id="i2",
        onset=_T - timedelta(minutes=40),
    )
    rows = dict(scope_rows(observed_start=_T, incident_end=_END, close=IncidentClose.SPAN_LIMIT, session=session))
    assert rows["마감 사유"] == "범위 상한"
    assert rows["세션"] == "s1 · 3번째 · 이전 incident i2"
    assert rows["장애 최초 관측"].startswith("2026-11-13 09:20:00")
    assert rows["추적"] == "장시간 미복구, 자동 추적 중단"


def test_range_row_has_no_microseconds():
    rows = dict(scope_rows(
        observed_start=_T, incident_end=_END + timedelta(microseconds=123456),
        close=IncidentClose.GREEN, session=None,
    ))
    assert "." not in rows["incident 범위"]


def test_html_and_text_show_rows():
    report = IncidentAnalysisReport(observations=Observations(), scope_rows=(("마감 사유", "green 확인"),))
    assert "<th>마감 사유</th><td>green 확인</td>" in _overview(report)
    assert "마감 사유: green 확인" in render_text(report)


def test_health_signals_use_incident_span():
    inside = HealthPoint(at=_T + timedelta(minutes=3), until=_T + timedelta(minutes=3), status="red")
    after = HealthPoint(at=_END + timedelta(minutes=5), until=_END + timedelta(minutes=5), status="red")
    observations = Observations(
        requested=((_T - timedelta(minutes=5), _T + timedelta(minutes=1)),),
        health=(inside, after),
        incident_span=(_T, _END),
    )
    starts = [signal.start for signal in _health_signals(observations, ())]
    assert starts == [inside.at]
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent/service/report_delivery/test_incident_scope_rows.py -v`
Expected: FAIL (`ModuleNotFoundError: ...incident_scope_rows`)

- [ ] **Step 3: 구현**

`incident_scope_rows.py` 생성:

```python
"""보고서 개요에 싣는 incident 범위·마감 사유·세션 줄. HTML과 평문이 같이 쓴다."""

from __future__ import annotations

from datetime import datetime

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentClose,
    SessionContext,
    tracking_stopped,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import (
    stamp,
)


def scope_rows(
    *,
    observed_start: datetime,
    incident_end: datetime | None,
    close: IncidentClose,
    session: SessionContext | None,
) -> tuple[tuple[str, str], ...]:
    if incident_end is None:
        return ()
    rows = [
        (
            "incident 범위",
            # 마감 시각은 벽시계 now라 마이크로초까지 붙는다. 표시에는 초까지만 쓴다.
            f"{stamp(observed_start.replace(microsecond=0))} ~ "
            f"{stamp(incident_end.replace(microsecond=0))}",
        ),
        ("마감 사유", close.value),
    ]
    if session is not None:
        previous = (
            f" · 이전 incident {session.previous_incident_id}"
            if session.previous_incident_id
            else ""
        )
        rows.append(("세션", f"{session.session_id} · {session.sequence}번째{previous}"))
        # 두 번째 incident부터 개요의 "발생 및 인지 일시"는 이번 범위의 시작이다.
        # 장애가 처음 관측된 시각을 따로 보여 준다.
        if session.sequence > 1 and session.onset is not None:
            rows.append(("장애 최초 관측", stamp(session.onset.replace(microsecond=0))))
    if tracking_stopped(close, session):
        rows.append(("추적", "장시간 미복구, 자동 추적 중단"))
    return tuple(rows)
```

`incident_report.py`의 `IncidentAnalysisReport`에 `wiki_patterns` 아래 추가:

```python
    # 개요의 incident 범위·마감 사유·세션 줄 (label, value). 수동 분석은 비어 있다.
    scope_rows: tuple[tuple[str, str], ...] = ()
```

`output_mapping.py`: `to_incident_analysis_report` 키워드 인자에 `scope_rows: tuple[tuple[str, str], ...] = (),`를 추가하고 두 `IncidentAnalysisReport(...)` 생성 모두에 `scope_rows=scope_rows,`를 넣는다.

`analyze_incident.py`:
- import: `from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_scope_rows import scope_rows`
- `_deliver` 키워드 인자에 `scope: tuple[tuple[str, str], ...] = (),`를 추가하고 `to_incident_analysis_report(..., scope_rows=scope)`로 넘긴다.
- `_handle`의 `_deliver` 호출에 추가:

```python
            scope=scope_rows(
                observed_start=command.observed_start,
                incident_end=command.incident_end,
                close=command.close,
                session=command.session,
            ),
```

`incident_timeline.py`의 `_health_signals` 앞부분을 교체:

```python
    # incident 구성 중의 이력이 있으면 incident 범위만 본다. 분석 구간마다 조회한
    # 분석 시점 값은 마감 뒤라 여기서 빠진다. 수동 분석은 분석 구간 기준이다.
    if observations.incident_span is not None:
        window_start, window_end = observations.incident_span
    elif observations.requested:
        window_start = min(start for start, _end in observations.requested)
        window_end = max(end for _start, end in observations.requested)
    else:
        return []
```

`report_layout.py`의 `_overview`: `generated = ...` 다음 줄에 추가하고, 반환 문자열의 `'<tr><th>분석 대상 시간</th>…'` 뒤·`+ generated +` 앞에 `+ scope +`를 넣는다.

```python
    scope = ''.join(
        f'<tr><th>{esc(label)}</th><td>{esc(value)}</td></tr>' for label, value in report.scope_rows
    )
```

`report_text.py` 45행을 교체:

```python
    out=['Elasticsearch 쿼리·노드 로그 분석', report.cluster, *(f'{label}: {value}' for label, value in report.scope_rows), '결론', verdict.headline]
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/incident_orchestrator_agent -q --deselect tests/unit/incident_orchestrator_agent/service/test_report_summary.py::test_demo_is_identified_once`
Expected: PASS. HTML 스냅샷 테스트가 있으면 수동 경로(빈 `scope_rows`)는 출력이 같아야 한다 — 바뀌었으면 구현을 고친다.

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/incident_orchestrator_agent/ tests/unit/incident_orchestrator_agent/service/report_delivery/test_incident_scope_rows.py
git commit -m "feat: 보고서 개요에 incident 범위·마감 사유·세션을, 타임라인에 incident 당시 health를 싣는다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: incident 마감·세션 계속 규칙

**Files:**
- Create: `src/cluster_doctor/kafka_consumer/trigger_settling/service/incident_session.py`
- Test: `tests/unit/kafka_consumer/trigger_settling/service/test_incident_session.py`

**Interfaces:**
- Consumes: `IncidentClose`(Task 3)
- Produces: `INCIDENT_SPAN_LIMIT = timedelta(minutes=20)`, `MAX_INCIDENTS_PER_SESSION = 3`, `Closing(end, close)`, `decide_close(*, start, now, arrived, green, limit=INCIDENT_SPAN_LIMIT) -> Closing | None`, `continue_session(*, sequence, queued, green) -> bool`

- [ ] **Step 1: 실패하는 테스트**

```python
from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentClose
from cluster_doctor.kafka_consumer.trigger_settling.service.incident_session import (
    INCIDENT_SPAN_LIMIT,
    MAX_INCIDENTS_PER_SESSION,
    Closing,
    continue_session,
    decide_close,
)

_S = datetime(2026, 11, 13, 1, 0, tzinfo=UTC)
_NOW = _S + timedelta(minutes=3)


def test_new_signals_keep_incident_open_even_if_green():
    assert decide_close(start=_S, now=_NOW, arrived=True, green=True) is None


def test_quiet_and_green_closes_now():
    assert decide_close(start=_S, now=_NOW, arrived=False, green=True) == Closing(_NOW, IncidentClose.GREEN)


def test_red_or_yellow_keeps_waiting():
    assert decide_close(start=_S, now=_NOW, arrived=False, green=False) is None


def test_health_failure_closes_now():
    assert decide_close(start=_S, now=_NOW, arrived=False, green=None) == Closing(_NOW, IncidentClose.HEALTH_UNKNOWN)


def test_span_limit_closes_at_limit_not_now():
    late = _S + INCIDENT_SPAN_LIMIT + timedelta(minutes=15)
    assert decide_close(start=_S, now=late, arrived=True, green=False) == Closing(
        _S + INCIDENT_SPAN_LIMIT, IncidentClose.SPAN_LIMIT
    )


def test_green_after_limit_ends_at_limit():
    late = _S + INCIDENT_SPAN_LIMIT + timedelta(minutes=1)
    assert decide_close(start=_S, now=late, arrived=False, green=True) == Closing(
        _S + INCIDENT_SPAN_LIMIT, IncidentClose.GREEN
    )


def test_session_continues_on_queue_or_not_green():
    assert continue_session(sequence=1, queued=True, green=None)
    assert continue_session(sequence=1, queued=False, green=False)
    assert not continue_session(sequence=1, queued=False, green=True)
    assert not continue_session(sequence=1, queued=False, green=None)


def test_session_stops_at_limit():
    assert not continue_session(sequence=MAX_INCIDENTS_PER_SESSION, queued=True, green=False)
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/kafka_consumer/trigger_settling/service/test_incident_session.py -v`
Expected: FAIL (`ModuleNotFoundError: ...incident_session`)

- [ ] **Step 3: 구현** — `incident_session.py`

```python
"""incident 범위를 언제 마감하고 세션을 이어 갈지 정하는 규칙.

대기와 마감·계속 판단을 프롬프트가 아니라 코드가 맡는다. 얼마나 기다릴지는
예산이고, "큐가 비었고 green이면 끝"은 판단이 아니라 규칙이다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentClose

# incident 하나의 범위 상한. 시작은 최초 관측(세션 첫 incident) 또는 이전
# incident의 끝이다. 긴 미복구를 이 단위로 나눠 보고서가 무한정 늦지 않게 한다.
INCIDENT_SPAN_LIMIT = timedelta(minutes=20)

# 세션 하나가 만드는 incident 수. 클러스터가 몇 시간째 green이 아니어도 분석
# 비용이 이 배수로 묶인다.
MAX_INCIDENTS_PER_SESSION = 3


@dataclass(frozen=True)
class Closing:
    end: datetime
    close: IncidentClose


def decide_close(
    *,
    start: datetime,
    now: datetime,
    arrived: bool,
    green: bool | None,
    limit: timedelta = INCIDENT_SPAN_LIMIT,
) -> Closing | None:
    """이번 확인에서 incident를 마감할지. ``green``이 None이면 health 조회 실패다.

    새 신호가 있으면 health를 보지 않는다 — 유입이 이어지는 동안은 green이어도
    끝난 것이 아니다. 끝은 상한을 넘지 않는다. 분석이 상한보다 오래 걸려 다음
    incident의 첫 확인이 이미 상한 뒤라면 [시작, 시작+상한]으로 바로 마감된다.
    """
    cap = start + limit
    if not arrived and green is None:
        return Closing(min(now, cap), IncidentClose.HEALTH_UNKNOWN)
    if not arrived and green:
        return Closing(min(now, cap), IncidentClose.GREEN)
    if now >= cap:
        return Closing(cap, IncidentClose.SPAN_LIMIT)
    return None


def continue_session(*, sequence: int, queued: bool, green: bool | None) -> bool:
    """발행 뒤 다음 incident를 만들지.

    큐에 신호가 있으면 상태와 상관없이 잇는다. 큐가 비었으면 green이 아닐 때만
    잇는다. 조회 실패로 끝내는 이유: 상태를 모르는 채 이으면 세션 상한까지 빈
    incident를 분석한다.
    """
    if sequence >= MAX_INCIDENTS_PER_SESSION:
        return False
    if queued:
        return True
    return green is False
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/kafka_consumer/trigger_settling/service/test_incident_session.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/kafka_consumer/trigger_settling/service/incident_session.py tests/unit/kafka_consumer/trigger_settling/service/test_incident_session.py
git commit -m "feat: incident 마감과 세션 계속 규칙을 코드로 둔다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: ProblemLogProcessor 세션 루프와 main 연결

**Files:**
- Modify: `src/cluster_doctor/kafka_consumer/trigger_settling/service/problem_log_processor.py`
- Modify: `src/cluster_doctor/main.py:254-262`
- Test: `tests/unit/kafka_consumer/trigger_settling/service/test_problem_log_session.py`

**Interfaces:**
- Consumes: Task 1 (`fold_health`, `is_green`), Task 3 (`StartIncident`, `SessionContext`, `IncidentClose`), Task 8 (전부)
- Produces: `ProblemLogProcessor(..., cluster_health: Callable[[], Awaitable[dict]] | None = None, incident_span_limit: timedelta = INCIDENT_SPAN_LIMIT)`

- [ ] **Step 1: 실패하는 테스트**

```python
import asyncio
from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisDetails,
    IncidentClose,
    IncidentOutcome,
)
from cluster_doctor.kafka_consumer.trigger_settling.service.inflow import ProblemLogSignal
from cluster_doctor.kafka_consumer.trigger_settling.service.problem_log_processor import (
    ProblemLogProcessor,
)


class _Analysis:
    def __init__(self, on_handle=None):
        self.commands = []
        self.on_handle = on_handle

    async def handle(self, command):
        self.commands.append(command)
        if self.on_handle:
            await self.on_handle(len(self.commands))
        start = command.observed_start
        return IncidentOutcome(
            incident_id=command.incident.incident_id,
            status=IncidentStatus.COMPLETED,
            diagnostics=IncidentAnalysisDetails(
                observations=Observations(requested=((start, start + timedelta(minutes=1)),))
            ),
        )


def _health(*statuses):
    remaining = list(statuses)

    async def probe():
        status = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        if isinstance(status, Exception):
            raise status
        return {"status": status, "unassigned_shards": 0}

    return probe


def _processor(analysis, health=None, span=timedelta(milliseconds=60)):
    return ProblemLogProcessor(
        analyze_incident=analysis,
        cluster_health=health,
        quiet_period_seconds=0.005,
        max_settling_wait_seconds=0.05,
        incident_span_limit=span,
    )


async def _run(processor):
    await processor.submit(ProblemLogSignal(timestamp=datetime.now(UTC)))
    await processor._task


def test_green_makes_single_incident():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, _health("green"))))
    [command] = analysis.commands
    assert command.close is IncidentClose.GREEN
    assert command.session.sequence == 1
    assert command.health_history[0].status == "green"


def test_red_splits_into_session_of_three():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, _health("red"))))
    assert [c.session.sequence for c in analysis.commands] == [1, 2, 3]
    assert [c.session.last_allowed for c in analysis.commands] == [False, False, True]
    assert all(c.close is IncidentClose.SPAN_LIMIT for c in analysis.commands)
    first, second, third = analysis.commands
    assert second.observed_start == first.incident_end
    assert second.session.previous_incident_id == first.incident.incident_id
    assert len(third.session.previous_windows) == 2
    assert third.session.pseudonyms is first.session.pseudonyms
    assert third.session.onset == first.observed_start


def test_health_failure_closes_and_ends_session():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, _health(RuntimeError("down")))))
    [command] = analysis.commands
    assert command.close is IncidentClose.HEALTH_UNKNOWN
    assert command.health_gaps == ("health 확인 불가로 incident를 마감했다",)


def test_signal_during_analysis_continues_session_even_if_green():
    processor = None

    async def late_signal(count):
        if count == 1:
            await processor.submit(ProblemLogSignal(timestamp=datetime.now(UTC)))

    analysis = _Analysis(on_handle=late_signal)
    processor = _processor(analysis, _health("green"))
    asyncio.run(_run(processor))
    assert [c.session.sequence for c in analysis.commands] == [1, 2]


def test_without_probe_behaves_as_before():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, None)))
    [command] = analysis.commands
    assert command.incident_end is None
    assert command.session is None
    assert command.close is IncidentClose.NOT_WAITED
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/kafka_consumer/trigger_settling/service/test_problem_log_session.py -v`
Expected: FAIL (`TypeError: ... unexpected keyword argument 'cluster_health'`)

- [ ] **Step 3: 구현** — `problem_log_processor.py`

모듈 docstring을 `"""problem-log 신호를 받아 유입을 정착시키고, health를 보며 incident 세션을 만든다."""`로 바꾼다.

import 추가:

```python
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import (
    HealthPoint,
    fold_health,
    is_green,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentClose,
    IncidentOutcome,
    SessionContext,
    StartIncident,
)
from cluster_doctor.kafka_consumer.trigger_settling.service.incident_session import (
    INCIDENT_SPAN_LIMIT,
    MAX_INCIDENTS_PER_SESSION,
    continue_session,
    decide_close,
)

HealthProbe = Callable[[], Awaitable[dict]]
```

기존 `from datetime import UTC, datetime`과 `from ...model.lifecycle import StartIncident` 줄은 위 import로 합쳐지므로 지운다(중복 import를 남기지 않는다). kafka_consumer가 `incident_analysis_agent.model`(`health_point`, `time_range`)을 직접 import하는 것은 의존 방향(kafka_consumer → orchestrator → analysis)을 거스르지 않는다.

`_Arrival` 아래에 추가:

```python
@dataclass(frozen=True)
class _Built:
    """incident 구성이 끝난 결과."""

    end: datetime
    close: IncidentClose
    history: tuple[HealthPoint, ...]
    gaps: tuple[str, ...]
```

클래스 docstring을 교체:

```python
    """문제성 로그 신호를 큐에 모아 정착시키고, health를 보며 incident 세션을 만든다.

    정착·incident 구성·분석은 하나의 순차 루프다 — 한 incident의 분석이 끝나야
    큐와 health를 다시 본다. 그 사이 들어온 신호는 큐에 쌓였다가 다음 incident에
    합쳐진다. ``cluster_health``가 없으면 정착 직후 incident 하나로 끝난다.
    """
```

`__init__`에 인자와 필드 추가(기존 인자 순서 유지, 키워드 전용):

```python
        cluster_health: HealthProbe | None = None,
        incident_span_limit: timedelta = INCIDENT_SPAN_LIMIT,
        ...
        self._cluster_health = cluster_health
        self._incident_span_limit = incident_span_limit
```

`_process_pending`의 `while` 본문을 교체한다(바깥 try/finally는 유지):

```python
            while not self._closed and not self._pending.empty():
                first = self._pending.get_nowait()
                tracker = await self._settle(first)
                if self._closed:
                    return
                await self._run_session(first, tracker)
```

새 메서드들:

```python
    async def _run_session(self, first: _Arrival, tracker: InflowTracker) -> None:
        if self._cluster_health is None:
            await self._handle(
                StartIncident(
                    self._new_incident(first, first.trigger.timestamp),
                    tracker.first_seen,
                    tracker.last_seen,
                    tracker.total_wait_seconds,
                )
            )
            return

        start = tracker.first_seen
        session: SessionContext | None = None
        previous: tuple[str, IncidentOutcome | None] | None = None
        sequence = 1
        while True:
            built = await self._build_incident(tracker, start)
            if self._closed:
                return
            last_allowed = sequence >= MAX_INCIDENTS_PER_SESSION
            if session is None:
                session = SessionContext(
                    session_id=uuid.uuid4().hex[:12],
                    sequence=1,
                    last_allowed=last_allowed,
                    onset=tracker.first_seen,
                )
            else:
                previous_id, outcome = previous
                session = session.following(
                    incident_id=previous_id,
                    report=outcome.report if outcome is not None else None,
                    windows=_analyzed_windows(outcome),
                    last_allowed=last_allowed,
                )
            incident = self._new_incident(
                first, first.trigger.timestamp if sequence == 1 else start
            )
            outcome = await self._handle(
                StartIncident(
                    incident,
                    start,
                    max(start, min(tracker.last_seen, built.end)),
                    tracker.total_wait_seconds if sequence == 1 else 0.0,
                    incident_end=built.end,
                    close=built.close,
                    health_history=built.history,
                    health_gaps=built.gaps,
                    session=session,
                )
            )
            if self._closed:
                return
            queued = not self._pending.empty()
            green = None if queued else await self._probe_green()
            if not continue_session(sequence=sequence, queued=queued, green=green):
                return
            previous = (incident.incident_id, outcome)
            start = built.end
            tracker = InflowTracker(
                first_seen=start, last_seen=start, time_basis=tracker.time_basis
            )
            sequence += 1

    async def _build_incident(self, tracker: InflowTracker, start: datetime) -> _Built:
        """큐가 비었고 green이면, 아니면 범위 상한에서 마감한다."""
        history: tuple[HealthPoint, ...] = ()
        while True:
            now = datetime.now(UTC)
            arrivals = self._drain_pending()
            if arrivals:
                tracker.observe([arrival.trigger for arrival in arrivals], now=now)
            green: bool | None = False
            if not arrivals:
                try:
                    payload = await self._cluster_health()
                    history = fold_health(history, payload, now)
                    green = is_green(payload)
                except Exception as exc:
                    _logger.warning("[session] cluster health 조회 실패: %s", type(exc).__name__)
                    green = None
            decided = decide_close(
                start=start, now=now, arrived=bool(arrivals), green=green,
                limit=self._incident_span_limit,
            )
            if decided is not None:
                gaps = (
                    ("health 확인 불가로 incident를 마감했다",)
                    if decided.close is IncidentClose.HEALTH_UNKNOWN
                    else ()
                )
                return _Built(decided.end, decided.close, history, gaps)
            await asyncio.sleep(self._quiet_period_seconds)

    async def _probe_green(self) -> bool | None:
        try:
            return is_green(await self._cluster_health())
        except Exception as exc:
            _logger.warning("[session] cluster health 조회 실패: %s", type(exc).__name__)
            return None

    async def _handle(self, command: StartIncident) -> IncidentOutcome | None:
        self._in_flight_analysis = True
        try:
            return await self._analysis_service.handle(command)
        except Exception:
            _logger.exception("incident analysis failed; settling loop will continue")
            return None
        finally:
            self._in_flight_analysis = False

    def _new_incident(self, first: _Arrival, trigger_time: datetime) -> Incident:
        return Incident(
            incident_id=uuid.uuid4().hex[:12],
            cluster=self._cluster,
            trigger_time=trigger_time,
            kafka_receive_time=first.received_at,
            trigger_type=TriggerType.PROBLEM_LOG,
        )
```

모듈 끝에 추가:

```python
def _analyzed_windows(outcome: IncidentOutcome | None) -> tuple[TimeRange, ...]:
    """이전 incident가 분석한 구간. 다음 incident가 다시 분석하지 않게 넘긴다."""
    if outcome is None:
        return ()
    return tuple(
        TimeRange(start=start, end=end)
        for start, end in outcome.diagnostics.observations.requested
    )
```

기존 `_process_pending` 안의 `Incident(...)`·`StartIncident(...)`·in-flight 블록은 `_run_session`/`_handle`로 옮겨졌으므로 지운다.

`main.py`의 `main()`:

```python
    with build_runtime_resources(settings) as runtime_resources:
        es_cluster = ElasticsearchClusterAdapter(runtime_resources.es_client)

        async def cluster_health() -> dict:
            # ES client는 동기다. 이벤트 루프를 막지 않게 스레드에서 부른다.
            return await asyncio.to_thread(es_cluster.health)

        problem_log_processor = ProblemLogProcessor(
            analyze_incident=compose_analysis_service(settings, runtime_resources),
            cluster=settings.cluster_name,
            cluster_health=cluster_health,
            max_pending=10_000,
        )
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/unit/kafka_consumer -q`
Expected: PASS(기존 정착 테스트 포함 — 기존 테스트는 `cluster_health`를 주지 않으므로 이전 동작 그대로).

- [ ] **Step 5: Commit**

```bash
git add src/cluster_doctor/kafka_consumer/ src/cluster_doctor/main.py tests/unit/kafka_consumer/trigger_settling/service/test_problem_log_session.py
git commit -m "feat: 정착 뒤 health를 보며 incident 세션을 만든다

큐가 비었고 green이면 incident를 마감하고, 아니면 20분 범위 상한에서
마감한다. 발행 뒤에도 green이 아니거나 큐에 신호가 있으면 이전 보고서를
넘기며 다음 incident를 만든다(세션당 3개).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: 전체 검증과 문서 커밋

**Files:**
- Check: `README.md`, `src/cluster_doctor/kafka_consumer/CLAUDE.md`, `src/cluster_doctor/incident_orchestrator_agent/CLAUDE.md`, `src/cluster_doctor/incident_analysis_agent/CLAUDE.md`, `.claude/plans/*.md`

- [ ] **Step 1: 전체 테스트**

Run: `uv run pytest -q --deselect tests/unit/incident_orchestrator_agent/service/test_report_summary.py::test_demo_is_identified_once`
Expected: 전부 PASS

- [ ] **Step 2: 컴파일·린트**

Run: `uv run python -m compileall -q src` → 출력 없음
Run: `uvx ruff check src tests` → 이번 변경 파일에 새 경고 없음(기존 I001·FURB167은 제외)

- [ ] **Step 3: Python 3.11**

```bash
uv venv --python 3.11 .venv311
UV_PROJECT_ENVIRONMENT=.venv311 uv sync
UV_PROJECT_ENVIRONMENT=.venv311 uv run pytest -q --deselect tests/unit/incident_orchestrator_agent/service/test_report_summary.py::test_demo_is_identified_once
```

Expected: 전부 PASS. 끝나면 `.venv311`을 지운다.

- [ ] **Step 4: 문서와 구현 이름 대조**

Grep 도구(패턴 `incident_session|fold_health|prior_windows|IncidentClose|세션당|범위 상한|20분`, 대상 `README.md`와 `src/cluster_doctor/*/CLAUDE.md`)로 문서가 가리키는 이름이 실제 코드에 있는지 확인한다. `IncidentClose.SPAN_LIMIT`의 표시 문자열은 "범위 상한"이다 — 문서에 "20분 상한"으로 적힌 마감 사유가 있으면 "범위 상한"으로 맞춘다. 두 번째 incident부터 Main 시작 메시지의 "최초 관측 시각"이 세션 첫 관측(`request.onset`)이라는 점이 README Main Agent 절과 맞는지도 본다.

- [ ] **Step 5: 문서 커밋**

```bash
git add README.md src/cluster_doctor/kafka_consumer/CLAUDE.md src/cluster_doctor/incident_orchestrator_agent/CLAUDE.md src/cluster_doctor/incident_analysis_agent/CLAUDE.md .gitignore .claude/plans/health-aware-incident-design.md .claude/plans/health-aware-incident-plan.md
git commit -m "docs: health 기반 incident 세션을 README와 패키지 문서에 적는다

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: 배포 뒤 확인할 서버 로그 (사용자에게 요청)**

- `[session] cluster health 조회 실패` 빈도
- 세션당 incident 수 분포(같은 `session_id`가 찍힌 보고서 수)
- 보고서 개요의 마감 사유 분포(green 확인 / 범위 상한 / health 확인 불가)
