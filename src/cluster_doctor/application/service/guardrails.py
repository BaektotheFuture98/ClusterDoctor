"""런타임이 강제하는 상한.

**프롬프트 문장으로는 아무것도 막을 수 없다.** 모델은 지시를 어기고,
실패한 호출은 재시도로 증폭된다. 그래서 여기 있는 것은 전부 호출 경로 위의
코드다 — Agent가 우회할 수 있는 자리에 두지 않는다.

상한은 셋으로 나뉜다.

  Request     들어온 요청이 형식과 범위를 지키는가.
  Runtime     한 Incident가 쓸 수 있는 예산(분·대기·수정·시간)을 넘었는가.
  Datasource  한 번의 조회가 가져올 수 있는 양.

Datasource 상한 중 ClickHouse 행 수와 노드 로그 줄 수는 이미 어댑터와 포트가
갖고 있다(``_MAX_ROWS_PER_SEGMENT_PER_SOURCE``, ``clamp_node_log_limit``).
여기서 다시 정의하지 않는다 — 같은 값을 두 곳에 박으면 한쪽만 고쳐지는 날이
온다.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from math import ceil

from cluster_doctor.application.exception import GuardrailViolation
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.time_range import (
    MAX_TIME_RANGE_DURATION,
    TimeRange,
    is_covered,
)

_logger = logging.getLogger(__name__)

# 분석 창 상한. 도메인의 ``TimeRange``가 같은 제약을 강제하므로 값을 두 번 쓰지
# 않는다. 그래도 여기서 다시 보는 이유는 반환 형태가 다르기 때문이다 — 도메인은
# 어떤 호출자에게든 예외를 던지고, 여기서는 Supervisor 사이클이 잡아 다음 행동을
# 고를 수 있는 ``GuardrailViolation``으로 만든다.
MAX_ANALYSIS_WINDOW_MINUTES = int(MAX_TIME_RANGE_DURATION.total_seconds() // 60)

# 한 Incident가 분석할 수 있는 **분 수**. 이것이 주 예산이다.
#
# 호출 수로 세면 안 된다. 비용 동인은 호출이 아니라 분이다 — 조회가 분 단위로
# 쪼개지고, 비어 있지 않은 분마다 datasource별로 LLM이 한 번씩 돈다(5분 창
# 실측 513,122 토큰). 호출 수로 세면 1분 창과 10분 창이 같은 예산을 쓰고,
# 그러면 두 가지가 동시에 나빠진다. 15분짜리 제안이 10분+5분으로 쪼개질 때
# 예산 2를 먹고, 1분짜리 gap 하나를 메우는 데도 예산 1을 먹는다.
MAX_ANALYZED_MINUTES = 60

# 호출 수 상한은 **2차 안전장치로만** 남긴다. 분 예산이 주 제약이므로 이 값은
# 폭주(1분짜리 요청을 수십 번)를 막는 데만 쓰인다.
MAX_ANALYSIS_CALLS = 12

# Supervisor 사이클 상한. 호출 상한보다 크게 두는 이유: Guardrail이 거절한
# 사이클은 분석을 하지 않았으므로 호출 수를 늘리지 않는다. 그 갈래에서 모델이
# 같은 요청을 반복하면 호출 상한에 영원히 닿지 못한 채 돌게 된다.
MAX_SUPERVISOR_CYCLES = MAX_ANALYSIS_CALLS + 4

# 연속으로 거절당할 수 있는 횟수. 모델이 상한을 이해하지 못하고 같은 요청을
# 되풀이하면 여기서 끊는다.
MAX_REJECTED_DECISIONS = 3

# Evidence와 Report가 어긋났을 때 다시 쓰게 하는 횟수. 0이면 검증 결과를
# 기록만 하고 고치지 않는다는 뜻이고, 2를 넘기면 같은 지적을 되풀이하며
# 호출만 태우는 것을 실패 모드로 본다.
MAX_REPORT_REVISIONS = 1

# 유입이 멎기를 기다리는 예산. 기다리는 동안 분석은 시작조차 되지 않고 큐만
# 쌓이므로, 1회를 짧게 끊어 매 사이클 유입을 다시 본다.
MAX_SINGLE_WAIT_SECONDS = 60
MAX_TOTAL_WAIT_SECONDS = 300

# Incident 하나가 점유할 수 있는 벽시계 시간. 대기·조회·LLM이 각자 상한을
# 갖고 있어도 그 곱은 묶이지 않는다.
INCIDENT_TIMEOUT_SECONDS = 1_800

# 한 datasource workflow가 남길 수 있는 Evidence 수. Reduce가 "중요하지 않은
# 것을 지운다"를 수행하지 않고 전부 통과시키면 Cross-source 단계의 프롬프트가
# 원문 크기로 돌아간다.
MAX_EVIDENCE_PER_SOURCE = 25
MAX_EVIDENCE_TOTAL = 80

# LLM에게 보여 줄 원문 한 덩어리의 상한.
MAX_RAW_LOG_CHARS = 60_000

# SSH 타임아웃(접속/명령).
SSH_CONNECT_TIMEOUT_SECONDS = 10
SSH_COMMAND_TIMEOUT_SECONDS = 30


class CancellationToken:
    """Incident 취소 신호.

    ``threading.Event``를 그대로 쓰지 않고 감싸는 이유는 이름이다 —
    ``event.is_set()``은 무엇이 일어났는지 말하지 않지만
    ``token.raise_if_cancelled()``는 말한다.
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason = ""

    def cancel(self, reason: str = "") -> None:
        self._reason = reason
        self._event.set()

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> str:
        return self._reason

    def raise_if_cancelled(self) -> None:
        if self._event.is_set():
            raise GuardrailViolation(f"취소됨{f': {self._reason}' if self._reason else ''}")


@dataclass
class Deadline:
    """벽시계 상한 하나.

    시작 시각을 생성 시점에 박는다. 호출부가 매번 경과 시간을 계산하면 그
    계산이 여러 벌이 되고, 한 곳에서 빠뜨리면 상한이 사라진다.
    """

    seconds: float
    started_at: float = 0.0

    def __post_init__(self) -> None:
        if not self.started_at:
            self.started_at = time.monotonic()

    @property
    def remaining(self) -> float:
        return max(0.0, self.seconds - (time.monotonic() - self.started_at))

    @property
    def expired(self) -> bool:
        return self.remaining <= 0.0

    def raise_if_expired(self, what: str) -> None:
        if self.expired:
            raise GuardrailViolation(f"{what} 실행 시간 상한 {self.seconds:.0f}초 초과")


def window_minutes(window: TimeRange) -> int:
    """구간의 분 수. 예산 회계의 단위다.

    올림한다. 90초짜리 구간은 분 버킷 둘을 만들고 LLM도 두 번 돈다.
    """
    seconds = (window.end - window.start).total_seconds()
    return max(1, ceil(seconds / 60))


def remaining_minutes(state: IncidentState) -> int:
    return max(0, MAX_ANALYZED_MINUTES - state.analyzed_minutes)


def check_analysis_budget(state: IncidentState) -> None:
    """이 Incident가 분석을 한 번 더 시킬 수 있는가."""
    if remaining_minutes(state) <= 0:
        raise GuardrailViolation(
            f"분석 예산 {MAX_ANALYZED_MINUTES}분을 모두 썼다"
        )
    if state.analysis_call_count >= MAX_ANALYSIS_CALLS:
        raise GuardrailViolation(
            f"분석 호출 상한 {MAX_ANALYSIS_CALLS}회에 도달했다"
        )


def fit_to_budget(window: TimeRange, state: IncidentState) -> TimeRange:
    """남은 예산에 맞게 구간을 줄인다. 예산이 없으면 ``GuardrailViolation``.

    거절하지 않고 줄이는 이유: 4분이 남았는데 10분을 요청받아 통째로 거절하면
    남은 4분을 쓰지 못한 채 Incident가 끝난다. 앞쪽을 남기는 것은 Supervisor가
    시작 시각을 의도해서 고르기 때문이다 — 원인은 구간 앞에서 만들어지는
    경우가 많고, 뒤를 남기면 그 판단이 뒤집힌다.
    """
    budget = remaining_minutes(state)
    if budget <= 0:
        raise GuardrailViolation(f"분석 예산 {MAX_ANALYZED_MINUTES}분을 모두 썼다")
    if window_minutes(window) <= budget:
        return window
    return TimeRange(start=window.start, end=window.start + timedelta(minutes=budget))


def admit_window(window: TimeRange, state: IncidentState) -> TimeRange:
    """Guardrail을 통과한 실제 분석 구간. 통과하지 못하면 ``GuardrailViolation``.

    **부분 중복은 잘라서 통과시킨다.** 13:50~14:05를 요청받았고 14:00~14:10이
    이미 분석됐다면 13:50~14:00으로 좁힌다 — 요청 전체를 거절하면 모델이 똑같은
    요청을 다시 내놓고 사이클만 태운다.

    승인 경로가 여기 하나뿐이어야 한다. 두 벌이 되면 어느 쪽이 승인한 구간이
    맞는지 판정할 근거가 없고, 예산 회계는 승인된 구간을 기준으로 한다.
    """
    check_analysis_budget(state)

    remaining = state.remaining_of(window)
    if not remaining:
        raise GuardrailViolation(
            f"{window.start:%H:%M}~{window.end:%H:%M} 구간은 이미 전부 분석했다"
        )

    # 앞 조각만 쓴다. 남은 조각이 여럿이면 뒤쪽은 다음 제안에서 다시 계산된다 —
    # 한 번에 둘을 승인하면 승인 하나에 위임 하나라는 회계가 깨진다.
    admitted = fit_to_budget(remaining[0], state)
    check_not_duplicate(admitted, state)
    return admitted


def check_not_duplicate(window: TimeRange, state: IncidentState) -> None:
    """이미 본 구간을 다시 보려는가.

    **완전히 같은 구간만 막는 것이 아니다.** 14:00~14:10을 분석한 뒤
    14:02~14:05를 요청하면 새로 얻는 것이 없는데, 같은지만 보면 통과한다.
    덮였는지로 판정하는 이유가 그것이다.
    """
    if is_covered(window, state.analyzed_windows):
        raise GuardrailViolation(
            f"{window.start:%H:%M}~{window.end:%H:%M} 구간은 이미 분석했다"
        )


def clamp_wait(requested_seconds: float, state: IncidentState) -> float:
    """실제로 기다릴 시간. 1회 상한과 누적 예산을 함께 적용한다."""
    remaining = MAX_TOTAL_WAIT_SECONDS - state.total_wait_seconds
    return max(0.0, min(float(requested_seconds), float(MAX_SINGLE_WAIT_SECONDS), remaining))


def clamp_evidence(items: list, limit: int, *, what: str) -> list:
    """Evidence 수를 상한으로 자르고 잘렸다는 사실을 남긴다.

    조용히 자르지 않는 이유: Cross-source 단계는 받은 목록이 전부라고 믿고
    추론한다. 잘린 것을 모르면 "그 시각에는 아무 일도 없었다"가 된다.
    """
    if len(items) <= limit:
        return items
    _logger.warning("[guardrail] %s Evidence %d건을 상한 %d건으로 자른다", what, len(items), limit)
    return items[:limit]


def truncate_raw(text: str, limit: int = MAX_RAW_LOG_CHARS) -> str:
    """프롬프트에 실을 원문을 상한으로 자른다. 잘린 사실을 본문에 적는다."""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... (원문 {len(text) - limit}자를 상한으로 잘랐다)"
