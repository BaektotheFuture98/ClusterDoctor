"""Diagnosis SubAgent 루프 드라이버."""

from __future__ import annotations

import logging

from langgraph.errors import GraphRecursionError

from cluster_doctor.adapters.outbound.deepagents.diagnosis.session import _DiagnosisSession

_logger = logging.getLogger(__name__)

# 그래프 재귀 상한. 도구가 실제로 하는 일은 위 상한들이 이미 묶으므로 이것은
# **아무 일도 안 하면서 도는** 모델에 대한 최후 방어다. 도구가 멱등해도 모델
# 호출 자체는 매 턴 토큰을 쓰고, 그 비용은 미들웨어가 선점한 예산 하나 안에서
# 발생한다 — 그래서 넉넉하게 잡지 않는다.
#
# 의도한 흐름은 수집 1 + 리포트 2 + 확장 요청 1 + 마무리 1 = 다섯 턴이고,
# langgraph는 한 턴을 두 superstep 남짓으로 센다. 24면 두 배의 여유가 있다.
# 상한에 닿으면 ``GraphRecursionError``가 나고, 그것을 잡아 그때까지 실제로
# 끝난 일로 결과를 조립한다 — 위임이 통째로 날아가면 이미 쓴 LLM 비용과 모은
# 근거가 함께 사라지기 때문이다.
_RECURSION_LIMIT = 24


def _drive(graph, agent_state: dict, delegation: _DiagnosisSession) -> None:
    """모델 루프를 돌린다. **무슨 일이 있어도 예외를 올리지 않는다.**

    포트 계약이 "예외를 올리지 않는다"인 이유가 여기에도 그대로 있다. 루프가
    죽어도 그때까지 모은 근거와 쓴 리포트는 유효하고, 그것을 버리면 이미 쓴
    LLM 비용까지 함께 버리는 것이다. 실패는 ``gaps``에 문장으로 남아 운영자에게
    닿고, 결과 조립은 실제로 끝난 일만 보고 계속된다.
    """
    try:
        graph.invoke(dict(agent_state), {"recursion_limit": _RECURSION_LIMIT})
    except GraphRecursionError:
        _logger.error(
            "[diagnosis] 재귀 상한 %d에 도달했다 — 그때까지 끝난 일로 결과를 만든다",
            _RECURSION_LIMIT,
        )
        delegation.run_state.mark_gap(
            "진단 루프가 상한에 도달해 중단됐다. 이 구간의 조사가 끝까지 가지 못했다."
        )
    except Exception as exc:  # noqa: BLE001 - 위 docstring 참조
        _logger.exception("[diagnosis] 진단 루프가 실패했다: %s", exc)
        delegation.run_state.mark_gap(f"진단 루프가 실패했다: {exc}")
