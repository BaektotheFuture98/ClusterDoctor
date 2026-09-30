"""Stateless admission of one state-changing tool per model turn."""

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, ToolMessage


def pending_calls(state: dict) -> list[dict]:
    for message in reversed(state.get("messages", ())):
        if isinstance(message, AIMessage):
            answered = {
                m.tool_call_id
                for m in state.get("messages", ())
                if isinstance(m, ToolMessage)
            }
            return [call for call in message.tool_calls if call["id"] not in answered]
    return []


def block_extra_calls(state: dict, mutating: frozenset[str]) -> list[ToolMessage]:
    calls = [call for call in pending_calls(state) if call["name"] in mutating]
    return [
        ToolMessage(
            "한 turn에는 상태 변경 도구를 하나만 실행한다. 다음 turn에 다시 요청해라.",
            tool_call_id=call["id"],
            name=call["name"],
            status="error",
        )
        for call in calls[1:]
    ]


class AnalysisToolAdmissionMiddleware(AgentMiddleware):
    def after_model(self, state, runtime):
        blocked = block_extra_calls(
            state,
            frozenset({"collect_evidence", "write_report", "report_insufficient"}),
        )
        return {"messages": blocked} if blocked else None
