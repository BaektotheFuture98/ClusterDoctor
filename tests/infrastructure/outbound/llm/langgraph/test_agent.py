import os

from elasticsearch import Elasticsearch
from langchain_core.messages import SystemMessage, ToolMessage
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, StateGraph, MessagesState

from cluster_doctor.infrastructure.outbound.elasticsearch.es_cluster_adapter import ElasticsearchClusterAdapter


def _make_adapter() -> ElasticsearchClusterAdapter:
    return ElasticsearchClusterAdapter(
        Elasticsearch(
            f"http://{os.environ['ES_HOST']}:{os.environ.get('ES_PORT', '9200')}",
            basic_auth=(os.environ["ES_USER"], os.environ["ES_PASSWORD"]),
        )
    )


@tool
def cluster_health_check() -> dict:
    """Elasticsearch 클러스터 헬스 상태를 반환한다."""
    return _make_adapter().health()


_TOOLS = [cluster_health_check]
_TOOLS_BY_NAME = {t.name: t for t in _TOOLS}

_SYSTEM = SystemMessage(content="You are an IT manager handling an Elasticsearch cluster.")


def _make_llm():
    return ChatGoogleGenerativeAI(
        model=os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite"),
        google_api_key=os.environ["GEMINI_API_KEY"],
    ).bind_tools(_TOOLS)


def llm_call(state: MessagesState) -> dict:
    llm_with_tools = _make_llm()
    return {
        "messages": [
            llm_with_tools.invoke([_SYSTEM] + state["messages"])
        ]
    }


def tool_node(state: MessagesState) -> dict:
    result = []
    for tool_call in state["messages"][-1].tool_calls:
        t = _TOOLS_BY_NAME[tool_call["name"]]
        observation = t.invoke(tool_call["args"])
        result.append(ToolMessage(content=str(observation), tool_call_id=tool_call["id"]))
    return {"messages": result}


def _should_continue(state: MessagesState) -> str:
    if state["messages"][-1].tool_calls:
        return "tool_node"
    return END


def build_graph():
    builder = StateGraph(MessagesState)
    builder.add_node("llm_call", llm_call)
    builder.add_node("tool_node", tool_node)
    builder.set_entry_point("llm_call")
    builder.add_conditional_edges("llm_call", _should_continue)
    builder.add_edge("tool_node", "llm_call")
    return builder.compile()


if __name__ == "__main__":
    graph = build_graph()
    result = graph.invoke({"messages": [("user", "현재 Elasticsearch 클러스터 상태를 확인해줘")]})
    print(result["messages"][-1].content)
