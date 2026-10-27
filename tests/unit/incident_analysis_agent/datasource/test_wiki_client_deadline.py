"""MonitoringWikiClient가 설정한 상한 안에서 끝나는지 로컬 서버로 검증한다.

운영 상한 30초 대신 짧은 값을 쓴다. 상한을 못 지키는 코드는 이 파일에서 300초 가까이
붙잡히므로, 호출은 별도 스레드에서 돌리고 기한 안에 안 끝나면 실패로 본다.
"""

import asyncio
import socket
import threading
import time

import pytest
import uvicorn
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from cluster_doctor.incident_analysis_agent.datasource.mcp import wiki_client
from cluster_doctor.incident_analysis_agent.datasource.mcp.wiki_client import (
    MonitoringWikiClient,
)

_DEADLINE_SECONDS = 4
# 상한 + 정리 + 여유. 이 안에 못 끝나면 상한이 지켜지지 않은 것이다.
_ALLOWED_SECONDS = _DEADLINE_SECONDS + 4


@pytest.fixture(autouse=True)
def short_deadline(monkeypatch):
    monkeypatch.setattr(wiki_client, "_TIMEOUT_SECONDS", _DEADLINE_SECONDS)


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def start_wiki_server():
    """`get_index` tool이 delay_seconds 뒤에 답하는 실제 MCP 서버를 띄운다."""
    servers: list[uvicorn.Server] = []

    def start(delay_seconds: float) -> str:
        server = MCPServer("fake-wiki")

        @server.tool()
        async def get_index() -> str:
            await asyncio.sleep(delay_seconds)
            return "wiki:index"

        port = _free_port()
        config = uvicorn.Config(
            server.streamable_http_app(), host="127.0.0.1", port=port, log_level="error"
        )
        running = uvicorn.Server(config)
        threading.Thread(target=running.run, daemon=True).start()
        started_by = time.monotonic() + 10
        while not running.started and time.monotonic() < started_by:
            time.sleep(0.05)
        assert running.started, "테스트용 MCP 서버가 뜨지 않았다"
        servers.append(running)
        return f"http://127.0.0.1:{port}/mcp"

    yield start
    for running in servers:
        running.should_exit = True


@pytest.fixture
def unresponsive_url():
    """TCP 연결은 받지만 아무 응답도 하지 않는 서버."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    held: list[socket.socket] = []

    def accept_forever():
        while True:
            try:
                connection, _ = listener.accept()
            except OSError:
                return
            held.append(connection)

    threading.Thread(target=accept_forever, daemon=True).start()
    yield f"http://127.0.0.1:{listener.getsockname()[1]}/mcp"
    listener.close()
    for connection in held:
        connection.close()


def _get_index_with_watchdog(url: str) -> tuple[dict, bool]:
    """get_index를 별도 스레드에서 실행하고 (결과, 기한 안에 끝났는지)를 돌려준다."""
    outcome: dict = {}

    def call() -> None:
        started = time.monotonic()
        try:
            outcome["answer"] = MonitoringWikiClient(url).get_index()
        except BaseException as exc:  # noqa: BLE001 - 어떤 예외로 끝나는지 그대로 본다
            outcome["error"] = exc
        outcome["elapsed"] = time.monotonic() - started

    worker = threading.Thread(target=call, daemon=True)
    worker.start()
    worker.join(_ALLOWED_SECONDS)
    return outcome, not worker.is_alive()


def test_returns_the_answer_from_the_server(start_wiki_server):
    outcome, finished = _get_index_with_watchdog(start_wiki_server(delay_seconds=0))

    assert finished
    assert outcome["answer"] == "wiki:index"


def test_slow_tool_ends_at_the_deadline(start_wiki_server):
    outcome, finished = _get_index_with_watchdog(start_wiki_server(delay_seconds=30))

    assert finished, f"{_ALLOWED_SECONDS}초 안에 끝나지 않았다"
    assert isinstance(outcome["error"], Exception)  # BaseException이면 synthesize_incident의 except를 빠져나간다


def test_unresponsive_server_ends_at_the_deadline(unresponsive_url):
    outcome, finished = _get_index_with_watchdog(unresponsive_url)

    assert finished, f"{_ALLOWED_SECONDS}초 안에 끝나지 않았다 — 설정 상한을 무시하고 붙잡혔다"
    assert isinstance(outcome["error"], Exception)  # BaseException이면 synthesize_incident의 except를 빠져나간다
    assert outcome["elapsed"] < _ALLOWED_SECONDS


@pytest.fixture
def start_recording_server():
    """get_index·get_pattern이 받은 인자를 기록하고 정해진 답을 돌려주는 MCP 서버를 띄운다."""
    servers: list[uvicorn.Server] = []

    def start(index_text: str = "| 패턴 |") -> tuple[str, list[dict]]:
        calls: list[dict] = []
        server = MCPServer("fake-wiki")

        @server.tool()
        async def get_index() -> str:
            calls.append({"tool": "get_index"})
            return index_text

        @server.tool()
        async def get_pattern(id: str, sections: list[str] | None = None) -> str:
            calls.append({"tool": "get_pattern", "id": id, "sections": sections})
            if id == "missing":
                raise ToolError("패턴 missing가 없습니다")
            return f"page:{id}"

        port = _free_port()
        config = uvicorn.Config(
            server.streamable_http_app(), host="127.0.0.1", port=port, log_level="error"
        )
        running = uvicorn.Server(config)
        threading.Thread(target=running.run, daemon=True).start()
        started_by = time.monotonic() + 10
        while not running.started and time.monotonic() < started_by:
            time.sleep(0.05)
        assert running.started, "테스트용 MCP 서버가 뜨지 않았다"
        servers.append(running)
        return f"http://127.0.0.1:{port}/mcp", calls

    yield start
    for running in servers:
        running.should_exit = True


def test_get_index_calls_the_tool_without_arguments(start_recording_server):
    url, calls = start_recording_server(index_text="색인 본문")

    assert MonitoringWikiClient(url).get_index() == "색인 본문"
    assert calls == [{"tool": "get_index"}]


def test_get_index_returns_empty_string_for_empty_content(start_recording_server):
    url, _ = start_recording_server(index_text="")

    assert MonitoringWikiClient(url).get_index() == ""


def test_get_pattern_without_sections_sends_only_the_id(start_recording_server):
    url, calls = start_recording_server()

    assert MonitoringWikiClient(url).get_pattern("p") == "page:p"
    assert calls == [{"tool": "get_pattern", "id": "p", "sections": None}]


def test_get_pattern_with_sections_sends_them_as_a_list(start_recording_server):
    url, calls = start_recording_server()

    MonitoringWikiClient(url).get_pattern("p", ("원인", "해결방법"))

    assert calls == [{"tool": "get_pattern", "id": "p", "sections": ["원인", "해결방법"]}]


def test_get_pattern_omits_sections_key_when_empty(monkeypatch):
    sent: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        MonitoringWikiClient, "_call_tool", lambda self, name, args: sent.append((name, args)) or ""
    )

    MonitoringWikiClient("http://unused").get_pattern("p")
    MonitoringWikiClient("http://unused").get_pattern("p", ("원인",))

    assert sent == [
        ("get_pattern", {"id": "p"}),
        ("get_pattern", {"id": "p", "sections": ["원인"]}),
    ]


def test_get_pattern_raises_with_the_server_message_on_error(start_recording_server):
    url, _ = start_recording_server()

    with pytest.raises(RuntimeError, match="패턴 missing가 없습니다"):
        MonitoringWikiClient(url).get_pattern("missing")
