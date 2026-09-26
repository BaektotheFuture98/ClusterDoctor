"""노드 로그 수집이 포트를 지나는지 고정한다.

``outbound/`` 어댑터는 저마다 포트 하나를 구현하고, 소비자는 구현 타입을 모른다.
이 규칙이 깨지면 조립을 바꾸지 않고는 수집 방식을 갈아끼울 수 없다.
"""

from __future__ import annotations

import inspect

from cluster_doctor.application.ports.node_log_fetcher import (
    DEFAULT_HOST_LOG_LINES,
    NodeLogFetcher,
)
from cluster_doctor.agent.integrations.ssh.fetcher import (
    SshNodeLogFetcher,
)


def test_ssh_어댑터가_포트를_구현한다():
    assert issubclass(SshNodeLogFetcher, NodeLogFetcher)


def test_포트는_직접_만들_수_없다():
    # 추상 메서드를 빠뜨린 구현이 조용히 조립되면, 실패는 진단 도중에야 난다.
    try:
        NodeLogFetcher()
    except TypeError:
        return
    raise AssertionError("추상 포트가 인스턴스화됐다")


def test_구현이_포트의_시그니처를_그대로_따른다():
    """인자 이름과 기본값까지 맞춘다.

    호출부가 키워드 인자로 부르기 때문이다(``fetch(ip, log_path, cluster_name,
    start_dt=..., end_dt=...)``). 이름이 어긋나면 타입 검사에는 걸리지 않고
    호출 시점에 TypeError가 나는데, 그 자리는 tool 안이라 예외가 새면 agent
    실행 전체가 죽는다.
    """
    port = inspect.signature(NodeLogFetcher.fetch)
    impl = inspect.signature(SshNodeLogFetcher.fetch)

    assert list(port.parameters) == list(impl.parameters)
    for name, expected in port.parameters.items():
        assert impl.parameters[name].default == expected.default, name


def test_기본_줄_수가_한_곳에서만_정의된다():
    # 두 곳에 각각 박아 두면 한쪽만 바뀌어도 아무도 모른다.
    assert (
        NodeLogFetcher.fetch.__defaults__[-1]
        == SshNodeLogFetcher.fetch.__defaults__[-1]
        == DEFAULT_HOST_LOG_LINES
    )


def test_분석기가_구현이_아니라_포트를_요구한다():
    """조립을 바꾸지 않고 수집 방식을 갈아끼울 수 있어야 한다.

    실행 호스트가 클러스터 내부 대역에 닿지 못하는 배치에서는 SSH가 구조적으로
    실패하므로, 다른 어댑터가 필요해질 수 있다.
    """
    from cluster_doctor.agent.diagnosis.state import (
        DiagnosisSeams,
    )

    annotation = inspect.signature(
        DiagnosisSeams.__init__
    ).parameters["node_log_fetcher"].annotation

    assert annotation is NodeLogFetcher or annotation == "NodeLogFetcher"


def test_포트가_아닌_구현은_주입되지_않는다():
    """포트를 상속하지 않은 오리 타입은 받지 않는다는 뜻은 아니다.

    파이썬은 구조적 타이핑이라 MagicMock도 들어간다(테스트가 그렇게 쓴다).
    여기서 고정하는 것은 **배선이 무엇을 만드는가**다 — 조립이 구현을 고르고,
    소비자는 그 선택을 모른다.
    """
    from cluster_doctor.bootstrap import dependencies

    source = inspect.getsource(dependencies.build_trigger_service)
    assert "SshNodeLogFetcher(" in source
