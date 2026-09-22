"""노드 호스트에 직접 붙어 ES 로그 파일을 읽는 포트.

노드 로그를 가져오는 경로는 둘이다. 마스터 노드 로그는 ClickHouse에 적재되어
``LogRepository.fetch_node_logs``로 오고, 데이터 노드 로그는 적재되는 곳이 없어
호스트에 직접 붙어야 한다.

**``LogRepository``에 합치지 않는 이유는 계약이 다르기 때문이다.** 저쪽은
구조화된 ``NodeLogEntry`` 목록을 돌려주고 시각 구간으로 질의하지만, 이쪽은 로그
파일 원문 문자열을 돌려주고 접속에 호스트 주소와 로그 경로가 필요하다. 한
인터페이스로 묶으면 두 구현 중 하나는 늘 쓰지 않는 인자를 받는다.

포트로 두는 이유는 수집 방식이 SSH에 묶여 있지 않아서다. 실행 호스트가 클러스터
내부 대역에 닿지 못하는 배치에서는 SSH가 구조적으로 실패하고, 그때 노드
에이전트나 로그 API로 갈아끼우려면 계약이 구현과 분리돼 있어야 한다.
"""

from abc import ABC, abstractmethod
from datetime import datetime

# 이 경로가 한 번에 돌려주는 기본 줄 수.
#
# ``clickhouse/port.py``의 ``DEFAULT_NODE_LOG_LIMIT``과 값이 같지만 뜻이 다르다. 저쪽은
# ClickHouse 질의의 행 수 상한이라 ``ORDER BY timestamp``로 **가장 이른** 행이
# 남고, 이쪽은 서버에서 ``tail``로 자르므로 **가장 늦은** 줄이 남는다. 같은
# 숫자라고 한 상수로 묶으면 그 반대 편향이 가려진다.
DEFAULT_HOST_LOG_LINES = 300


class NodeLogFetcher(ABC):
    """노드 호스트의 ES 로그 파일을 읽는다."""

    @abstractmethod
    def fetch(
        self,
        ip: str,
        log_path: str,
        cluster_name: str,
        start_dt: datetime,
        end_dt: datetime,
        keyword: str = "",
        max_lines: int = DEFAULT_HOST_LOG_LINES,
    ) -> str:
        """``start_dt ~ end_dt`` 구간의 로그 줄을 원문 그대로 돌려준다.

        ``ip``·``log_path``·``cluster_name``은 ``ClusterRepository.node_info``가
        돌려주는 값이다. 호출자가 노드 이름을 주소로 푸는 책임을 지고, 이 포트는
        그 주소에 어떻게 닿을지만 안다.

        **예외를 삼키지 않는다.** 접속 실패와 "그 시각에 로그가 없다"는 다르고,
        둘을 같은 빈 문자열로 뭉개면 리포트에서 "못 봤다"와 "봤는데 없다"가
        구별되지 않는다. 호출하는 tool이 예외를 받아 ``gaps``로 남긴다.

        Args:
            ip:           접속할 호스트 주소.
            log_path:     로그 디렉터리. 파일명은 ``{log_path}/{cluster_name}.log``.
            cluster_name: 로그 파일명을 구성하는 클러스터 이름.
            start_dt:     구간 시작. timezone-aware여야 한다.
            end_dt:       구간 끝. timezone-aware여야 한다.
            keyword:      주면 그 문자열을 포함한 줄만 남긴다.
            max_lines:    돌려줄 최대 줄 수. 넘으면 **늦은** 줄이 남는다.

        Returns:
            줄바꿈으로 이어 붙인 로그 원문. 해당 구간에 줄이 없으면 빈 문자열.
        """
        ...
