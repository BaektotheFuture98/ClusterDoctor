"""프로세스 시작/종료 시 정리해야 하는 자원.

composition 자체(``dependency/wiring.py``)와 나누는 이유는 "무엇을 조립하는가"와
"언제 놓아주는가"가 다른 관심사이기 때문이다.
"""

from cluster_doctor.bootstrap.dependency.wiring import get_clickhouse_client


def close_clickhouse_client() -> None:
    if get_clickhouse_client.cache_info().currsize == 0:
        return
    get_clickhouse_client().close()
