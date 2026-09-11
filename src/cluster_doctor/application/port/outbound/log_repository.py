from abc import ABC, abstractmethod
from datetime import datetime

from cluster_doctor.domain.model.log_entry import LogEntry, NodeLogEntry
from cluster_doctor.domain.model.time_range import TimeRange

# 노드 로그 한 번 조회로 돌려줄 기본/최대 줄 수. 프롬프트에 그대로 실리므로
# 상한을 둔다 — WARN 폭주 구간은 1분에 수천 줄이 쌓인다.
DEFAULT_NODE_LOG_LIMIT = 300
MAX_NODE_LOG_LIMIT = 2000


def clamp_node_log_limit(limit: int) -> int:
    """LLM이 준 줄 수 요청을 실제 적용값으로 바꾼다.

    호출부와 구현부가 **같은 값을 알아야** 한다. 어댑터만 조여 놓으면 tool은
    "몇 줄이 잘렸는지"를 모른 채 원래 요청값과 비교하게 되고,
    ``max_lines=5000``을 받아 2000건이 돌아왔을 때 ``2000 >= 5000``이 거짓이라
    절단 사실을 알리지 못한다 — 모델은 창 전체를 본 줄로 알고 추론한다.
    반대로 ``0``은 1로 조여지는데 그것을 모르면 "상한 0줄에 걸려"라고 쓴다.
    """
    return max(1, min(int(limit), MAX_NODE_LOG_LIMIT))


class LogRepository(ABC):
    @abstractmethod
    def fetch_logs(self, time_range: TimeRange) -> list[LogEntry]:
        """분 단위 분석에 쓰는 세 소스(slowlog·쿼리 로그·노드 메트릭)를 한 리스트로."""
        ...

    @abstractmethod
    def fetch_node_logs(
        self,
        start: datetime,
        end: datetime,
        *,
        node: str = "",
        node_role: str = "",
        levels: tuple[str, ...] = (),
        loggers: tuple[str, ...] = (),
        keyword: str = "",
        limit: int = DEFAULT_NODE_LOG_LIMIT,
    ) -> list[NodeLogEntry]:
        """ES 노드 로그를 조건으로 걸러 시간순으로 돌려준다.

        ``TimeRange``를 받지 않는 것은 의도적이다. 그 타입은 10분 상한을
        강제하는데, 그 상한은 "소스당 1분마다 쿼리 하나 + 분마다 LLM 호출
        하나"라는 분석 파이프라인의 팬아웃 비용에서 온 것이다. 이 조회는
        단일 쿼리이고 비용이 ``limit``으로 이미 묶여 있어 같은 상한을 물려받을
        이유가 없다 — 사고 전체 구간을 한 번에 보는 것이 이 조회의 용도다.

        빈 문자열·빈 튜플은 "그 조건으로 걸러내지 않는다"는 뜻이다.

        ``levels``와 ``loggers``만 서로 **OR**로 묶이고, 나머지 조건은 AND다.
        레벨이 높으면 로거와 무관하게 받고, INFO라도 지정한 로거면 받는다는
        뜻이다. 이렇게 하지 않으면 둘 중 하나를 포기해야 한다 —
        실측(``packetbeat.loki_logs``)에서 샤드 할당을 남기는
        ``o.e.c.r.a.AllocationService``는 INFO였고, 같은 INFO 안에 ML 유지보수·
        만료 데이터 삭제처럼 진단과 무관한 줄이 90%를 차지했다. 레벨만 쓰면
        전자를 잃고, 레벨을 열면 후자가 프롬프트를 채운다.

        Args:
            start:     조회 시작(포함). timezone-aware여야 한다.
            end:       조회 종료(제외). timezone-aware여야 한다.
            node:      노드 이름 정확히 일치.
            node_role: 노드 역할 정확히 일치. 마스터 로그는 여기에 "master".
            levels:    로그 레벨. ``("WARN", "ERROR")`` 형태. 대소문자 무시.
            loggers:   ES 로거 이름. 축약형이다. ``("o.e.c.c.Coordinator",)``.
                       대소문자를 구분한다 — 클래스 이름이기 때문이다.
            keyword:   ``line`` 부분 일치. 대소문자 무시.
            limit:     최대 줄 수. ``MAX_NODE_LOG_LIMIT``으로 잘린다.
        """
        ...
