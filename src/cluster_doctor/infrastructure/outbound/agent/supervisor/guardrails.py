"""Main Agent 런타임 정책.

모델은 프롬프트의 지시를 어길 수 있고, deepagents의 ``recursion_limit``은
9,999라 프레임워크도 폭주를 막아 주지 않는다. 그래서 상한을 코드가 강제한다.

여기 있는 것은 **agent 실행 정책**뿐이다. 마이크로 배치·재트리거처럼
use case 수준의 정책은 ``application`` 계층이 갖는다. 분석 범위를 정하는
값(lookback, 유입 정착 판정)은 정책이 아니라 분석 방법이라 tool에 남는다.
"""

from cluster_doctor.domain.model.time_range import MAX_TIME_RANGE_DURATION

# 분석 창 상한. 도메인의 TimeRange가 같은 제약을 강제하므로 값을 두 번 쓰지
# 않는다 — 두 곳에 박아 두면 한쪽만 고쳤을 때 tool은 통과시키고 TimeRange가
# 거부하며 서로 다른 오류 메시지를 낸다.
#
# 그래도 tool 쪽 검사를 남기는 이유는 반환 형태가 다르기 때문이다. tool은
# 모델이 읽고 스스로 고칠 수 있는 안내 문장을 돌려주고, 도메인은 어떤
# 호출자에게든 예외를 던진다.
MAX_WINDOW_MINUTES = int(MAX_TIME_RANGE_DURATION.total_seconds() // 60)

# 유입 대기 예산. agent는 slowlog 유입이 멎을 때까지 sleep으로 기다리는데,
# 그동안 분석은 시작조차 되지 않고 큐만 쌓인다.
#
# 1회 상한을 60초로 둔 이유: 대기를 여러 번으로 쪼개야 매 사이클마다
# check_new_slowlogs로 유입 여부를 다시 볼 수 있다. 한 번에 5분을 자면
# 그 사이 유입이 멎어도 알아채지 못한다.
MAX_SLEEP_SECONDS = 60
MAX_WAIT_SECONDS = 300

# 한 번의 진단에서 analyze_logs를 부를 수 있는 횟수. 호출 하나가 구간의
# 분 수만큼 LLM을 부르므로(5분 창 실측 513,122 토큰) 가장 비싼 도구다.
# 10분 창을 10분 이하로 쪼개 부르는 경우와 허용된 재시도 1회를 합쳐도
# 6회면 넉넉하다.
MAX_ANALYZE_CALLS = 6

# 구조화 출력 검증 실패를 되돌려 보는 횟수. langchain 기본값은 무제한이라
# 실패할 때마다 오류 ToolMessage를 붙여 다시 시킨다. 이 저장소는 429를
# 최우선 제약으로 다뤄 재시도를 0으로 두는 곳이므로 그 결정을 스키마가
# 우회하게 둘 수 없다.
STRUCTURED_OUTPUT_RETRY_LIMIT = 2

# LLM 재시도 횟수. 이 경로의 실패는 대부분 429이고 원인은 분당 입력 토큰
# 한도 초과다. 같은 프롬프트를 다시 보내면 실패가 보장된 채 소비만 배로
# 는다(실측 513,122 토큰 -> 재시도 포함 2,052,488 토큰, 한도 250,000의 821%).
# 오케스트레이터는 tool 루프라 재시도가 루프 전체로 증폭된다.
LLM_MAX_RETRIES = 0
