"""Main DeepAgent가 쓰는 chat model.

``litellm_client.complete()``는 **왕복 한 번에 텍스트 하나**를 돌려주는
함수다. tool loop를 돌 수 없으므로 DeepAgent는 그것을 쓸 수 없고,
LangChain ``BaseChatModel``이 필요하다.

그래서 이 모듈이 있다. 경로가 갈라지면 ``litellm_client``가 지키던 보호
장치가 새 경로에서만 사라지는 사고가 난다 — 아래 세 가지는 그쪽과 **같은
값**이어야 하고, 그래서 표를 옮겨 적지 않고 import해서 쓴다:

1. 재시도 0회 (429 증폭 방지)
2. 요청 타임아웃 120초
3. temperature를 비롯한 샘플링 인자를 보내지 않는다

여기에 하나가 더 붙는다. Main DeepAgent는 tool call로만 움직이므로, tool
call을 낼 수 없는 모델이 걸리면 그 사실을 첫 호출 전에 알아야 한다.
"""

from __future__ import annotations

import logging
import os

# litellm_client와 같은 이유로 import 순서를 고정한다. litellm이 import
# 시점에 이 값을 읽으므로 나중에 넣으면 효과가 없고, 없으면 import 때
# GitHub에서 비용 데이터를 받아온다. 아래 import가 E402인 것은 의도다.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

import litellm  # noqa: E402
from langchain_core.language_models import BaseChatModel  # noqa: E402
from langchain_litellm import ChatLiteLLM  # noqa: E402

from cluster_doctor.config.settings import (  # noqa: E402
    ConfigurationError,
)

# ``_PROVIDER_PREFIX``와 ``_REQUEST_TIMEOUT_SECONDS``는 밑줄로 시작하지만
# 일부러 가져다 쓴다. 이 둘은 "litellm에 어떤 문자열로 어떤 조건으로
# 부르는가"이고, 그것이 두 모듈에서 달라지면 provider를 하나 추가할 때
# 한쪽만 고쳐진다. 복제본을 만드는 것보다 사유를 적고 참조하는 쪽이 낫다.
from cluster_doctor.agent.common.litellm_client import (  # noqa: E402
    _PROVIDER_PREFIX,
    _REQUEST_TIMEOUT_SECONDS,
    require_supported_provider,
)

_logger = logging.getLogger(__name__)

# 운영자에게 알려 줄 **설정 이름**. 값은 넣지 않는다 — 이 경로가 함께 다루는
# 값 중 하나가 API 키이고, "지금 값은 이것"을 찍는 습관이 키를 찍는 사고로
# 이어진다. 이름만으로 고칠 자리는 충분히 특정된다.
_SETTING_HINT = "settings의 llm_provider와 모델 설정(gemini_model / nvidia_model)"

# 로그 메시지는 ASCII로 쓴다. root StreamHandler가 sys.stderr에 쓰고 Python이
# OS 로케일로 인코딩하므로(한국어 Windows에서 cp949) 한국어를 쓰면 UTF-8
# 수집기에서 깨진다. litellm_client._log_provider_error와 같은 이유다.
_UNVERIFIED_WARNING = (
    "LLM tool-call support could not be verified: litellm has no registry "
    "entry for the configured provider/model, so its capability is unknown. "
    "The main DeepAgent delegates only through tool calls and will not be "
    "able to work if the model cannot emit them. "
    "Settings to check: llm_provider, gemini_model, nvidia_model."
)


def require_tool_calling(provider: str, model: str) -> None:
    """tool call을 낼 수 없다고 **확인된** 조합이면 기동을 막는다.

    Main DeepAgent는 tool call 말고는 움직일 방법이 없다. 구간 제안도 위임도
    tool이므로, tool call을 못 내는 모델이 걸리면 Agent는 아무것도 하지 못한
    채 텍스트만 뱉는다. 그 사실을 첫 Incident를 태운 뒤에 아는 것보다 조립
    시점에 아는 쪽이 싸다.

    판정은 두 갈래이고, 갈래를 나누는 것이 이 함수의 요점이다.

    - litellm 레지스트리(``model_cost``)에 항목이 **있고** 그 항목이 function
      calling을 지원하지 않는다고 말하면, 그것은 실제 부정이므로 끊는다.
    - 항목이 **없으면** 그것은 부정이 아니라 침묵이다. litellm의 nvidia_nim
      항목은 reranker 셋뿐이라 우리가 쓰는 모델은 애초에 등재되어 있지 않다.
      침묵을 부정으로 읽으면 근거 없이 provider 하나가 통째로 쓸 수 없게
      되므로, 경고만 남기고 통과시킨다.

    실패 메시지에는 고쳐야 할 설정의 **이름**만 담는다(``_SETTING_HINT``).
    """
    provider = require_supported_provider(provider)
    model_id = f"{_PROVIDER_PREFIX[provider]}/{model}"

    if model_id not in litellm.model_cost:
        _logger.warning(_UNVERIFIED_WARNING)
        return

    # 항목은 있는데 지원 표시가 없는 경우도 여기서 부정으로 읽힌다. litellm
    # 자신이 그렇게 읽으므로 판단 기준을 따로 두지 않는다 — 등재된 모델의
    # 항목은 이 값을 갖고 있는 것이 정상이다.
    if not litellm.supports_function_calling(model=model_id):
        raise ConfigurationError(
            "선택된 LLM 모델은 tool call을 지원하지 않습니다. Main DeepAgent는 "
            "tool call로만 분석을 위임하므로 이 모델로는 동작할 수 없습니다. "
            f"{_SETTING_HINT}을 tool calling이 가능한 조합으로 바꾸세요."
        )


def build_chat_model(*, provider: str, model: str, api_key: str) -> BaseChatModel:
    """DeepAgent가 tool loop를 돌릴 수 있는 chat model을 만든다.

    ``ChatLiteLLM``을 쓰는 이유는 provider 선택이 이미 litellm 문자열 규약
    위에 서 있기 때문이다. 같은 ``prefix/model`` 문자열을 쓰므로 provider를
    늘릴 때 고칠 자리가 ``_PROVIDER_PREFIX`` 하나로 남는다.
    """
    require_tool_calling(provider, model)

    return ChatLiteLLM(
        model=f"{_PROVIDER_PREFIX[provider]}/{model}",
        # 키는 파라미터로만 전달한다. 모델 문자열이나 URL에 싣지 않는다.
        api_key=api_key,
        # LangChain 계층의 재시도. 기본값이 1이라 **끄지 않으면 켜져 있다.**
        # 이 경로의 실패는 대부분 429이고 원인은 분당 입력 토큰 한도 초과라,
        # 같은 프롬프트를 다시 보내면 실패가 보장된 채 소비만 배로 늘어난다
        # (실측: 513,122 토큰 → 재시도 포함 2,052,488 토큰, 한도 250,000의
        # 821%). tenacity stop_after_attempt(0)은 첫 호출은 그대로 하고 재시도
        # 없이 원래 예외를 그대로 올린다.
        max_retries=0,
        request_timeout=_REQUEST_TIMEOUT_SECONDS,
        # litellm 계층의 재시도. ``max_retries``와 별개의 층이고 이쪽은
        # ChatLiteLLM이 건드리지 않으므로 직접 못 박는다. 한 층만 끄면 나머지
        # 한 층이 같은 증폭을 그대로 일으킨다. ``model_kwargs``는 그대로
        # ``litellm.completion(**kwargs)``로 흘러간다.
        model_kwargs={"num_retries": 0},
        # temperature/top_p/top_k/n은 건드리지 않는다. ChatLiteLLM의 기본값이
        # None이고, litellm은 None을 보내지 않은 것과 같게 취급하므로
        # provider 기본값이 그대로 쓰인다 — 모델을 바꾸면 그 모델에 맞는
        # 기본값을 따라가는 것이 우리가 원하는 동작이다.
    )
