"""테스트 전역 격리.

``litellm/__init__.py``가 import 시점에 ``load_dotenv()``를 호출해 ``.env``의
모든 키를 ``os.environ``에 주입한다. ``litellm_client`` → ``deepagent.analyzer``
→ ``test_dependencies``의 import 사슬을 타므로, pytest가 수집 단계에서 모든 테스트
모듈을 import하는 순간 주입이 끝나 있다.

그 결과 ``Settings(_env_file=None)``으로도 격리되지 않는다. dotenv 파일 읽기는
껐지만 os.environ은 이미 오염돼 있기 때문이다. 실제로 ``test_settings.py``는
단독 실행하면 통과하고 전체 실행하면 실패했다(수집 순서상 test_dependencies가
먼저 import된다) — 즉 결과가 실행 방식에 따라 달라졌다.

키를 개별 테스트에서 지우는 방식은 이 취약성을 그대로 남긴다. 여기서 ``.env``에
있는 키를 매 테스트 전에 걷어내 개발자의 ``.env`` 내용과 무관하게 만든다.

autouse 픽스처는 테스트 본문보다 먼저 실행되므로, 각 테스트가 직접 하는
``monkeypatch.setenv``는 그대로 우선한다.

주의: 이 픽스처는 os.environ 오염만 막는다. ``Settings``의 ``env_file=".env"``
설정 때문에 ``get_settings()``/``Settings()``는 여전히 ``.env`` *파일*을 직접
읽는다. 그 경로가 걸리는 테스트는 환경 변수 격리에 기대지 말고 대상 함수를
직접 patch해야 한다.
"""

from pathlib import Path

import pytest
from dotenv import dotenv_values

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
# import 시점에 한 번만 읽는다. 파일이 없으면 빈 dict이므로 CI에서도 안전하다.
_DOTENV_KEYS: tuple[str, ...] = tuple(dotenv_values(_ENV_FILE).keys())


@pytest.fixture(autouse=True)
def _isolate_dotenv_env(monkeypatch):
    for key in _DOTENV_KEYS:
        monkeypatch.delenv(key, raising=False)
