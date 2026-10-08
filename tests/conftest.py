import pytest

from cluster_doctor.bootstrap.configuration.settings import Settings


@pytest.fixture(autouse=True)
def _isolate_settings_from_local_environment(monkeypatch):
    """로컬 .env나 셸에서 새어 든 Settings 값이 테스트의 기본값 전제를 깨지 않게 한다.

    ``Settings(_env_file=None)``은 .env 파일만 막는다. 환경변수는 그대로 읽으므로,
    개발 PC의 값이 프로세스 환경에 올라와 있으면 "기본은 꺼짐" 같은 전제가 깨진다.
    """
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)
