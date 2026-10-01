import re
import gc
import weakref
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import (
    PSEUDONYMS,
    Pseudonymizer,
    pseudonym_scope,
)


def test_same_ip_gets_same_alias_even_before_a_sentence_ending_period():
    p = Pseudonymizer()
    masked = p.mask('host "10.0.1.5" and node 10.0.1.5. Also 10.0.1.5, 10.0.1.5:9200')
    assert "10.0.1" not in masked
    assert len(set(re.findall(r"ip-\d{4}", masked))) == 1


def test_not_an_ip_is_left_alone():
    p = Pseudonymizer()
    assert p.mask("v1.2.3.4.5 and 999.1.1.1 and 1.2.3") == "v1.2.3.4.5 and 999.1.1.1 and 1.2.3"


def test_registered_values_and_email_round_trip():
    p = Pseudonymizer()
    p.register("user", "hong_gildong")
    p.register("company", "ab")  # too short to register
    text = "user=hong_gildong ab mail a.b@corp.co.kr ip 10.0.1.5."
    masked = p.mask(text)
    assert "hong_gildong" not in masked and "corp.co.kr" not in masked
    assert "10.0.1.5" not in masked and " ab " in masked
    assert p.restore(masked) == text
    assert p.restore("ip-9999") == "ip-9999"


@pytest.mark.parametrize("fail", [False, True])
def test_scope_releases_mapping_after_exit(fail):
    def run():
        try:
            with pseudonym_scope(fresh=True) as mapping:
                ref = weakref.ref(mapping)
                PSEUDONYMS.register("user", "first-user")
                assert PSEUDONYMS.mask("first-user") == "user-0001"
                if fail:
                    raise ValueError("failed call")
        except ValueError:
            pass
        return ref

    ref = run()
    gc.collect()
    assert ref() is None
    with pseudonym_scope():
        assert PSEUDONYMS.restore("user-0001") == "user-0001"
        assert PSEUDONYMS.mask("first-user") == "first-user"


def test_nested_analysis_reuses_mapping_and_fresh_scope_restores_parent():
    with pseudonym_scope(fresh=True):
        PSEUDONYMS.register("user", "parent-user")
        with pseudonym_scope():
            assert PSEUDONYMS.mask("parent-user") == "user-0001"
            PSEUDONYMS.register("user", "child-user")
        assert PSEUDONYMS.restore("user-0002") == "child-user"
        with pseudonym_scope(fresh=True):
            assert PSEUDONYMS.restore("user-0001") == "user-0001"
        assert PSEUDONYMS.restore("user-0001") == "parent-user"


def test_concurrent_incidents_keep_independent_mapping():
    gate = Barrier(2)

    def run(user):
        with pseudonym_scope(fresh=True):
            PSEUDONYMS.register("user", user)
            gate.wait(timeout=2)
            return PSEUDONYMS.mask(user), PSEUDONYMS.restore("user-0001")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(run, "first-user")
        second = executor.submit(run, "second-user")
        assert first.result() == ("user-0001", "first-user")
        assert second.result() == ("user-0001", "second-user")


def test_standalone_llm_calls_restore_with_temporary_mapping(monkeypatch):
    from cluster_doctor.incident_analysis_agent.agent.runtime import litellm_client

    sent = []

    def completion(**kwargs):
        content = kwargs["messages"][0]["content"]
        sent.append(content)
        return SimpleNamespace(
            choices=[SimpleNamespace(
                message=SimpleNamespace(content=content), finish_reason="stop"
            )]
        )

    monkeypatch.setattr(litellm_client.litellm, "completion", completion)
    for ip in ("10.0.1.5", "10.0.1.6"):
        result = litellm_client.complete(
            [{"role": "user", "content": ip}], "nvidia_nim", "test-model", "test-key"
        )
        assert result == ip
    assert sent == ["ip-0001", "ip-0001"]
