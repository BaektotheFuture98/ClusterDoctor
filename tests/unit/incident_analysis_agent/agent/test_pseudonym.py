import re

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import (
    Pseudonymizer,
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
