from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import (
    PSEUDONYMS,
    Pseudonymizer,
    pseudonym_scope,
)


def test_given_mapping_is_reused_across_scopes():
    shared = Pseudonymizer()
    with pseudonym_scope(fresh=True, mapping=shared):
        PSEUDONYMS.register("company", "acme-corp")
        first = PSEUDONYMS.mask("acme-corp 요청")
    with pseudonym_scope(fresh=True, mapping=shared):
        second = PSEUDONYMS.mask("acme-corp 요청")
    assert first == second == "company-0001 요청"


def test_fresh_scope_without_mapping_forgets_names():
    with pseudonym_scope(fresh=True):
        PSEUDONYMS.register("company", "acme-corp")
    with pseudonym_scope(fresh=True):
        assert PSEUDONYMS.mask("acme-corp 요청") == "acme-corp 요청"
