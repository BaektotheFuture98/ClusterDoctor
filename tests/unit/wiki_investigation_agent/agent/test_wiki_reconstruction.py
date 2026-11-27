"""위키 에이전트가 구성 문서를 미리 가져와 싣고, 재구성 답을 출처 검사 뒤에 남기는지 검증한다."""

import json
import logging

from test_wiki_toolbox import FakeClient

from cluster_doctor.wiki_investigation_agent.agent.findings import (
    ChainStep,
    Reconstruction,
    WikiFindings,
    WikiPattern,
)
from cluster_doctor.wiki_investigation_agent.agent.investigator import (
    INFRA_MAX_CHARS,
    WikiInvestigator,
    compose_message,
    parse_reconstructions,
)
from cluster_doctor.wiki_investigation_agent.agent.prompt import INFRA_SECTION_TITLE, WINDOWS_SECTION_TITLE

INFRA = "# infra\n| 티어 | 데이터 날짜 | 노드 그룹 |\n| WARM | 13개월 ~ 36개월 | RC97 |"


class InfraClient(FakeClient):
    def __init__(self, infra=INFRA, *, fail_infra=False, **kwargs):
        super().__init__(**kwargs)
        self.infra = infra
        self.fail_infra = fail_infra
        self.infra_calls = 0

    def get_infra(self):
        self.infra_calls += 1
        if self.fail_infra:
            raise RuntimeError("down")
        return self.infra


def _chain(*steps):
    return [{"step": s, "basis": b, "refs": list(r)} for s, b, r in steps]


def _answer(patterns=(), reconstruction=()):
    return json.dumps({"patterns": [{"id": i, "reason": ""} for i in patterns],
                       "reconstruction": list(reconstruction)}, ensure_ascii=False)


WARM_CHAIN = {
    "statement": "장기간 검색이 WARM(RC97)에 몰려 노드 이탈을 일으킨 것으로 보인다",
    "chain": _chain(("기간 검색 집중", "evidence", ["E-1"]), ("13개월 이전은 WARM", "infra", []),
                    ("RC97-04 이탈", "evidence", ["E-2"])),
    "wiki_ref": "heap-pressure",
}


def _investigate(client, answer, **inputs):
    seen = {}

    def run(toolbox, message):
        seen["message"] = message
        toolbox.get_index()
        toolbox.get_pattern("heap-pressure")
        return answer

    findings = WikiInvestigator(client=client, run_agent=run).investigate("{}", **inputs)
    return findings, seen["message"]


def test_infra_windows_and_base_date_are_sent_once_with_the_context():
    client = InfraClient()

    findings, message = _investigate(
        client, _answer(), windows='{"windows": []}', base_date="2026-11-17", evidence_ids=("E-1",))

    assert client.infra_calls == 1
    assert findings.infra == INFRA
    assert message.startswith("{}")
    assert "티어 기준일(사건 발생일, KST): 2026-11-17" in message
    assert f"{WINDOWS_SECTION_TITLE}\n\n" + '{"windows": []}' in message
    assert f"{INFRA_SECTION_TITLE}\n\n{INFRA}" in message


def test_infra_failure_is_logged_and_the_investigation_goes_on_without_it(caplog):
    with caplog.at_level(logging.WARNING):
        findings, message = _investigate(InfraClient(fail_infra=True), _answer(("heap-pressure",)))

    assert findings.infra == ""
    assert findings.known_ids == ("heap-pressure",)
    assert message == "{}"
    assert "구성 문서를 가져오지 못해" in caplog.text


def test_agent_failure_keeps_the_infra_document():
    def fail(toolbox, message):
        raise TimeoutError

    findings = WikiInvestigator(client=InfraClient(), run_agent=fail).investigate("{}")

    assert findings == WikiFindings(infra=INFRA)


def test_infra_is_cut_at_its_cap():
    findings, _ = _investigate(InfraClient(infra=("가" * 99 + "\n") * 100), _answer())

    assert 0 < len(findings.infra) <= INFRA_MAX_CHARS


def test_a_grounded_chain_is_kept_with_its_pattern():
    findings, _ = _investigate(
        InfraClient(), _answer(("heap-pressure",), [WARM_CHAIN]), evidence_ids=("E-1", "E-2"))

    assert findings.reconstructions == (Reconstruction(
        WARM_CHAIN["statement"],
        (ChainStep("기간 검색 집중", "evidence", ("E-1",)), ChainStep("13개월 이전은 WARM", "infra"),
         ChainStep("RC97-04 이탈", "evidence", ("E-2",))),
        "heap-pressure"),)


def test_unknown_evidence_ids_and_unchosen_patterns_are_dropped():
    item = {"statement": "s", "wiki_ref": "gc-overhead", "chain": _chain(
        ("a", "evidence", ["E-1", "E-404"]), ("b", "evidence", ["E-404"]), ("c", "wiki", ["gc-overhead"]))}

    (kept,) = parse_reconstructions(
        _answer(reconstruction=[item]), evidence_ids={"E-1"}, pattern_ids={"heap-pressure"}, infra_loaded=True)

    assert kept.chain == (ChainStep("a", "evidence", ("E-1",)),)
    assert kept.wiki_ref == ""


def test_a_chain_without_an_observed_link_is_dropped(caplog):
    no_evidence = {"statement": "구성만으로 세운 원인", "chain": _chain(("WARM", "infra", []))}
    unknown_only = {"statement": "모르는 근거", "chain": _chain(("a", "evidence", ["E-404"]))}

    with caplog.at_level(logging.WARNING):
        kept = parse_reconstructions(_answer(reconstruction=[no_evidence, unknown_only]),
                                     evidence_ids={"E-1"}, pattern_ids=set(), infra_loaded=True)

    assert kept == ()
    assert "재구성 2개를 버린다" in caplog.text


def test_infra_links_need_the_infra_document():
    (kept,) = parse_reconstructions(_answer(reconstruction=[WARM_CHAIN]),
                                    evidence_ids={"E-1", "E-2"}, pattern_ids=set(), infra_loaded=False)

    assert [step.basis for step in kept.chain] == ["evidence", "evidence"]


def test_reconstructions_are_capped_at_three_and_bad_answers_give_none():
    many = [WARM_CHAIN] * 5
    assert len(parse_reconstructions(_answer(reconstruction=many), evidence_ids={"E-1", "E-2"},
                                     pattern_ids=set(), infra_loaded=True)) == 3
    for text in ("not json", json.dumps({"reconstruction": "x"}), json.dumps([1])):
        assert parse_reconstructions(text, evidence_ids={"E-1"}, pattern_ids=set(), infra_loaded=True) == ()


def test_message_is_the_context_alone_without_extra_inputs():
    assert compose_message('{"a": 1}') == '{"a": 1}'


def test_prompt_block_puts_infra_then_reconstructions_then_pages():
    findings = WikiFindings(
        patterns=(WikiPattern("heap-pressure", "이유", "# heap-pressure\n증상"),),
        infra=INFRA,
        reconstructions=(Reconstruction("원인 후보", (
            ChainStep("집중", "evidence", ("E-1", "E-2")), ChainStep("WARM", "infra"),
            ChainStep("패턴 증상", "wiki", ("heap-pressure",))), "heap-pressure"),),
    )

    assert findings.as_prompt_block() == (
        f"[인프라 구성]\n{INFRA}\n\n"
        "[재구성 초안 1] 원인 후보 (패턴 heap-pressure)\n"
        "- 집중 [근거 E-1, E-2]\n- WARM [인프라 구성]\n- 패턴 증상 [위키 heap-pressure]\n\n"
        "[패턴 heap-pressure]\n# heap-pressure\n증상"
    )
