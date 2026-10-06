import pytest

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import Pseudonymizer


def _registered():
    mapping = Pseudonymizer()
    mapping.register("user", "logstash")
    mapping.register("company", "제품안전관리원")
    return mapping


@pytest.mark.parametrize(
    "text, hidden",
    [
        ("logstash가 bulk를 보냈다", "logstash"),
        ("logstash의 요청", "logstash"),
        ("제품안전관리원이 조회했다", "제품안전관리원"),
        ("(logstash)", "logstash"),
        ("user=logstash", "logstash"),
        ("회사는 제품안전관리원.", "제품안전관리원"),
    ],
)
def test_registered_values_are_masked_next_to_korean_particles(text, hidden):
    assert hidden not in _registered().mask(text)


@pytest.mark.parametrize("text", ["mylogstash", "logstash2", "logstash_old", "xlogstash가"])
def test_registered_values_inside_other_ascii_words_are_left_alone(text):
    assert _registered().mask(text) == text


@pytest.mark.parametrize(
    "suffix", ["에서 발생", "으로부터 유입", "의 로그", "를 확인", " 에서", ".", ", 다음", ")"]
)
def test_pseudonym_is_restored_before_korean_particles_and_punctuation(suffix):
    mapping = Pseudonymizer()
    alias = mapping.mask("192.168.1.40")

    assert mapping.restore(f"요청 호스트 {alias}{suffix}") == f"요청 호스트 192.168.1.40{suffix}"


def test_pseudonym_is_restored_after_korean_text():
    mapping = Pseudonymizer()
    alias = mapping.mask("192.168.1.40")

    assert mapping.restore(f"호스트{alias}") == "호스트192.168.1.40"


@pytest.mark.parametrize("text", ["skip-0001", "xip-0001", "ip-00011", "ip-0001abc", "ip-0001_old"])
def test_text_that_only_looks_like_a_pseudonym_is_not_restored(text):
    mapping = Pseudonymizer()
    mapping.mask("192.168.1.40")

    assert mapping.restore(text) == text


def test_mask_then_restore_round_trips_through_korean_sentences():
    mapping = _registered()
    original = (
        "logstash가 192.168.1.40에서 제품안전관리원의 인덱스로 bulk를 보냈고 "
        "btkim2@newen.ai가 확인했다."
    )

    masked = mapping.mask(original)

    assert "logstash" not in masked
    assert "192.168.1.40" not in masked
    assert "제품안전관리원" not in masked
    assert "btkim2@newen.ai" not in masked
    assert mapping.restore(masked) == original
