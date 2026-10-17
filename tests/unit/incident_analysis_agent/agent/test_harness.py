from unittest.mock import patch

import pytest
from deepagents import GeneralPurposeSubagentProfile, HarnessProfile
from deepagents.profiles.harness import harness_profiles
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from cluster_doctor.incident_analysis_agent.agent.runtime import harness


class PolicyModel(FakeListChatModel):
    model_name: str = "cleanup-policy-test"


@pytest.fixture
def model(monkeypatch):
    model = PolicyModel(responses=["done"])
    provider = harness.get_model_provider(model)
    key = f"{provider}:{model.model_name}"
    harness_profiles._get_harness_profile(key)
    monkeypatch.setattr(harness_profiles, "_HARNESS_PROFILES", {})
    return model, key


def test_repeated_restriction_keeps_policy_without_registering_again(model):
    model, _ = model
    with patch.object(
        harness, "register_harness_profile", wraps=harness.register_harness_profile
    ) as register:
        harness.restrict_harness(model)
        first = harness_profiles._harness_profile_for_model(model, None)
        harness.restrict_harness(model)
    assert register.call_count == 1
    assert first.general_purpose_subagent.enabled is False
    assert {
        "ls", "read_file", "write_file", "edit_file", "delete", "glob", "grep", "execute"
    } <= first.excluded_tools
    assert harness_profiles._harness_profile_for_model(model, None) == first


def test_restriction_restores_policy_if_delegation_was_reenabled(model):
    model, key = model
    harness.restrict_harness(model)
    harness.register_harness_profile(
        key,
        HarnessProfile(general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=True)),
    )
    harness.restrict_harness(model)
    profile = harness_profiles._harness_profile_for_model(model, None)
    assert profile.general_purpose_subagent.enabled is False


def test_provider_defaults_do_not_hide_a_model_specific_override(model):
    model, _ = model
    model.model_name = "other:harness-policy-test"
    harness.register_harness_profile(
        harness.get_model_provider(model),
        HarnessProfile(
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
            excluded_tools=frozenset(
                {"ls", "read_file", "write_file", "edit_file", "delete", "glob", "grep", "execute"}
            ),
        ),
    )
    harness.register_harness_profile(
        model.model_name,
        HarnessProfile(general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=True)),
    )
    harness.restrict_harness(model)
    profile = harness_profiles._harness_profile_for_model(model, None)
    assert profile.general_purpose_subagent.enabled is False
    assert "execute" in profile.excluded_tools


def test_failed_registration_can_be_retried(model):
    model, _ = model
    register = harness.register_harness_profile
    with patch.object(
        harness, "register_harness_profile", side_effect=RuntimeError("registration failed")
    ):
        with pytest.raises(RuntimeError, match="registration failed"):
            harness.restrict_harness(model)
    with patch.object(harness, "register_harness_profile", wraps=register) as retry:
        harness.restrict_harness(model)
    retry.assert_called_once()
    profile = harness_profiles._harness_profile_for_model(model, None)
    assert profile.general_purpose_subagent.enabled is False
