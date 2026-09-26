"""Public composition API for the DeepAgents incident-analyzer adapter."""

from cluster_doctor.adapters.outbound.deepagents.adapter import (
    DeepAgentsConfig,
    build_deepagents_incident_analyzer,
)

__all__ = ["DeepAgentsConfig", "build_deepagents_incident_analyzer"]
