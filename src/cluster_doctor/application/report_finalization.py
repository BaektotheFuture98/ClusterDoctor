"""Application service for persisting an incident-wide merged report."""

from __future__ import annotations

from cluster_doctor.application.ports.artifact_store import ArtifactStore
from cluster_doctor.domain.incident.report_merge import merge_window_reports


def finalize_incident_report(
    incident_id: str,
    report_refs: list[str],
    store: ArtifactStore,
) -> str | None:
    """Load window reports, apply the domain merge policy, and persist the result."""
    reports = [
        report
        for ref in report_refs
        if (report := store.get_report(ref)) is not None
    ]
    if not reports:
        return None
    return store.put_report(incident_id, merge_window_reports(reports))
