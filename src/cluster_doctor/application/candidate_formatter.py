"""Pure formatting shared by diagnosis and report adapters."""

from cluster_doctor.domain.diagnosis.kst import KST
from cluster_doctor.domain.diagnosis.observations import SlowCandidate


def candidate_line(candidate: SlowCandidate) -> str:
    parts = [
        f"[{candidate.candidate_id}]", candidate.source,
        candidate.timestamp.astimezone(KST).strftime("%H:%M:%S"),
    ]
    if candidate.took:
        parts.append(f"took={candidate.took}")
    if candidate.run_time is not None:
        parts.append(f"runtime={candidate.run_time}s")
    if candidate.total_hits:
        parts.append(f"hits={candidate.total_hits}")
    if candidate.total_shards:
        parts.append(f"shards={candidate.total_shards}")
    if candidate.index_name:
        parts.append(f"index={candidate.index_name}")
    if candidate.node:
        parts.append(f"node={candidate.node}")
    if candidate.cmd:
        parts.append(f"cmd={candidate.cmd}")
    if candidate.company:
        parts.append(f"company={candidate.company}")
    if candidate.user:
        parts.append(f"user={candidate.user}")
    return " ".join(parts)
