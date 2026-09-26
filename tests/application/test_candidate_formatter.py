from datetime import UTC, datetime

from cluster_doctor.domain.diagnosis.observations import SlowCandidate


def test_candidate_formatter_keeps_the_shared_candidate_representation():
    from cluster_doctor.application.candidate_formatter import candidate_line

    candidate = SlowCandidate(
        candidate_id="C1",
        source="slowlog",
        timestamp=datetime(2026, 9, 26, 10, 0, tzinfo=UTC),
        took="2s",
        index_name="orders",
    )

    assert candidate_line(candidate) == "[C1] slowlog 19:00:00 took=2s index=orders"
