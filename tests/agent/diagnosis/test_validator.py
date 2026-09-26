"""Evidence와 Draft Report의 대조.

검증에 필요한 것은 판단이 아니라 대조다. 모델에게 "이 리포트가 근거와
맞습니까"를 물으면 같은 모델이 자기 출력을 채점하게 되고, 그 채점은 거의
통과한다. 그래서 규칙은 전부 구조화된 필드에서 나온다.
"""

from datetime import datetime, timedelta

from cluster_doctor.domain.diagnosis.evidence import Evidence, EvidenceSource
from cluster_doctor.domain.diagnosis.report import (
    LogAnalysisReport,
    ReportFinding,
    RootCause,
    TimelineEvent,
)
from cluster_doctor.agent.diagnosis.validator import (
    validate_report,
)
from tests.contracts.test_time_range_spans import KST


def at(minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 18, 14, minute, second, tzinfo=KST)


def evidence(
    evidence_id: str,
    minute: int,
    *,
    node: str | None = None,
    source: EvidenceSource = EvidenceSource.MASTER_LOG,
) -> Evidence:
    return Evidence(
        evidence_id=evidence_id,
        event_time=at(minute),
        source=source,
        node_name=node,
        message=f"{evidence_id} 근거",
    )


def report(**overrides) -> LogAnalysisReport:
    base = {
        "incident_id": "inc-1",
        "analyzed_from": at(0),
        "analyzed_to": at(10),
        "summary": "요약",
    }
    base.update(overrides)
    return LogAnalysisReport(**base)


def issues_of(result) -> str:
    return " / ".join(result.issues)


class TestMissingAndUnknownReferences:
    def test_없는_근거를_인용하면_잡는다(self):
        result = validate_report(
            report(
                findings=(
                    ReportFinding(title="노드 이탈", evidence_refs=("E-999",)),
                )
            ),
            [evidence("E-1", 2)],
        )

        assert not result.passed
        assert "E-999" in issues_of(result)

    def test_근거_참조가_없는_주장을_잡는다(self):
        result = validate_report(
            report(findings=(ReportFinding(title="노드 이탈"),)),
            [evidence("E-1", 2)],
        )

        assert "근거 참조가 없다" in issues_of(result)

    def test_근거_없는_원인_후보를_잡는다(self):
        result = validate_report(
            report(root_causes=(RootCause(statement="GC 때문으로 보인다"),)),
            [evidence("E-1", 2)],
        )

        assert "뒷받침하는 근거가 없다" in issues_of(result)


class TestTimestampConsistency:
    def test_인용한_근거와_시각이_다르면_잡는다(self):
        result = validate_report(
            report(
                timeline=(
                    TimelineEvent(at=at(8), description="노드 이탈", evidence_refs=("E-1",)),
                )
            ),
            [evidence("E-1", 2)],
        )

        assert "근거의 시각" in issues_of(result)

    def test_분_단위_반올림은_허용한다(self):
        """모델은 14:02:11을 14:02로 쓴다. 그것까지 잡으면 지적이 잡음이 되고,
        잡음이 된 지적은 revision을 의미 없이 태운다."""
        item = Evidence(
            evidence_id="E-1",
            event_time=at(2, 41),
            source=EvidenceSource.MASTER_LOG,
            message="근거",
        )

        result = validate_report(
            report(
                timeline=(
                    TimelineEvent(at=at(2), description="노드 이탈", evidence_refs=("E-1",)),
                )
            ),
            [item],
        )

        assert result.passed


class TestNodeConsistency:
    def test_근거에_없는_노드를_지목하면_잡는다(self):
        result = validate_report(
            report(
                findings=(
                    ReportFinding(
                        title="es-data-09 노드가 이탈했다",
                        evidence_refs=("E-1",),
                    ),
                )
            ),
            [evidence("E-1", 2, node="es-data-03"), evidence("E-2", 3, node="es-data-09")],
        )

        assert "es-data-09" in issues_of(result)

    def test_근거의_노드를_지목하면_통과한다(self):
        result = validate_report(
            report(
                findings=(
                    ReportFinding(
                        title="es-data-03 노드가 이탈했다", evidence_refs=("E-1",)
                    ),
                )
            ),
            [evidence("E-1", 2, node="es-data-03")],
        )

        assert result.passed

    def test_모르는_단어를_노드로_오인하지_않는다(self):
        """본문에서 노드 이름을 추출하려 들면 임의의 단어를 노드로 읽고, 그
        오탐이 revision을 태운다. 이미 아는 이름하고만 대조한다."""
        result = validate_report(
            report(
                findings=(
                    ReportFinding(
                        title="검색 지연이 커졌다 (shard relocation 영향)",
                        evidence_refs=("E-1",),
                    ),
                )
            ),
            [evidence("E-1", 2, node="es-data-03")],
        )

        assert result.passed


class TestOrdering:
    def test_타임라인이_시간순이_아니면_잡는다(self):
        """정렬해서 조용히 고치지 않는다. 순서가 어긋났다는 것은 모델이 전개를
        잘못 읽었다는 신호이고, 그 신호는 원인 판단에도 남아 있다."""
        result = validate_report(
            report(
                timeline=(
                    TimelineEvent(at=at(5), description="나중", evidence_refs=("E-2",)),
                    TimelineEvent(at=at(2), description="먼저", evidence_refs=("E-1",)),
                )
            ),
            [evidence("E-1", 2), evidence("E-2", 5)],
        )

        assert "시간순이 아니다" in issues_of(result)


class TestOverclaiming:
    def test_근거가_얇은데_High면_잡는다(self):
        result = validate_report(
            report(
                root_causes=(
                    RootCause(
                        statement="GC 압력이 원인으로 보인다",
                        confidence="High",
                        supporting_evidence_refs=("E-1",),
                    ),
                )
            ),
            [evidence("E-1", 2)],
        )

        assert "confidence를 낮춰라" in issues_of(result)

    def test_근거가_얇은데_확정적으로_쓰면_잡는다(self):
        result = validate_report(
            report(
                root_causes=(
                    RootCause(
                        statement="heap 부족 때문이다",
                        confidence="Low",
                        supporting_evidence_refs=("E-1",),
                    ),
                )
            ),
            [evidence("E-1", 2)],
        )

        assert "표현을 낮춰라" in issues_of(result)

    def test_근거가_충분한_High는_통과한다(self):
        result = validate_report(
            report(
                root_causes=(
                    RootCause(
                        statement="GC 압력이 원인으로 보인다",
                        confidence="High",
                        supporting_evidence_refs=("E-1", "E-2"),
                    ),
                )
            ),
            [evidence("E-1", 2), evidence("E-2", 3)],
        )

        assert result.passed


class TestCausality:
    def test_원인_근거가_결과보다_늦으면_잡는다(self):
        """사고가 눈에 띄는 것은 결과 쪽이라 모델이 그쪽 근거를 원인으로
        집는다. 시각 비교는 코드가 할 수 있다."""
        result = validate_report(
            report(
                findings=(ReportFinding(title="검색 거절", evidence_refs=("E-1",)),),
                root_causes=(
                    RootCause(
                        statement="샤드 재배치로 보인다",
                        confidence="Low",
                        supporting_evidence_refs=("E-2",),
                    ),
                ),
            ),
            [evidence("E-1", 2), evidence("E-2", 8)],
        )

        assert "원인은 결과보다 먼저" in issues_of(result)

    def test_원인이_먼저면_통과한다(self):
        result = validate_report(
            report(
                findings=(ReportFinding(title="검색 거절", evidence_refs=("E-2",)),),
                root_causes=(
                    RootCause(
                        statement="샤드 재배치로 보인다",
                        confidence="Low",
                        supporting_evidence_refs=("E-1",),
                    ),
                ),
            ),
            [evidence("E-1", 2), evidence("E-2", 8)],
        )

        assert result.passed


class TestCandidatePicks:
    def test_제시되지_않은_후보를_지목하면_잡는다(self):
        """근거 참조와 같은 규칙이다. 목록 밖의 id를 고르면 코드가 수치를
        조인할 수 없다."""
        from cluster_doctor.domain.diagnosis.observations import SuspectPick

        result = validate_report(
            report(suspect_picks=(SuspectPick(candidate_id="C9", reason="r"),)),
            [evidence("E-1", 2)],
            candidate_ids={"C1", "C2"},
        )

        assert "C9" in issues_of(result)

    def test_제시된_후보는_통과한다(self):
        from cluster_doctor.domain.diagnosis.observations import SuspectPick

        result = validate_report(
            report(suspect_picks=(SuspectPick(candidate_id="C1", reason="r"),)),
            [evidence("E-1", 2)],
            candidate_ids={"C1", "C2"},
        )

        assert result.passed

    def test_지목이_없으면_볼_것도_없다(self):
        assert validate_report(report(), [evidence("E-1", 2)], candidate_ids=set()).passed


class TestAllIssuesAtOnce:
    def test_첫_불일치에서_멈추지_않는다(self):
        """한 번의 revision에 지적을 전부 실어야 모델이 한 번에 고칠 수 있다.
        하나씩 알려 주면 허용된 횟수 안에 끝나지 않는다."""
        result = validate_report(
            report(
                findings=(ReportFinding(title="근거 없음"),),
                root_causes=(RootCause(statement="원인 없음"),),
                timeline=(TimelineEvent(at=at(1), description="근거 없음"),),
            ),
            [evidence("E-1", 2)],
        )

        assert len(result.issues) >= 3


def test_빈_리포트는_지적할_것도_없다():
    """근거를 인용하지 않은 것이 아니라 주장 자체가 없다. 그것은 불일치가
    아니라 빈 리포트이고, 판단이 없다는 사실은 다른 자리에서 드러난다."""
    assert validate_report(report(), [evidence("E-1", 2)]).passed


def test_허용오차는_60초다():
    item = Evidence(
        evidence_id="E-1",
        event_time=at(2, 0),
        source=EvidenceSource.MASTER_LOG,
        message="근거",
    )
    inside = validate_report(
        report(timeline=(TimelineEvent(at=at(2, 0) + timedelta(seconds=59), description="d", evidence_refs=("E-1",)),)),
        [item],
    )
    outside = validate_report(
        report(timeline=(TimelineEvent(at=at(2, 0) + timedelta(seconds=61), description="d", evidence_refs=("E-1",)),)),
        [item],
    )

    assert inside.passed
    assert not outside.passed
