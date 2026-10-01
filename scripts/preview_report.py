"""Generate a fictional, offline report to review the layout without an LLM."""

import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    NodeMetricRow,
    Observations,
    SlowCandidate,
    TimelineRow,
)
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    RootCause,
    SuspectPick,
    TimelineEvent,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    render_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.summary_view import (
    DEMO_GAP,
)


LONG_QUERY = '{"query":{"bool":{"filter":[{"terms":{"keyword":["' + "반도체수출" * 60 + '"]}}]}}}'
LONG_PATH = "/var/log/elasticsearch/demo-es/archive/2026/10/01/" + "nested-directory/" * 6 + "demo-es-2026-10-01-14.log.gz"
LONG_SENTENCE = (
    "검색 스레드풀 큐가 가득 찬 상태에서 고비용 집계 요청이 같은 시간대에 반복적으로 들어오면서 "
    "데이터 노드의 처리 용량이 포화되었고 그 결과 일부 요청이 거절되었을 가능성이 있으나 "
    "요청량 증가와 쿼리 비용 증가 중 어느 쪽이 먼저였는지는 수집된 근거만으로 확정할 수 없다"
)
VARIANTS = {
    "default": (VerificationStatus.PASSED, ()),
    "mismatch": (
        VerificationStatus.MISMATCH,
        ("E-demo-2 인용문이 수집된 원문과 일치하지 않음", "E-demo-9 존재하지 않는 근거 참조"),
    ),
    "not-verified": (VerificationStatus.NOT_VERIFIED, ()),
}


def main(variant: str = "default") -> None:
    status, issues = VARIANTS[variant]
    start = datetime(2026, 10, 1, 14, 2, tzinfo=KST)
    end = start + timedelta(minutes=10)
    queue_time = start + timedelta(minutes=1)
    rejection_time = start + timedelta(minutes=2)
    evidence = [
        Evidence(
            evidence_id="E-demo-1",
            event_time=queue_time,
            source=EvidenceSource.NODE_METRIC,
            node_name="data-03",
            event_type="node_metric_queue",
            severity="Warning",
            message="data-03 search_queue=128 search_rejected=0",
            raw_kind="record",
            raw='{"node_name": "data-03", "search_queue": 128, "search_rejected": 0}',
            provenance=EvidenceProvenance(
                method="clickhouse",
                table="demo.node_metrics",
                query_from=start,
                query_to=end,
                collected_at=end,
            ),
        ),
        Evidence(
            evidence_id="E-demo-2",
            event_time=rejection_time,
            source=EvidenceSource.NODE_LOG,
            node_name="data-03",
            event_type="rejection",
            severity="Warning",
            message="data-03: EsRejectedExecutionException",
            raw_kind="log",
            raw="[2026-10-01T14:04:00,000][WARN ][o.e.a.s.TransportSearchAction] [data-03]\n"
            "EsRejectedExecutionException: rejected execution of search task\n"
            "on EsThreadPoolExecutor[name = data-03/search, queue capacity = 1000]\n"
            + "\n".join(
                f"    at example.search.Frame{n}.execute(Frame{n}.java:42)"
                for n in range(14)
            ),
            provenance=EvidenceProvenance(
                method="ssh",
                host="192.0.2.23",
                file_path="/var/log/elasticsearch/demo-es.log",
                query_from=start,
                query_to=end,
                collected_at=end,
                excerpt=True,
            ),
        ),
    ]
    gc_time = start + timedelta(minutes=4)
    recovery_time = start + timedelta(minutes=6)
    evidence.extend(
        [
            Evidence(
                evidence_id="E-demo-3",
                event_time=gc_time,
                source=EvidenceSource.NODE_LOG,
                node_name="data-03",
                event_type="gc",
                severity="Warning",
                message="GC overhead: spent 2.1s collecting in 2.5s",
                raw="[2026-10-01T14:06:00,000][WARN ][o.e.m.j.JvmGcMonitorService] [data-03]\n"
                "[gc][overhead] spent [2.1s] collecting in the last [2.5s]",
                provenance=evidence[1].provenance,
            ),
            Evidence(
                evidence_id="E-demo-4",
                event_time=recovery_time,
                source=EvidenceSource.NODE_METRIC,
                node_name="data-03",
                raw_kind="record",
                message="search_queue=8",
                raw='{"node_name":"data-03","search_queue":8}',
                provenance=evidence[0].provenance,
            ),
        ]
    )
    evidence.extend(
        [
            Evidence(
                evidence_id="E-demo-5",
                event_time=start + timedelta(minutes=3),
                source=EvidenceSource.SLOWLOG,
                node_name="data-03",
                event_type="slowlog",
                severity="Warning",
                message="slow search took 31s",
                raw_kind="query",
                raw=LONG_QUERY,
                raw_truncated=True,
                provenance=EvidenceProvenance(
                    method="ssh",
                    host="192.0.2.23",
                    file_path=LONG_PATH,
                    query_from=start,
                    query_to=end,
                    collected_at=end,
                ),
            ),
            Evidence(
                evidence_id="E-demo-6",
                event_time=start + timedelta(minutes=5),
                source=EvidenceSource.NODE_LOG,
                node_name="data-03",
                message="stack trace with an unbroken line",
                raw="java.lang.OutOfMemoryError: " + "Java heap space " * 3 + "\n"
                "    at " + ".".join(f"demo.pkg{n}" for n in range(40)) + ".run(Demo.java:1)\n"
                + "\n".join(f"    at example.Frame{n}.run(Frame{n}.java:{n})" for n in range(30)),
                provenance=EvidenceProvenance(
                    method="ssh", host="192.0.2.23", file_path=LONG_PATH
                ),
            ),
        ]
    )
    report = LogAnalysisReport(
        incident_id="DEMO-1",
        analyzed_from=start,
        analyzed_to=end,
        summary="검색 지연 증가와 일부 요청 거절 발생",
        verification_status=status,
        verification_issues=issues,
        timeline=(
            TimelineEvent(
                at=queue_time, description="검색 큐 증가", evidence_refs=("E-demo-1",)
            ),
            TimelineEvent(
                at=rejection_time,
                description="검색 요청 거절 발생",
                evidence_refs=("E-demo-2",),
            ),
            TimelineEvent(
                at=gc_time,
                description="긴 GC pause 관측 · 앞선 요청 거절보다 늦어 최초 원인인지는 확인 필요",
                evidence_refs=("E-demo-3",),
            ),
            TimelineEvent(
                at=recovery_time,
                description="검색 대기열 감소 · 서비스 회복 여부는 추가 확인 필요",
                evidence_refs=("E-demo-1", "E-demo-4"),
            ),
        ),
        root_causes=(
            RootCause(
                statement="검색 부하 집중에 따른 처리 포화 가능성",
                confidence="Medium",
                supporting_evidence_refs=("E-demo-1", "E-demo-2"),
            ),
            RootCause(
                statement=LONG_SENTENCE,
                confidence="Low",
                supporting_evidence_refs=("E-demo-3", "E-demo-5", "E-demo-6"),
                counter_evidence_refs=("E-demo-4",),
            ),
        ),
        suspect_picks=(
            SuspectPick(
                candidate_id="C1",
                reason="같은 키워드 집계가 거절 시점 직전에 가장 오래 걸렸다. " + LONG_SENTENCE,
            ),
        ),
        unresolved_questions=(
            "요청량 증가 또는 고비용 쿼리 중 무엇이 부하를 유발했는지 확인 필요",
            LONG_SENTENCE,
        ),
        recommendations=(
            "data-03의 검색 큐와 요청 거절 추이를 확인합니다.",
            "해당 구간의 고비용 쿼리와 요청량 변화를 확인합니다.",
            "다른 데이터 노드에서도 같은 현상이 발생했는지 확인합니다.",
        ),
    )
    query_requests = tuple(
        QueryLogEntry(
            reg_date=start + timedelta(minutes=minute, seconds=12),
            host="demo-es-host",
            run_time=Decimal(duration),
            success="Y" if success else "N",
            cmd=cmd,
            service="web",
            env="demo",
            project="demo-project",
            cluster="demo-es",
            keyword=keywords,
            s_date=20260924 if minute == 2 else 20260901,
            e_date=20260930,
            date_range=7 if minute == 2 else 30,
            search_count=42,
            url="/search",
            etc="",
            company=company,
            user=user,
            provenance=EvidenceProvenance(
                method="clickhouse",
                table="demo.log",
                query_from=start,
                query_to=end,
                collected_at=end,
            ),
        )
        for minute, duration, cmd, keywords, company, user, success in (
            (1, "31", "agg", ("반도체", "수출"), "가상 회사 A", "demo-user-1", True),
            (2, "12", "agg", ("반도체", "수출"), "가상 회사 A", "demo-user-1", False),
            (3, "4", "search", ("환율",), "가상 회사 B", "demo-user-2", True),
            (5, "8", "search", ("환율",), "가상 회사 B", "demo-user-2", True),
            (6, "3", "count", ("반도체",), "가상 회사 A", "demo-user-3", True),
        )
    )
    obs = Observations(
        query_requests=query_requests,
        candidates=(
            SlowCandidate(
                candidate_id="C1",
                source=QueryLogEntry.source,
                timestamp=query_requests[1].timestamp,
                node="demo-es-host",
                run_time=Decimal("12"),
                cmd="agg",
                company="가상 회사 A",
                user="demo-user-1",
            ),
        ),
        nodes=(
            NodeMetricRow(
                node="data-03",
                samples=10,
                jvm_heap_max=82,
                search_queue_max=128,
                search_rejected_max=3,
            ),
            NodeMetricRow(node="data-09", samples=0),
        ),
        requested=((start, end),),
        time_basis="event_time",
        timeline=(
            TimelineRow(minute=queue_time, counts={"node_metric": 1}, jvm_heap_max=82),
            TimelineRow(minute=gc_time, counts={"es_node_log": 2}),
            TimelineRow(
                minute=rejection_time, counts={"es_query_log": 8}, search_rejected_max=3
            ),
        ),
    )
    name = "preview-report" if variant == "default" else f"preview-{variant}"
    output = Path(f"reports/{name}.html")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        render_report(
            to_incident_analysis_report(report, obs, evidence, cluster="demo-es"),
            generated_at=end,
            gaps=(DEMO_GAP,),
        ),
        encoding="utf-8",
    )
    print(output.resolve())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=sorted(VARIANTS), default="default")
    main(parser.parse_args().variant)
