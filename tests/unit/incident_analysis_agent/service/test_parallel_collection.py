from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from types import SimpleNamespace

from cluster_doctor.incident_analysis_agent.model.log_fetch import LogFetchResult
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.evidence_collection.collector import EvidenceCollector
from cluster_doctor.incident_analysis_agent.service.observation.builder import ObservationBuilder
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import AnalysisResult

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def collector(fetch_logs):
    return EvidenceCollector(new_evidence_id=lambda: 'E', fetch_logs=fetch_logs,
        fetch_node_logs=lambda *a, **k: [], cluster=None, node_resolver=None,
        node_log_fetcher=None, call_llm=lambda *a, **k: '{}')


def test_minute_queries_overlap_and_keep_failure_statuses():
    barrier = Barrier(2)
    window = TimeRange(T0, T0 + timedelta(minutes=2))
    def fetch(span):
        barrier.wait(timeout=2)
        if span.start == T0:
            raise OSError('one minute failed')
        return LogFetchResult()
    state = ObservationBuilder(window)
    collector(fetch)._fetch_and_bucket(window, state)
    statuses = state.source_statuses
    assert len(statuses) == 6
    assert sum(s.status == 'failed' for s in statuses) == 3
    assert not state.degraded


def test_sources_overlap_with_master_and_use_independent_builders(monkeypatch):
    barrier = Barrier(3)
    builders = []
    window = TimeRange(T0, T0 + timedelta(minutes=1))
    c = collector(lambda _: LogFetchResult())
    monkeypatch.setattr(c, '_collect_cluster_health', lambda state: [])
    monkeypatch.setattr(c, '_fetch_and_bucket', lambda w, state: ([], [], []))
    def master(w, state):
        builders.append(state)
        barrier.wait(timeout=2)
        state.mark_gap('master gap')
        return []
    def select(spec, buckets, state):
        builders.append(state)
        barrier.wait(timeout=2)
        state.mark_gap(spec.label)
        return AnalysisResult(evidence=[])
    monkeypatch.setattr(c, '_collect_master', master)
    monkeypatch.setattr(c, '_run_analysis', select)
    monkeypatch.setattr(c, '_investigate_nodes', lambda m, w, state:
        SimpleNamespace(evidence=[], investigated=[]))
    state = ObservationBuilder(window)
    c.collect(window, state)
    assert len({id(b) for b in builders}) == 3
    assert 'master gap' in state.gaps
    assert len(state.gaps) == 3


def test_llm_transport_never_exceeds_fifteen_requests(monkeypatch):
    from threading import Event, Lock
    from cluster_doctor.incident_analysis_agent.agent.runtime import litellm_client
    ready, release, overflow = Event(), Event(), Event()
    lock = Lock()
    active = peak = 0
    calls = 0
    def completion(**kwargs):
        nonlocal active, peak, calls
        with lock:
            active += 1
            calls += 1
            peak = max(peak, active)
            if active == 15:
                ready.set()
            if active > 15:
                overflow.set()
        try:
            assert release.wait(timeout=3)
            return SimpleNamespace(choices=[SimpleNamespace(
                finish_reason='stop', message=SimpleNamespace(content='{}'))])
        finally:
            with lock:
                active -= 1
    monkeypatch.setattr(litellm_client, '_throttle', lambda _: None)
    monkeypatch.setattr(litellm_client.litellm, 'completion', completion)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(litellm_client.complete, [], 'gemini', 'mock', 'mock')
            for _ in range(20)]
        try:
            assert ready.wait(timeout=2)
            assert not overflow.wait(timeout=0.1)
        finally:
            release.set()
        assert all(f.result() == '{}' for f in futures)
    assert peak == 15 and calls == 20


def test_provider_spacing_is_applied_after_capacity_is_acquired(monkeypatch):
    from cluster_doctor.incident_analysis_agent.agent.runtime import litellm_client
    events = []
    class Slot:
        def __enter__(self): events.append('acquire')
        def __exit__(self, *args): events.append('release')
    monkeypatch.setattr(litellm_client, '_llm_slots', Slot())
    monkeypatch.setattr(litellm_client, '_throttle', lambda _: events.append('throttle'))
    def completion(**kwargs):
        events.append('request')
        return SimpleNamespace(choices=[SimpleNamespace(
            finish_reason='stop', message=SimpleNamespace(content='{}'))])
    monkeypatch.setattr(litellm_client.litellm, 'completion', completion)
    assert litellm_client.complete([], 'gemini', 'mock', 'mock') == '{}'
    assert events == ['acquire', 'throttle', 'request', 'release']
