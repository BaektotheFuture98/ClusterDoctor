"""Kafka도 main 프로세스도 없이 진단을 한 번 돌린다.

왜 있는가: 시각을 지정해 진단을 확인하려면 원래 터미널 둘이 필요했다 —
`cluster_doctor.main`을 띄우고(auto_offset_reset=latest라 반드시 먼저),
다른 창에서 produce_test_message로 메시지를 보내는 식이다. 이 스크립트는
그 두 단계를 하나로 접는다.

프로덕션과 같은 경로를 타는가: 탄다. 핵심은 **pending 큐에 합성 항목을
심는 것**이다. 큐가 비어 있으면 check_new_slowlogs가 last_seen을 끝내
채우지 못하고, 그러면 suggested_windows가 실리지 않아(tools.py의
``if settled and first_seen and last_seen``) agent가 구간을 스스로 정하는
다른 갈래로 빠진다. 큐를 채우면 first_seen/last_seen/zero_streak/
suggested_windows가 전부 실제와 같이 흐른다.

타지 않는 것: SlowlogTriggerService의 배치 창과 재트리거다. analyze를
직접 부르므로 진단이 정확히 한 번 돈다. 의도한 것이다 — 재트리거가
할당량을 예고 없이 더 태우면 측정이 흐려진다. 그 계층은 단위 테스트가
덮는다.

주의: 실제 LLM·ClickHouse·Elasticsearch를 호출한다. 5분 창 실측이 입력
513,122 토큰이므로 조용한 구간이나 짧은 구간부터 시작할 것.

사용 예:
    uv run python scripts/run_diagnosis.py --at "2026-08-27T14:00:00" --dry-run
    uv run python scripts/run_diagnosis.py --at "2026-08-27T14:00:00" --count 5 --span 3m
"""
import argparse
import json
import pathlib
import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

# _timeargs를 가장 먼저 import한다. 그 안의 force_utf8_console()이 콘솔
# 인코딩을 고치는데, 아래 cluster_doctor.main이 stderr를 붙잡는 로깅 핸들러를
# 만들기 때문이다. reconfigure는 스트림 객체를 제자리에서 바꾸므로 순서가
# 뒤바뀌어도 동작하지만, 의도를 순서로 드러내 둔다.
import _timeargs
from _timeargs import KST

# import 시점에 configure_logging()이 돌아 stderr와 logs/app.log에 로그가 붙는다.
import cluster_doctor.main  # noqa: F401

from cluster_doctor.domain.model.kafka.slowlog_entry import SlowlogEntry
from cluster_doctor.infrastructure.config.dependencies import (
    build_trigger_service,
    close_clickhouse_client,
)
from cluster_doctor.infrastructure.config.settings import get_settings
from cluster_doctor.infrastructure.outbound.notifier.report_text import render_text

# 기준 시각 판정을 다시 구현하지 않는다. 실제 동작과 어긋날 수 있고,
# 어긋난 안내는 없느니만 못하다.
from cluster_doctor.application.service.inflow import base_time
from cluster_doctor.application.service.report_assembler import to_diagnosis_report
from cluster_doctor.domain.model.incident import Incident, TriggerType


# ── LLM 호출 측정 ──────────────────────────────────────────────────
# 오케스트레이터는 tool 루프라 매 턴 전체 히스토리를 다시 보낸다. 그래서
# 호출별 입력 토큰의 증가가 곧 컨텍스트 누적량이다. 그 값을 추정이 아니라
# 실측으로 남기려고 litellm 콜백을 건다.
_llm_calls: list[dict] = []

# --dump-context가 켜졌을 때만 채운다. 기본적으로 들고 있지 않는 이유는
# 부피다 — 오케스트레이터 한 턴이 1만 토큰이고 턴은 7회까지 간다.
_capture_messages = False


def _record_llm_call(kwargs, response, start_time, end_time) -> None:
    """litellm success 콜백. 어떤 실패도 진단을 막아서는 안 된다.

    부가 정보를 얻으려다 본래 작업을 죽이는 것은 뒤바뀐 우선순위다 —
    litellm_client._log_provider_error가 같은 이유로 통째로 try에 싸여 있다.
    """
    try:
        usage = getattr(response, "usage", None)
        _llm_calls.append(
            {
                "end": end_time,
                "prompt": getattr(usage, "prompt_tokens", None),
                "completion": getattr(usage, "completion_tokens", None),
                # 도구 정의가 실린 호출이 오케스트레이터다. 분 단위 분석과
                # 종합은 도구 없이 프롬프트만 보낸다.
                "has_tools": bool(kwargs.get("tools")),
                "max_tokens": kwargs.get("max_tokens"),
                # 실제로 보낸 것. 토큰 수는 "얼마나 쌓였나"만 말하고 "무엇이
                # 쌓였나"는 말하지 않는다.
                "messages": kwargs.get("messages") if _capture_messages else None,
                "tools": kwargs.get("tools") if _capture_messages else None,
            }
        )
    except Exception:
        pass


def _dump_context(path: pathlib.Path) -> None:
    """오케스트레이터가 매 턴 실제로 받은 것을 파일로 떨군다.

    로그에는 tool이 **돌려준 값**이 남지만 조립된 메시지 배열은 남지 않는다.
    SYSTEM_PROMPT가 매 턴 재전송된다는 것도, ToolMessage가 어떤 모양으로
    포장되는지도 여기서만 보인다.

    JSON은 기계용이고, 옆에 사람이 읽을 요약을 함께 쓴다 — 턴마다 역할별
    글자 수를 세어 두면 어디서 컨텍스트가 불어나는지 바로 보인다.
    """
    calls = sorted(_llm_calls, key=lambda c: c["end"] or 0)
    if not calls or not calls[0].get("messages"):
        print("  (컨텍스트를 붙들지 못했다 — --dump-context 없이 실행했거나 호출이 없다)")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "call": i,
            # 도구 정의가 실린 호출이 오케스트레이터, 아닌 것이 분 단위
            # 분석과 종합이다. 둘을 함께 남겨야 한 진단의 전모가 보인다.
            "kind": "orchestrator" if c["has_tools"] else "pipeline",
            "completion_tokens": c["completion"],
            "max_tokens": c["max_tokens"],
            "prompt_tokens": c["prompt"],
            "tool_count": len(c["tools"] or []),
            # tool 스키마도 남긴다. 매 턴 재전송되므로 이것도 컨텍스트이고,
            # 실측에서 우리가 정의하지 않은 것이 절반을 넘었다.
            "tools": c["tools"],
            "messages": c["messages"],
        }
        for i, c in enumerate(calls, 1)
    ]
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print("\n-- LLM 호출 전문 -------------------------------------------")
    print(f"  전문: {path}")
    tool_names = [
        (t.get("function") or {}).get("name", "?")
        for t in (next((c["tools"] for c in calls if c["tools"]), []) or [])
    ]
    print(f"  tool 정의 {len(tool_names)}개: {', '.join(tool_names)}")
    print()
    print("  호출  구분            입력토큰  출력상한  메시지  역할별 글자 수")
    for i, call in enumerate(calls, 1):
        per_role: dict[str, int] = {}
        for message in call["messages"] or []:
            role = str(message.get("role", "?"))
            size = len(str(message.get("content") or ""))
            for tc in message.get("tool_calls") or []:
                size += len(json.dumps(tc, ensure_ascii=False))
            per_role[role] = per_role.get(role, 0) + size
        shape = "  ".join(f"{r}={n:,}" for r, n in sorted(per_role.items()))
        kind = "오케스트레이터" if call["has_tools"] else "파이프라인"
        print(
            f"  {i:4d}  {kind:12}  {_fmt_int(call['prompt']):>8}  "
            f"{_fmt_int(call['max_tokens']):>8}  "
            f"{len(call['messages'] or []):5d}   {shape}"
        )


def _install_probe() -> bool:
    try:
        import litellm

        litellm.success_callback = [_record_llm_call]
        return True
    except Exception:
        return False


def _fmt_int(value) -> str:
    return f"{value:,}" if isinstance(value, int) else "-"


def _print_measurements() -> None:
    print("\n-- LLM 호출 측정 -------------------------------------------")
    if not _llm_calls:
        print("  기록된 호출이 없다 (콜백이 걸리지 않았거나 호출 전에 실패했다).")
        return

    calls = sorted(_llm_calls, key=lambda c: c["end"] or 0)
    prompts = [c["prompt"] for c in calls if isinstance(c["prompt"], int)]
    completions = [c["completion"] for c in calls if isinstance(c["completion"], int)]
    print(f"  총 호출      : {len(calls)}")
    print(f"  입력 토큰 합 : {_fmt_int(sum(prompts)) if prompts else '-'}")
    print(f"  출력 토큰 합 : {_fmt_int(sum(completions)) if completions else '-'}")

    orchestrator = [c for c in calls if c["has_tools"]]
    pipeline = [c for c in calls if not c["has_tools"]]

    if orchestrator:
        print(
            "\n  오케스트레이터 (도구 정의 포함)"
            " — 입력 토큰의 증가가 곧 컨텍스트 누적이다"
        )
        previous = None
        for i, call in enumerate(orchestrator, 1):
            prompt = call["prompt"]
            delta = ""
            if isinstance(prompt, int) and isinstance(previous, int):
                delta = f"   (+{prompt - previous:,})"
            print(f"    {i:3d}. 입력 {_fmt_int(prompt):>10}{delta}")
            previous = prompt
        first, last = orchestrator[0]["prompt"], orchestrator[-1]["prompt"]
        if isinstance(first, int) and isinstance(last, int):
            print(f"    -> 첫 호출 {first:,} / 마지막 호출 {last:,} ({last - first:+,})")
    else:
        print(
            "\n  오케스트레이터 호출을 구분하지 못했다"
            " (kwargs에 tools가 실리지 않는 경로일 수 있다)."
        )

    if pipeline:
        pipeline_prompts = [
            c["prompt"] for c in pipeline if isinstance(c["prompt"], int)
        ]
        total = _fmt_int(sum(pipeline_prompts)) if pipeline_prompts else "-"
        print(f"\n  분 단위/종합 파이프라인 : {len(pipeline)}회, 입력 합계 {total}")


# ── 실행 ───────────────────────────────────────────────────────────
def _snapshot_reports(report_dir: Path) -> set:
    try:
        return set(report_dir.glob("*.html"))
    except OSError:
        return set()


def run(moments: list) -> int:
    settings = get_settings()
    report_dir = Path(settings.report_dir)
    before = _snapshot_reports(report_dir)

    # build_trigger_service가 큐·Agent·notifier를 한곳에서 조립한다.
    # 여기서 다시 조립하지 않고 그 결과를 빌려 쓴다 — 조립을 복제하면
    # dependencies.py가 바뀔 때 이 스크립트만 조용히 낡는다.
    service = build_trigger_service(settings)
    orchestrator = service._orchestrator
    for moment in moments:
        service._pending.put(SlowlogEntry(timestamp=moment))

    log_time = min(moments)
    receive_time = datetime.now(timezone.utc)
    incident = Incident(
        incident_id=f"manual-{int(time.time())}",
        cluster=settings.cluster_name,
        trigger_time=log_time,
        kafka_receive_time=receive_time,
        trigger_type=TriggerType.MANUAL,
    )

    started = time.monotonic()
    try:
        # orchestrator가 리포트 전달까지 맡는다. 여기서 따로 notify하지 않는다 —
        # 두 번 부르면 리포트 파일이 두 개 생긴다.
        outcome = asyncio.run(orchestrator.run(incident))
    except Exception as exc:
        elapsed = time.monotonic() - started
        print(f"\n[실패] 진단이 예외로 끝났다 ({elapsed:.1f}초): {type(exc).__name__}: {exc}")
        _print_measurements()
        return 1
    elapsed = time.monotonic() - started

    store = orchestrator._store
    observations = store.get_observations(incident.incident_id)
    report = store.get_report(outcome.report_ref) if outcome.report_ref else None
    evidence = store.list_evidence(incident.incident_id)
    rendered = to_diagnosis_report(report, observations, evidence)

    print("\n-- 결과 ----------------------------------------------------")
    print(f"  소요 시간       : {elapsed:.1f}초")
    print(f"  Incident 상태   : {outcome.status}")
    print(f"  분석 호출       : {outcome.analysis_calls}회")
    print(f"  analysis_failed : {outcome.analysis_failed}")
    print(f"  gaps            : {len(outcome.gaps)}건")
    for gap in outcome.gaps:
        print(f"      - {gap}")
    if report is not None:
        print(f"  검증            : {report.verification_status} (수정 {report.revision_count}회)")
        for issue in report.verification_issues:
            print(f"      - {issue}")
    print(f"  Evidence        : {len(evidence)}건")
    print(f"  리포트 길이     : {len(render_text(rendered)):,}자")
    print(
        f"  관측값          : 타임라인 {len(observations.timeline)}분 / "
        f"노드 {len(observations.nodes)}개 / "
        f"마스터로그 {observations.master_log_total}줄 / "
        f"상태 {len(observations.health)}건 / "
        f"후보 {len(observations.candidates)}건"
    )

    created = _snapshot_reports(report_dir) - before
    for path in sorted(created):
        print(f"  리포트 파일     : {path}")

    _print_measurements()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Kafka 없이 진단을 한 번 실행한다.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "실제 LLM·ClickHouse·Elasticsearch를 호출한다."
            " --dry-run으로 먼저 확인할 것."
        ),
    )
    _timeargs.add_arguments(parser)
    parser.add_argument(
        "--dump-context",
        metavar="PATH",
        nargs="?",
        const="context-dump.json",
        help=(
            "오케스트레이터가 매 턴 실제로 받은 메시지 배열을 JSON으로 남긴다. "
            "SYSTEM_PROMPT와 tool 정의 원문이 들어가므로 저장소에 올리지 말 것."
        ),
    )
    args = parser.parse_args()

    moments = _timeargs.resolve(args)

    print("  실행 방식 : Kafka 없이 Incident 직접 실행 (pending 큐에 합성 항목 주입)")
    _timeargs.describe_moments(moments)

    log_time = min(moments)
    receive_time = datetime.now(timezone.utc)
    base, basis = base_time(log_time, receive_time)
    print(f"\n  기준 시각 판정    : {basis}")
    print(f"  first_seen 초기값 : {base.astimezone(KST):%Y-%m-%d %H:%M:%S} KST")
    print(
        "  큐 주입 후 갱신될 값 : first_seen="
        f"{min(moments).astimezone(KST):%Y-%m-%d %H:%M:%S}, "
        f"last_seen={max(moments).astimezone(KST):%Y-%m-%d %H:%M:%S} KST"
    )

    if args.dry_run:
        print("\n(dry-run: 실행하지 않았다)")
        return 0

    print("\n  실제 LLM·ClickHouse를 호출한다. 2단계 대기 루프 때문에 최소 ~60초 걸린다.")
    if not _install_probe():
        print("  (litellm 콜백을 걸지 못했다 — 토큰 측정 없이 진행한다)")

    if args.dump_context:
        global _capture_messages
        _capture_messages = True
        print(f"  컨텍스트 전문을 {args.dump_context}에 남긴다.")

    try:
        code = run(moments)
        if args.dump_context:
            _dump_context(pathlib.Path(args.dump_context))
        return code
    except KeyboardInterrupt:
        print("\n중단했다.")
        _print_measurements()
        return 130
    finally:
        close_clickhouse_client()


if __name__ == "__main__":
    raise SystemExit(main())
