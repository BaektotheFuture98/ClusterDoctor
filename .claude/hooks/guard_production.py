"""운영 자원에 닿는 명령을 Claude Code의 셸 도구에서 막는다(PreToolUse hook).

로컬 `.env`가 운영 ClickHouse·ES·Gemini·Kafka를 가리키므로 CLAUDE.md의 금지 규칙을 문장 대신
여기서 강제한다. exit code 2는 도구 호출을 막고 stderr를 모델에게 돌려준다.
"""
import json
import re
import sys

# 실제 분석 경로나 운영 Kafka에 닿는 진입점. --dry-run은 허용한다.
_ENTRYPOINTS = re.compile(r"run_analysis\.py|produce_test_message\.py|cluster_doctor\.main\b")
# 프롬프트 평가는 기본이 오프라인 fixture이고 --mode live만 실제 모델을 부른다.
_LIVE_EVAL = re.compile(r"evaluate_report_prompts\.py.*--mode[ =]live")
# .env 본문을 출력하는 명령. .env.example은 허용한다.
_ENV_READ = re.compile(r"(cat|type|more|less|head|tail|grep|rg|Get-Content|gc|Select-String)\b[^|;&]*\.env(?![\w.])")


def main() -> int:
    # Windows Python은 stderr를 로케일(cp949)로 써서 한글 메시지가 깨진다.
    sys.stderr.reconfigure(encoding="utf-8")
    payload = json.load(sys.stdin)
    command = str((payload.get("tool_input") or {}).get("command") or "")
    if (_ENTRYPOINTS.search(command) and "--dry-run" not in command) or _LIVE_EVAL.search(command):
        print("로컬에서 실제 분석 경로·운영 Kafka 진입점을 실행하지 않는다(CLAUDE.md). --dry-run이나 단위 테스트를 쓴다.",
              file=sys.stderr)
        return 2
    if _ENV_READ.search(command):
        print(".env 값을 출력하지 않는다(CLAUDE.md). 필요한 변수 이름만 .env.example에서 확인한다.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
