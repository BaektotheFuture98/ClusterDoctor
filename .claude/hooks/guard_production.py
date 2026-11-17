"""운영 자원에 닿는 명령을 Claude Code의 셸 도구에서 막는다(PreToolUse hook).

로컬 `.env`가 운영 ClickHouse·ES·Gemini·Kafka·리포트 SFTP를 가리키므로 CLAUDE.md의 금지 규칙을 문장 대신
여기서 강제한다. exit code 2는 도구 호출을 막고 stderr를 모델에게 돌려준다.

python이 이 파일을 찾지 못해도 exit code 2로 끝나므로 모든 셸 호출이 막힌다. 셸이 전부 막히면
`$CLAUDE_PROJECT_DIR`이 이 저장소를 가리키는지 먼저 본다.
"""
import json
import re
import sys

# 실제 분석 경로나 운영 Kafka에 닿는 진입점. --dry-run은 같은 명령 조각 안에 있을 때만 허용한다.
_ENTRYPOINTS = re.compile(
    r"run_analysis\.py|produce_test_message\.py|cluster_doctor\.main\b"
    r"|from\s+cluster_doctor\s+import\s+[^;\n]*\bmain\b"
)
# 리포트를 실제 SFTP 서버에 올린다. dry-run 모드가 없어 항상 막는다.
_SFTP_UPLOAD = re.compile(r"upload_report_sftp\.py")
# 프롬프트 평가는 기본이 오프라인 fixture이고 --mode live만 실제 모델을 부른다.
_LIVE_EVAL = re.compile(r"evaluate_report_prompts\.py.*--mode[ =]live")
# `.env`·`.env.<접미사>`를 명령에 쓰면 막는다. 출력 명령 이름으로 거르면 sed·awk·python -c로 빠져나가므로
# 명령 종류와 상관없이 파일 이름으로 판정한다. 변수 이름 확인용 `.env.example`만 허용한다.
_ENV_FILE = re.compile(r"\.env((?:\.[\w-]+)*)(?![\w-])")
# `--dry-run; 실제 실행`처럼 한 줄에 이어 붙인 명령을 조각마다 따로 판정한다.
_SEGMENT_SPLIT = re.compile(r"&&|\|\||[;|&\n]")


def _touches_env_file(command: str) -> bool:
    return any(match.group(1) != ".example" for match in _ENV_FILE.finditer(command))


def _runs_live_entrypoint(command: str) -> bool:
    for segment in _SEGMENT_SPLIT.split(command):
        if _ENTRYPOINTS.search(segment) and "--dry-run" not in segment:
            return True
        if _LIVE_EVAL.search(segment):
            return True
    return False


def check(command: str) -> str | None:
    """막아야 하면 모델에게 돌려줄 사유를, 아니면 None을 반환한다."""
    if _runs_live_entrypoint(command):
        return "로컬에서 실제 분석 경로·운영 Kafka 진입점을 실행하지 않는다(CLAUDE.md). --dry-run이나 단위 테스트를 쓴다."
    if _SFTP_UPLOAD.search(command):
        return "로컬에서 리포트를 실제 SFTP 서버에 올리지 않는다(CLAUDE.md). 업로드 확인이 필요하면 사용자에게 실행을 요청한다."
    if _touches_env_file(command):
        # 문자열 안의 언급도 막는다. 따옴표 안을 빼고 판정하면 python -c "open('.env')"가, git commit을 빼면
        # git commit -F .env가 통과한다. 오탐은 아래 안내대로 피한다.
        return (".env 값을 출력하지 않는다(CLAUDE.md). 셸 명령에 .env를 쓰지 않고, 변수 이름은 .env.example에서 확인한다. "
                "커밋 메시지에 .env를 써야 하면 파일로 써서 git commit -F <파일>로 넘기고, 코드 검색은 Grep 도구를 쓴다.")
    return None


def main() -> int:
    # Windows Python은 stderr를 로케일(cp949)로 써서 한글 메시지가 깨진다.
    sys.stderr.reconfigure(encoding="utf-8")
    payload = json.load(sys.stdin)
    command = str((payload.get("tool_input") or {}).get("command") or "")
    reason = check(command)
    if reason is not None:
        print(reason, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
