---
name: verify-change
description: ClusterGuard에서 코드를 바꾼 뒤, 완료라고 말하기 전에 쓴다. 바뀐 범위에 맞춰 테스트·컴파일·Python 3.11·리포트 렌더를 확인하고, 통과하면 기능 브랜치에 커밋하며, 검증하지 못한 것을 보고한다.
---

# 변경 검증

완료를 주장하기 전에 아래를 순서대로 한다. 실행한 명령과 결과를 그대로 보고한다.

## 1. 범위 파악

- `git diff --name-only main...HEAD`와 `git status --short`로 바뀐 파일을 모은다.
- 바뀐 패키지를 정하고, 그 패키지 CLAUDE.md의 불변식을 다시 읽는다.

## 2. 테스트

- 바뀐 패키지의 테스트부터: `uv run pytest -q tests/unit/<패키지>/`
- 그다음 전체: `uv run pytest -q`
- 동작을 바꿨는데 실패하는 테스트가 없다면, 그 동작을 잡는 테스트가 있는지 확인하고 없으면 추가한다.

## 3. 컴파일

- `uv run python -m compileall -q src`

## 4. Python 3.11 (서버 버전)

asyncio, 타입 표기, 새 문법, 표준 라이브러리 API를 건드렸을 때만 한다.

- 로컬 `.venv`(3.13)를 덮어쓰지 않도록 별도 환경을 쓴다:
  `UV_PROJECT_ENVIRONMENT=.venv311 uv run --python 3.11 pytest -q`

## 5. 리포트 렌더 (projection·rendering을 바꿨을 때)

- `uv run python scripts/preview_report.py --variant default`
  (합성 DTO만 쓰고 설정·외부 연결 없음. `reports/preview-default.html`을 연다.)
- HTML과 평문 폴백(`rendering/text/report_text.py`)을 둘 다 고쳤는지 확인한다.

## 6. 하지 않는 것

- 실제 분석·Kafka·SFTP 진입점은 실행하지 않는다(hook이 막는다). `run_analysis.py`는 `--dry-run`만.
- 사고 원인의 정확성은 여기서 검증할 수 없다. 서버 리포트로 사람이 확인할 항목으로 남긴다.

## 7. 문서 동기화

`/release`도 이 목록으로 문서 누락을 확인한다. 기준은 여기에만 둔다.

- 불변식이 바뀌었으면 해당 패키지 CLAUDE.md.
- 운영 규칙(회사 내규 등) 때문에 둔 선별·판정 기준이면, 프롬프트만 바꿨어도 해당 패키지 CLAUDE.md에 이유와 함께 적는다. 이유가 없으면 나중에 기준을 정리하다 지운다.
- 고정 한도가 바뀌었으면 README 고정 한도 표.
- 보고서 섹션·표시가 바뀌었으면 README 보고서 표.
- `pyproject.toml`의 `requires-python`·의존성이 바뀌었으면 README 요구 사항.
- 새 실행 진입점이 생겼으면 `.claude/hooks/guard_production.py` 목록.
- 해당 없음으로 판단했으면 9단계 "건너뜀"에 무엇을 왜 안 고쳤는지 적는다.

## 8. 커밋

2~3단계(와 해당하면 4·5단계)가 모두 통과하고 기능 브랜치(`feat/…`, `fix/…`, `docs/…`)에 있으면 묻지 않고 커밋한다.

- `main`에 있거나 하나라도 실패했으면 커밋하지 않고 보고만 한다.
- 이번 task에서 바꾼 파일만 `git add`한다. 무관한 변경이 섞여 있으면 커밋하지 않고 알린다.
- 메시지에 왜 바꿨는지 적는다. hook이 `.env` 문자열을 막으므로 메시지는 `git commit -F <파일>`이나 heredoc으로 넘긴다.
- 병합·버전·태그·push는 하지 않는다(`/release` 소관).

## 9. 보고

- 통과: 실행한 명령과 결과
- 커밋: 커밋 해시와 메시지 첫 줄, 또는 커밋하지 않은 이유
- 건너뜀: 무엇을 왜 건너뛰었는지
- 사람이 확인할 것: 서버 리포트에서 볼 항목
