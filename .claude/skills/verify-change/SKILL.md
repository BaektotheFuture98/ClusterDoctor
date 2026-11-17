---
name: verify-change
description: ClusterGuard에서 코드를 바꾼 뒤, 완료라고 말하거나 커밋하기 전에 쓴다. 바뀐 범위에 맞춰 테스트·컴파일·Python 3.11·리포트 렌더를 확인하고 검증하지 못한 것을 보고한다.
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

- 불변식이 바뀌었으면 해당 패키지 CLAUDE.md.
- 고정 한도가 바뀌었으면 README 고정 한도 표.
- 새 실행 진입점이 생겼으면 `.claude/hooks/guard_production.py` 목록.

## 8. 보고

- 통과: 실행한 명령과 결과
- 건너뜀: 무엇을 왜 건너뛰었는지
- 사람이 확인할 것: 서버 리포트에서 볼 항목
