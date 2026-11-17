---
name: release
description: 검증이 끝난 기능 브랜치를 main에 --no-ff로 병합하고, 버전을 올리고, CHANGELOG와 태그를 남긴다. 사용자가 /release로 부를 때만 쓴다.
argument-hint: "[major|minor|patch]"
disable-model-invocation: true
---

# 릴리스

인자: `$ARGUMENTS` (major·minor·patch 중 하나. 비어 있으면 4단계에서 제안하고 묻는다.)

각 단계의 결과를 짧게 보고한다. "묻는다"라고 적힌 곳에서는 답을 받기 전에 다음으로 가지 않는다.

## 1. 출발 상태 확인

- `git status --short`가 비어 있어야 한다. 커밋 안 된 변경이 있으면 멈추고 알린다.
- 지금 브랜치를 확인한다(`git branch --show-current`).
  - 기능 브랜치(`feat/…`, `fix/…`, `docs/…`)면 2단계부터.
  - `main`이면 병합할 것이 없으므로 4단계부터(이미 병합한 변경을 릴리스할 때).

## 2. 병합 전 검증 (기능 브랜치일 때)

- `verify-change` 스킬 절차로 브랜치 전체를 검증한다.
- `/code-review`로 `main...HEAD` 전체 diff를 리뷰한다.
- 검증 결과, 리뷰 지적, `git log --oneline main..HEAD`를 보여 주고 **병합할지 묻는다**.

## 3. main에 병합

- `git switch main`
- `git merge --no-ff <기능 브랜치> -m "merge <기능 브랜치>: <한 줄 요약>"`
- 충돌이 나면 해결하지 말고 `git merge --abort` 후 멈추고 알린다.
- fast-forward로 병합하지 않는다. 기능 하나를 `git revert -m 1 <병합 커밋>`으로 통째로 되돌릴 수 있게 병합 커밋을 남긴다.

## 4. 버전 결정

- 마지막 버전 태그: `git describe --tags --abbrev=0 --match "v*"`. 없으면 첫 릴리스다.
- 범위의 변경: `git log --oneline <마지막 태그>..HEAD` (첫 릴리스면 전체).
- 규칙(SemVer):
  - **major**: 운영에 영향을 주는 비호환 변경 — `.env` 변수 이름·의미 변경, 리포트 형식·파일명·SFTP 경로 변경, Kafka 메시지 해석 변경.
  - **minor**: 기능 추가·동작 변경 — 선별 기준, 판정 규칙, 리포트 섹션 추가.
  - **patch**: 버그 수정, 문서·테스트만 바뀐 경우.
- 인자가 있으면 그 값이 규칙과 맞는지 확인하고, 어긋나면 이유와 함께 알린다.
- 첫 릴리스면 `pyproject.toml`의 현재 버전을 그대로 태그할지, 올려서 태그할지 묻는다.
- 결정한 새 버전을 보여 주고 **진행할지 묻는다**.

## 5. 버전 올리기

- 먼저 `uv version --bump <major|minor|patch> --dry-run`으로 결과를 확인한다.
- 이상이 없으면 `uv version --bump <major|minor|patch>`를 실행한다.
- 이 명령이 지원되지 않으면 `pyproject.toml`의 `version`을 직접 고치고 `uv lock`을 실행한다.
- 보고서에 찍히는 버전도 같은 값으로 고친다: `src/cluster_doctor/incident_orchestrator_agent/service/report_delivery/projection/report_content.py`의 `REPORT_VERSION`.
- `uv run pytest -q tests/unit/incident_orchestrator_agent/service/test_report_version.py`로 두 값이 같은지 확인한다.
- `git diff --stat`으로 `pyproject.toml`·`report_content.py`(와 `uv.lock`)만 바뀌었는지 확인한다.

## 6. CHANGELOG

- 저장소 루트 `CHANGELOG.md`에 새 버전 단락을 맨 위에 추가한다. 파일이 없으면 만든다.
- 커밋 메시지를 그대로 옮기지 않고, 운영자가 읽을 문장으로 정리한다. 형식:

```markdown
## [X.Y.Z] - YYYY-MM-DD

### 추가
### 변경
### 수정
### 운영 영향
- (.env 변수, 리포트 형식, SFTP 경로, 배포 시 해야 할 일. 없으면 "없음")
```

- 비어 있는 소제목은 지운다. "운영 영향"은 비어 있어도 "없음"으로 남긴다.

## 7. 커밋과 태그

- `git add pyproject.toml uv.lock CHANGELOG.md src/cluster_doctor/incident_orchestrator_agent/service/report_delivery/projection/report_content.py`
- `git commit -m "chore: vX.Y.Z 릴리스"`
- `git tag -a vX.Y.Z -m "vX.Y.Z"` (주석 태그. 메시지 본문에 CHANGELOG 단락을 넣어도 된다.)

## 8. push (묻고 나서만)

- **push할지 묻는다.** 답을 받으면:
  - `git push origin main`
  - `git push origin vX.Y.Z`
- force push는 하지 않는다. 거절되면 멈추고 알린다.

## 9. 보고

- 새 버전, 태그, 병합 커밋 해시
- 서버에 배포할 태그(`vX.Y.Z`)와 CHANGELOG의 "운영 영향"
- 배포 뒤 사람이 서버 리포트로 확인할 것
- 되돌리는 방법: `git revert -m 1 <병합 커밋>` 뒤 patch 릴리스

## 하지 않는 것

- 배포는 하지 않는다. 서버 배포는 사용자가 한다.
- 실제 분석·Kafka·SFTP 진입점은 실행하지 않는다(hook이 막는다).
- 이미 push한 태그를 지우거나 옮기지 않는다. 잘못됐으면 새 patch 버전을 낸다.
