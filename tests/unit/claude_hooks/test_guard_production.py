import importlib.util
from pathlib import Path

import pytest

_HOOK_PATH = Path(__file__).resolve().parents[3] / ".claude" / "hooks" / "guard_production.py"
_spec = importlib.util.spec_from_file_location("guard_production", _HOOK_PATH)
guard = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(guard)


@pytest.mark.parametrize(
    "command",
    [
        "uv run python -m cluster_doctor.main",
        "python scripts/run_analysis.py --dry-run; python scripts/run_analysis.py",
        "python scripts/run_analysis.py --dry-run && python scripts/run_analysis.py --since 10m",
        'python -c "from cluster_doctor import main; main.run()"',
        "python scripts/produce_test_message.py",
        "python scripts/upload_report_sftp.py",
        "uv run python scripts/evaluate_report_prompts.py --mode live",
        "uv run python scripts/evaluate_report_prompts.py --mode=live",
        "cat .env",
        "sed -n p .env",
        "awk 1 .env",
        "python -c \"print(open('.env').read())\"",
        "Get-Content C:\\repo\\.env",
        "cat .env.local",
        "type .env.production",
    ],
)
def test_blocks_production_commands(command):
    assert guard.check(command) is not None


@pytest.mark.parametrize(
    "command",
    [
        "uv run pytest -q",
        "uv run python scripts/run_analysis.py --dry-run",
        "uv run python scripts/preview_report.py --variant default",
        "uv run python scripts/evaluate_report_prompts.py",
        "cat .env.example",
        "ls .venv311",
        'python -c "import os; print(os.environ.get(\'PATH\'))"',
        "git status --short",
    ],
)
def test_allows_local_commands(command):
    assert guard.check(command) is None
