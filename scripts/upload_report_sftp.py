"""REPORT_SFTP_* 설정으로 리포트 한 건을 올려 접속·인증·권한을 확인한다.

    uv run python scripts/upload_report_sftp.py                 # 가장 최근 리포트
    uv run python scripts/upload_report_sftp.py reports/x.html  # 지정한 파일
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cluster_doctor.bootstrap.configuration.settings import get_settings  # noqa: E402
from cluster_doctor.bootstrap.dependency.wiring import build_sftp_uploader  # noqa: E402
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.sftp.sftp_uploader import (  # noqa: E402
    SftpUploadError,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", nargs="?", type=Path)
    args = parser.parse_args()

    settings = get_settings()
    if not settings.report_sftp_enabled:
        print("REPORT_SFTP_HOST가 비어 있어 SFTP가 꺼져 있습니다.", file=sys.stderr)
        return 2

    path = args.file
    if path is None:
        candidates = sorted(Path(settings.report_dir).glob("report-*.html"))
        if not candidates:
            print(f"{settings.report_dir}에 올릴 리포트가 없습니다.", file=sys.stderr)
            return 2
        path = candidates[-1]

    uploader = build_sftp_uploader(settings, attempts=1)
    try:
        remote = uploader.upload(path)
    except SftpUploadError as exc:
        print(f"업로드 실패: {exc}", file=sys.stderr)
        return 1
    print(f"업로드 완료: {path} -> {settings.report_sftp_host}:{remote}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
