import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from cluster_doctor.adapter.inbound.rest.router import router

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_HANDLER_MARKER = "_cluster_doctor_owned_handler"


def configure_logging() -> None:
    """Attach ClusterDoctor's file/stream handlers to the root logger.

    ``logging.basicConfig()`` is documented to be a no-op once the root
    logger already has handlers. Under pytest, the logging plugin installs
    its own root handlers before this module is imported, so relying on
    ``basicConfig`` silently drops the file handler in that environment.

    This attaches the handlers explicitly instead, and marks them so a
    second import/call does not stack duplicate handlers. Any pre-existing
    root handlers (pytest's ``LogCaptureHandler`` included) are left alone.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    already_configured = any(
        getattr(handler, _HANDLER_MARKER, False) for handler in root_logger.handlers
    )
    if already_configured:
        return

    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter(_LOG_FORMAT)

    file_handler = logging.FileHandler("logs/app.log", encoding="utf-8")
    stream_handler = logging.StreamHandler()
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        setattr(handler, _HANDLER_MARKER, True)
        root_logger.addHandler(handler)


configure_logging()

app = FastAPI(title="ClusterDoctor", version="0.1.0")


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=400, content={"error": str(exc)})


app.include_router(router)
