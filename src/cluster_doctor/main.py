import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from cluster_doctor.adapter.inbound.rest.router import router
from cluster_doctor.config.dependencies import close_clickhouse_client
from cluster_doctor.config.settings import get_settings
from cluster_doctor.domain.model.time_range import InvalidTimeRangeError

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_HANDLER_MARKER = "_cluster_doctor_owned_handler"
_SERVER_PORT = 8082


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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Resolve configuration once, at startup.

    Settings used to be built lazily inside the request path, so a
    misconfigured deployment surfaced as a per-request validation error
    instead of a failed boot. Validating here makes a bad ``.env`` crash the
    process before it ever serves traffic. This runs only when the app is
    actually started (uvicorn, or ``with TestClient(app)``) -- importing the
    module does not trigger it.

    The secret-scrubbing guard itself lives in ``get_settings`` rather than
    here, so callers that never go through this lifespan are protected too.
    """
    get_settings()
    yield
    close_clickhouse_client()


app = FastAPI(title="ClusterDoctor", version="0.1.0", lifespan=lifespan)


@app.exception_handler(InvalidTimeRangeError)
async def invalid_time_range_handler(request: Request, exc: InvalidTimeRangeError):
    """Map only the domain's invalid-range rejection to 400.

    Deliberately narrow: handling bare ``ValueError`` here also caught
    ``pydantic.ValidationError`` (which embeds the parsed ``.env``) and
    ``json.JSONDecodeError``, leaking internals to unauthenticated callers.
    Everything else propagates and becomes a detail-free 500.
    """
    return JSONResponse(status_code=400, content={"error": str(exc)})


app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=_SERVER_PORT)
