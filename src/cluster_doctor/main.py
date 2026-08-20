import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from cluster_doctor.adapter.inbound.rest.router import router

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

app = FastAPI(title="ClusterDoctor", version="0.1.0")


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=400, content={"error": str(exc)})


app.include_router(router)
