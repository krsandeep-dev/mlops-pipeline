"""FastAPI application serving the champion trip-duration model."""

from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from prometheus_fastapi_instrumentator import Instrumentator

from serving.app.config import get_settings
from serving.app.metrics import observe_predictions, record_loaded
from serving.app.model import LoadedModel, coerce, load_with_timeout
from serving.app.schemas import ModelInfo, PredictRequest, PredictResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


class ModelState:
    """Holds the load result. The model loads off the event loop so /health/live
    answers immediately while the artifact download is still in flight."""

    def __init__(self) -> None:
        self.model: LoadedModel | None = None
        self.error: str | None = None


def _load_into(state: ModelState) -> None:
    settings = get_settings()
    try:
        state.model = load_with_timeout(settings)
        record_loaded(state.model.name, state.model.version, state.model.run_id)
        logger.info("model ready: %s v%s", state.model.name, state.model.version)
    except Exception as exc:  # noqa: BLE001 -- see below
        # Deliberately broad: this runs in a supervisor thread, and an exception that
        # escapes it would leave readiness permanently red with no diagnostic. Every
        # failure is recorded and surfaced through /health/ready instead.
        state.error = f"{type(exc).__name__}: {exc}"
        logger.error("model load failed: %s", state.error)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.model_state = ModelState()
    worker = threading.Thread(
        target=_load_into, args=(app.state.model_state,), name="model-load", daemon=True
    )
    worker.start()
    yield


app = FastAPI(title="taxi trip-duration serving", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)


def _state(request: Request) -> ModelState:
    return request.app.state.model_state


@app.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "alive"}


@app.get("/health/ready")
def ready(request: Request):
    state = _state(request)
    if state.model is None:
        raise HTTPException(status_code=503, detail=state.error or "model still loading")
    return {"status": "ready", "version": state.model.version}


@app.get("/model", response_model=ModelInfo)
def model_info(request: Request) -> ModelInfo:
    state = _state(request)
    if state.model is None:
        raise HTTPException(status_code=503, detail=state.error or "model still loading")
    return ModelInfo(
        name=state.model.name, version=state.model.version, run_id=state.model.run_id
    )


@app.post("/predict", response_model=PredictResponse)
def predict(payload: PredictRequest, request: Request) -> PredictResponse:
    state = _state(request)
    if state.model is None:
        raise HTTPException(status_code=503, detail=state.error or "model still loading")

    loaded = state.model
    frame = coerce([record.model_dump() for record in payload.records], loaded.dtypes)
    predictions = [float(value) for value in loaded.pyfunc.predict(frame)]
    observe_predictions(predictions)
    return PredictResponse(
        predictions=predictions,
        model=ModelInfo(name=loaded.name, version=loaded.version, run_id=loaded.run_id),
    )
