"""FastAPI app: REST control, the /ws/stream push channel and the static dashboard.
One process, one port: python -m uvicorn backend.main:app --port 8000
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config
from .service import LockstepService
from .validation import summarize_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
service = LockstepService()
_validation_cache = {"rows": None}


@asynccontextmanager
async def lifespan(app):
    await service.start()
    yield
    await service.stop()


app = FastAPI(title="Lockstep", version="1.0", lifespan=lifespan)
# The dashboard is served by this same app (same origin), so CORS is not needed for it; this lets
# other pages and tools call the API during testing.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class ReplayCommand(BaseModel):
    action: Literal["play", "pause", "restart", "speed", "load", "autopause"]
    speed: Optional[int] = None
    dataset: Optional[str] = None
    enabled: Optional[bool] = None


class ModeCommand(BaseModel):
    mode: Literal["live", "replay"]


MISSING_DATA = "replay file missing — run: python -m backend.fetch_replay"


@app.get("/api/health")
async def health():
    return {"ok": True, "mode": service.mode, "feed_state": service.feed_state,
            "storage_ok": bool(service.storage and service.storage.ok),
            "clients": len(service.hub.clients), "rejected_recent": service.recent_rejections()}


@app.get("/api/state")
async def state():
    return service.latest or {}


@app.get("/api/history")
async def history(limit: int = Query(240, ge=1, le=config.HISTORY_KEEP)):
    return list(service.history)[-limit:]


@app.get("/api/events")
async def events(limit: int = Query(100, ge=1, le=1000)):
    return service.storage.recent_events(limit)


@app.get("/api/datasets")
async def datasets():
    return service.session()["datasets"]


@app.get("/api/validation")
async def validation():
    if _validation_cache["rows"] is None:
        _validation_cache["rows"] = await asyncio.to_thread(summarize_all)
    return _validation_cache["rows"]


@app.post("/api/replay")
async def replay(cmd: ReplayCommand):
    try:
        await service.control(cmd.action, speed=cmd.speed, dataset=cmd.dataset, enabled=cmd.enabled)
    except KeyError:
        raise HTTPException(404, f"unknown dataset {cmd.dataset!r}")
    except ValueError as e:
        raise HTTPException(400, str(e))
    except FileNotFoundError:
        raise HTTPException(409, MISSING_DATA)
    return service.session()


@app.post("/api/mode")
async def mode(cmd: ModeCommand):
    try:
        await service.set_mode(cmd.mode)
    except FileNotFoundError:
        raise HTTPException(409, MISSING_DATA)
    return service.session()


@app.websocket("/ws/stream")
async def stream(ws: WebSocket):
    await service.hub.connect(ws)
    try:
        await ws.send_json(service.hello())          # full state: this is what restores a refreshed page
        while True:
            await ws.receive_text()                  # keep the socket open; the browser sends nothing
    except WebSocketDisconnect:
        pass
    finally:
        service.hub.disconnect(ws)


# Mounted last so /api/* and /ws/* routes take priority.
app.mount("/", StaticFiles(directory=str(config.FRONTEND_DIR), html=True), name="dashboard")