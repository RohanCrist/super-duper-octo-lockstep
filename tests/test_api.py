"""REST, WebSocket delivery, SQLite persistence and live -> replay fallback (no browser needed).
Stop the dev server before running: each test starts the app with its own temporary database."""
import time

import pytest
from fastapi.testclient import TestClient

from backend import config
from backend.main import app
from backend.storage import Storage

needs_data = pytest.mark.skipif(not (config.DATA_DIR / "aug2024.json").exists(),
                                reason="run: python -m backend.fetch_replay aug2024")


def wait_for(fetch, condition, tries=50, delay=0.2):
    for _ in range(tries):
        value = fetch()
        if condition(value):
            return value
        time.sleep(delay)
    return value


@needs_data
def test_rest_websocket_and_sqlite(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "test.db")
    with TestClient(app) as client:
        health = client.get("/api/health").json()
        assert health["ok"] and health["mode"] == "replay" and health["storage_ok"]
        state = client.get("/api/state").json()
        assert state["status"] in ("NORMAL", "WARNING", "CONTAGION")
        assert len(state["correlation"]) == state["n_assets"]
        with client.websocket_connect("/ws/stream") as ws:
            hello = ws.receive_json()
            assert hello["type"] == "hello" and hello["state"]["ts"] == state["ts"]
            assert client.post("/api/replay", json={"action": "autopause", "enabled": False}).json()["autopause"] is False
            assert client.post("/api/replay", json={"action": "speed", "speed": 60}).status_code == 200
            assert client.post("/api/replay", json={"action": "play"}).json()["playing"] is True
            seen = set()
            for _ in range(1000):
                msg = ws.receive_json()
                if msg["type"] == "snapshot":
                    seen.add(msg["status"])
                if "CONTAGION" in seen:
                    break
            assert {"WARNING", "CONTAGION"} <= seen
        events = wait_for(lambda: client.get("/api/events").json(),
                          lambda evs: any(e["to"] == "CONTAGION" for e in evs))
        assert any(e["to"] == "CONTAGION" for e in events)          # persisted in SQLite
        assert client.post("/api/replay", json={"action": "speed", "speed": 7}).status_code == 400


@needs_data
def test_autopause_holds_each_status_change(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "test.db")
    with TestClient(app) as client:
        with client.websocket_connect("/ws/stream") as ws:
            ws.receive_json()                                            # hello
            client.post("/api/replay", json={"action": "speed", "speed": 60})
            assert client.post("/api/replay", json={"action": "play"}).json()["autopause"] is True
            last_status = None
            for _ in range(500):
                msg = ws.receive_json()
                if msg["type"] == "snapshot":
                    last_status = msg["status"]
                if msg["type"] == "session" and not msg["playing"] and last_status:
                    break
        assert last_status == "WARNING"
        assert client.get("/api/state").json()["time"] == "2024-08-05T00:19:00Z"


@needs_data
def test_live_failure_falls_back_to_replay(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr(config, "REST_BASE", "http://127.0.0.1:9")    # nothing listens here
    with TestClient(app) as client:
        client.post("/api/mode", json={"mode": "live"})
        events = wait_for(lambda: client.get("/api/events").json(),
                          lambda evs: any("switched to replay" in e["message"] for e in evs))
        assert any("switched to replay" in e["message"] for e in events)
        assert client.get("/api/health").json()["mode"] == "replay"


def test_storage_degrades_to_memory(tmp_path):
    store = Storage(tmp_path / "missing_dir" / "x.db")
    assert store.ok is False
    store.save_event(1, {"ts": 1, "kind": "system", "level": "info", "message": "kept in memory"})
    assert store.recent_events()[-1]["message"] == "kept in memory"