"""Lockstep orchestration: one engine, two feeds (replay | live), one WebSocket hub.

Replay and live both call pipeline.run_minute -> RiskEngine.push -> _publish, so the
dashboard, storage and alerts do not depend on where the prices came from.
"""
import asyncio
import logging
import os
import time
from collections import deque

import websockets

from . import config
from .feeds import SSL_CONTEXT, fetch_klines, load_replay, parse_iso, parse_kline_message, stream_url
from .pipeline import MINUTE, BarAligner, run_minute
from .risk_engine import RiskEngine, iso
from .storage import Storage

log = logging.getLogger("lockstep.service")
HISTORY_FIELDS = ("ts", "time", "status", "effective_bets", "z_score", "exposure", "basket_return_60m")


def history_point(snap):
    return {k: snap.get(k) for k in HISTORY_FIELDS}


def status_event(snap):
    """Alert for a status change (or for the end of warm-up)."""
    to, frm = snap["status"], snap["previous_status"]
    bets, exposure = snap["effective_bets"] or 0.0, snap["exposure"] or 0.0
    leaders = [x["short"] for x in snap["leaders"]]
    if frm == "WARMING_UP":
        return {"kind": "system", "level": "info", "leaders": leaders,
                "message": f"History ready — monitoring started. Status {to} ({bets:.1f} independent bets)."}
    if to == "WARNING" and frm == "CONTAGION":
        msg = f"Easing to WARNING — {bets:.1f} independent bets. Suggested exposure {exposure:.0%}."
    elif to == "WARNING":
        msg = f"WARNING — assets starting to move together ({bets:.1f} independent bets). Suggested exposure {exposure:.0%}."
    elif to == "CONTAGION":
        msg = f"CONTAGION — assets moving almost as one ({bets:.1f} independent bets). Suggested exposure {exposure:.0%}."
    else:
        msg = f"Back to NORMAL — diversification restored ({bets:.1f} independent bets)."
    level = {"NORMAL": "info", "WARNING": "warning", "CONTAGION": "critical"}[to]
    return {"kind": "status", "level": level, "from": frm, "to": to, "effective_bets": snap["effective_bets"],
            "exposure": snap["exposure"], "z_score": snap["z_score"], "leaders": leaders, "message": msg}


class Hub:
    """Connected browsers. Broadcast drops any socket that fails."""

    def __init__(self):
        self.clients = set()

    async def connect(self, ws):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws):
        self.clients.discard(ws)

    async def broadcast(self, message):
        for ws in list(self.clients):
            try:
                await ws.send_json(message)
            except Exception:
                self.clients.discard(ws)


class LockstepService:
    def __init__(self):
        self.hub = Hub()
        self.storage = self.task = self.lock = self.resume = None
        self.mode, self.feed_state = "replay", "replay"
        self.dataset = self.replay_dataset = config.DEFAULT_DATASET
        self.speed, self.playing, self.finished = config.DEFAULT_SPEED, False, False
        self.autopause = config.REPLAY_AUTOPAUSE
        self._reset()

    def _reset(self):
        self.engine = self.aligner = self.latest = None
        self.history = deque(maxlen=config.HISTORY_KEEP)
        self.events = deque(maxlen=300)
        self.minutes, self.pos, self.start_pos, self.end_pos = [], 0, 0, 0
        self.run_id = int(time.time() * 1000)
        self.symbols = list(config.REPLAY_SYMBOLS)

    # ---------------- lifecycle ----------------
    async def start(self):
        self.storage = Storage(config.DB_PATH)
        self.hub, self.lock, self.resume, self.task = Hub(), asyncio.Lock(), asyncio.Event(), None
        self.speed, self.autopause = config.DEFAULT_SPEED, config.REPLAY_AUTOPAUSE
        try:
            if os.environ.get("LOCKSTEP_MODE") == "live":
                await self.set_mode("live")
            else:
                await self.load_replay(config.DEFAULT_DATASET)
        except FileNotFoundError as e:
            log.error("Replay file missing (%s) - run: python -m backend.fetch_replay", e)
            self.feed_state = "offline"

    async def stop(self):
        await self._cancel()

    async def _cancel(self):
        task, self.task = self.task, None
        self.playing = False
        if self.resume is not None:
            self.resume.clear()
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    # ---------------- messages ----------------
    def session(self, message=None):
        return {"mode": self.mode, "feed_state": self.feed_state, "dataset": self.dataset,
                "datasets": [{"id": k, "label": v["label"], "start": v["start"], "end": v["end"]}
                             for k, v in config.DATASETS.items()],
                "playing": self.playing, "speed": self.speed, "finished": self.finished, "autopause": self.autopause,
                "storage_ok": bool(self.storage and self.storage.ok), "symbols": self.symbols,
                "message": message}

    def hello(self):
        return {"type": "hello", "session": self.session(), "state": self.latest,
                "history": list(self.history), "events": list(self.events)}

    def recent_rejections(self):
        return list(self.aligner.rejected)[-5:] if self.aligner else []

    async def _session(self, message=None):
        await self.hub.broadcast({"type": "session", **self.session(message)})

    def _decorate(self, snap):
        progress = None
        if self.mode == "replay":
            progress = {"index": self.pos - self.start_pos, "total": self.end_pos - self.start_pos}
        return {**snap, "mode": self.mode, "dataset": self.dataset, "progress": progress}

    async def _event(self, event, snap=None):
        ref = snap or self.latest
        ts = ref["ts"] if ref else int(time.time() * 1000)
        full = {"ts": ts, "time": iso(ts), "kind": "system", "level": "info", "from": None, "to": None,
                "effective_bets": None, "exposure": None, "z_score": None, "leaders": [], "message": "",
                **event, "mode": self.mode, "dataset": self.dataset}
        self.events.append(full)
        self.storage.save_event(self.run_id, full)
        await self.hub.broadcast({"type": "event", **full})

    async def _publish(self, snap):
        """Every processed minute, live or replay: remember, store, broadcast, alert."""
        snap = self._decorate(snap)
        self.latest = snap
        self.history.append(history_point(snap))
        storage_was_ok = self.storage.ok
        self.storage.save_snapshot(self.run_id, snap)
        await self.hub.broadcast({"type": "snapshot", **snap})
        if snap["previous_status"] and snap["previous_status"] != snap["status"]:
            await self._event(status_event(snap), snap)
        if storage_was_ok and not self.storage.ok:
            await self._event({"kind": "system", "level": "warning",
                               "message": f"Storage degraded ({self.storage.error}) — continuing in memory."}, snap)
            await self._session()

    # ---------------- replay ----------------
    def _prepare_replay(self, dataset):
        """Worker thread: load the file and process the 26 h before the window silently (warm-up)."""
        symbols, minutes = load_replay(dataset)
        meta = config.DATASETS[dataset]
        start, end = parse_iso(meta["start"]), parse_iso(meta["end"])
        engine, aligner = RiskEngine(symbols), BarAligner(symbols)
        warm, pos = deque(maxlen=config.SEED_HISTORY), 0
        while pos < len(minutes) and minutes[pos][0] + MINUTE <= start:    # bar close <= window start
            warm.extend(run_minute(engine, aligner, *minutes[pos]))
            pos += 1
        stop = pos
        while stop < len(minutes) and minutes[stop][0] + MINUTE <= end:
            stop += 1
        return symbols, minutes, engine, aligner, pos, stop, list(warm)

    async def load_replay(self, dataset, notice=None):
        if dataset not in config.DATASETS:
            raise KeyError(dataset)
        await self._cancel()
        prepared = await asyncio.to_thread(self._prepare_replay, dataset)
        self._reset()
        self.symbols, self.minutes, self.engine, self.aligner, self.pos, self.end_pos, warm = prepared
        self.start_pos = self.pos
        self.mode, self.feed_state, self.dataset, self.replay_dataset = "replay", "replay", dataset, dataset
        self.playing, self.finished = False, False
        for snap in warm:
            self.history.append(history_point(snap))
        self.latest = self._decorate(warm[-1]) if warm else None
        if notice:
            await self._event({"kind": "system", "level": "warning", "message": notice})
        meta = config.DATASETS[dataset]
        await self._event({"kind": "system", "level": "info",
                           "message": f"Replay loaded: {meta['label']}. Starts {meta['start'][11:16]} UTC — press Play."})
        await self.hub.broadcast(self.hello())

    async def _replay_loop(self):
        while self.pos < self.end_pos:
            await self.resume.wait()
            ts, observations = self.minutes[self.pos]
            self.pos += 1
            snaps = run_minute(self.engine, self.aligner, ts, observations)
            for snap in snaps:
                await self._publish(snap)
            changed = any(s["previous_status"] not in (None, "WARMING_UP") and s["previous_status"] != s["status"]
                          for s in snaps)
            if changed and self.autopause and self.pos < self.end_pos:
                self.playing = False                     # hold the new status on screen for the presenter
                self.resume.clear()
                await self._session("Paused on a status change — press Play to continue.")
            await asyncio.sleep(1 / self.speed)
        self.playing, self.finished = False, True
        self.resume.clear()
        await self._event({"kind": "system", "level": "info",
                           "message": "Replay complete — press Restart to run it again."})
        await self._session()

    async def play(self):
        if self.mode != "replay" or self.finished or self.engine is None:
            return
        self.playing = True
        self.resume.set()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._replay_loop())
        await self._session()

    async def pause(self):
        self.playing = False
        self.resume.clear()
        await self._session()

    async def control(self, action, speed=None, dataset=None, enabled=None):
        async with self.lock:
            if action == "load":
                await self.load_replay(dataset)
            elif action == "restart":
                await self.load_replay(self.replay_dataset)
            elif action == "speed":
                if speed not in config.REPLAY_SPEEDS:
                    raise ValueError(f"speed must be one of {config.REPLAY_SPEEDS}")
                self.speed = speed
                await self._session()
            elif action == "play":
                if self.mode != "replay":
                    await self.load_replay(self.replay_dataset)
                await self.play()
            elif action == "pause":
                await self.pause()
            elif action == "autopause":
                self.autopause = bool(enabled)
                await self._session()

    # ---------------- live ----------------
    async def set_mode(self, mode):
        async with self.lock:
            if mode == "replay":
                await self.load_replay(self.replay_dataset)
                return
            await self._cancel()
            self._reset()
            self.mode, self.feed_state, self.dataset = "live", "backfilling", "live"
            self.playing, self.finished = False, False
            self.symbols = list(config.LIVE_SYMBOLS)
            await self.hub.broadcast(self.hello())
            self.task = asyncio.create_task(self._live_main())

    async def _live_main(self):
        try:
            await self._live_backfill()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("live start failed: %s", e)
            await self.load_replay(self.replay_dataset,
                                   notice=f"Live feed unavailable ({e}) — switched to replay.")
            return
        await self._live_stream()

    async def _fetch_all(self, start, end):
        results = await asyncio.gather(*(asyncio.to_thread(fetch_klines, s, start, end) for s in self.symbols),
                                       return_exceptions=True)
        return dict(zip(self.symbols, results))

    async def _live_backfill(self):
        """26 hours of real 1-minute history through the same pipeline, so status is live at once."""
        await self._session("Loading the last 26 hours of 1-minute prices from Binance…")
        last_closed = (int(time.time() * 1000) // MINUTE) * MINUTE - MINUTE
        first = last_closed - (config.WARMUP_BARS + 5) * MINUTE
        results = await self._fetch_all(first, last_closed)
        good = {s: r for s, r in results.items() if not isinstance(r, BaseException) and len(r) > config.WINDOW}
        if not good:
            errors = [r for r in results.values() if isinstance(r, BaseException)]
            raise ConnectionError(f"Binance REST unreachable: {errors[0] if errors else 'no data'}")
        missing = [s for s in self.symbols if s not in good]
        self.symbols = [s for s in self.symbols if s in good]
        self.engine, self.aligner = RiskEngine(self.symbols), BarAligner(self.symbols)
        if missing:
            await self._event({"kind": "system", "level": "warning",
                               "message": f"No recent data for {', '.join(missing)} — live basket reduced to {len(self.symbols)} assets."})
        by_minute = {}
        for s in self.symbols:
            for t, close in good[s]:
                by_minute.setdefault(t, []).append((s, close))
        last = None
        for t in sorted(by_minute):
            for snap in run_minute(self.engine, self.aligner, t, by_minute[t]):
                self.history.append(history_point(snap))
                last = snap
        self.latest = self._decorate(last) if last else None
        self.feed_state = "connecting"
        await self.hub.broadcast(self.hello())

    async def _live_stream(self):
        backoff = 1
        while True:
            reason = "stream closed by server"
            try:
                self.feed_state = "connecting"
                await self._session()
                url = stream_url(self.symbols)
                extra = {"ssl": SSL_CONTEXT} if url.startswith("wss://") else {}
                async with websockets.connect(url, open_timeout=10, ping_interval=20, ping_timeout=20, **extra) as ws:
                    self.feed_state, backoff = "connected", 1
                    await self._session("Live: connected to the Binance 1-minute stream.")
                    await self._gap_fill()
                    flusher = asyncio.create_task(self._flush_loop())
                    try:
                        async for text in ws:
                            kline = parse_kline_message(text)
                            if kline:
                                await self._ingest(*kline)
                    finally:
                        flusher.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
            self.feed_state = "reconnecting"
            await self._event({"kind": "system", "level": "warning",
                               "message": f"Live feed interrupted ({reason}) — reconnecting in {backoff}s. Replay stays available."})
            await self._session()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _ingest(self, symbol, open_ms, close):
        for bar in self.aligner.add(symbol, open_ms, close):
            snap = self.engine.push(bar)
            if snap:
                await self._publish(snap)

    async def _flush_loop(self):
        """Forward-fill an asset whose closed candle has not arrived LIVE_FLUSH_GRACE_S after the minute."""
        while True:
            await asyncio.sleep(2)
            upto = int(time.time() * 1000) - MINUTE - config.LIVE_FLUSH_GRACE_S * 1000
            for bar in self.aligner.flush(upto):
                snap = self.engine.push(bar)
                if snap:
                    await self._publish(snap)

    async def _gap_fill(self):
        """After (re)connecting, fetch the closed minutes missed since the last processed bar."""
        if self.aligner.last_ts is None:
            return
        last_closed = (int(time.time() * 1000) // MINUTE) * MINUTE - MINUTE
        start = self.aligner.last_ts + MINUTE
        if start > last_closed:
            return
        results = await self._fetch_all(start, last_closed)
        by_minute = {}
        for s, rows in results.items():
            if isinstance(rows, BaseException):
                continue
            for t, close in rows:
                by_minute.setdefault(t, []).append((s, close))
        for t in sorted(by_minute):
            for snap in run_minute(self.engine, self.aligner, t, by_minute[t]):
                await self._publish(snap)