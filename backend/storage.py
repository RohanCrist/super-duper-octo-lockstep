"""SQLite persistence for snapshots and alert events.

If SQLite fails (bad path, full disk, locked file) Lockstep keeps running: events stay in
memory, storage_ok becomes False and the dashboard shows a 'storage degraded' banner.
"""
import json
import logging
import sqlite3
from collections import deque

log = logging.getLogger("lockstep.storage")

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    run_id INTEGER, mode TEXT, dataset TEXT, ts INTEGER, status TEXT,
    effective_bets REAL, absorption_ratio REAL, z_score REAL, exposure REAL,
    avg_correlation REAL, basket_return_60m REAL, leaders TEXT,
    PRIMARY KEY (run_id, ts));
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, mode TEXT, dataset TEXT,
    ts INTEGER, time TEXT, kind TEXT, level TEXT, from_status TEXT, to_status TEXT,
    effective_bets REAL, exposure REAL, z_score REAL, leaders TEXT, message TEXT);
"""
EVENT_KEYS = ["ts", "time", "kind", "level", "from", "to", "effective_bets", "exposure",
              "z_score", "leaders", "message", "mode", "dataset"]


class Storage:
    def __init__(self, path):
        self.path, self.ok, self.error, self.conn = str(path), True, None, None
        self.memory_events = deque(maxlen=500)
        try:
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.executescript(SCHEMA)
            self.conn.commit()
        except sqlite3.Error as e:
            self._fail(e)

    def _fail(self, error):
        self.ok, self.error = False, str(error)
        log.error("SQLite unavailable, continuing in memory: %s", error)
        if self.conn is not None:
            try:
                self.conn.close()
            except sqlite3.Error:
                pass
        self.conn = None

    def save_snapshot(self, run_id, s):
        if not self.ok:
            return
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, s.get("mode"), s.get("dataset"), s["ts"], s["status"], s["effective_bets"],
                 s["absorption_ratio"], s["z_score"], s["exposure"], s["avg_correlation"],
                 s["basket_return_60m"], json.dumps([x["short"] for x in s["leaders"]])))
            self.conn.commit()
        except sqlite3.Error as e:
            self._fail(e)

    def save_event(self, run_id, e):
        self.memory_events.append(dict(e))
        if not self.ok:
            return
        try:
            self.conn.execute(
                "INSERT INTO events (run_id, mode, dataset, ts, time, kind, level, from_status, "
                "to_status, effective_bets, exposure, z_score, leaders, message) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, e.get("mode"), e.get("dataset"), e["ts"], e.get("time"), e["kind"],
                 e.get("level"), e.get("from"), e.get("to"), e.get("effective_bets"),
                 e.get("exposure"), e.get("z_score"), json.dumps(e.get("leaders") or []), e["message"]))
            self.conn.commit()
        except sqlite3.Error as err:
            self._fail(err)

    def recent_events(self, limit=100):
        """Newest `limit` events, oldest first. Falls back to memory if SQLite is unavailable."""
        if self.ok:
            try:
                rows = self.conn.execute(
                    "SELECT ts, time, kind, level, from_status, to_status, effective_bets, exposure, "
                    "z_score, leaders, message, mode, dataset FROM events ORDER BY id DESC LIMIT ?",
                    (limit,)).fetchall()
                events = [dict(zip(EVENT_KEYS, r)) for r in reversed(rows)]
                for ev in events:
                    ev["leaders"] = json.loads(ev["leaders"] or "[]")
                return events
            except sqlite3.Error as e:
                self._fail(e)
        return list(self.memory_events)[-limit:]