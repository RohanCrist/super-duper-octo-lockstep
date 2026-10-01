"""Data validation + timestamp alignment: one gate for live and replay.

Observations (symbol, open_time_ms, close) arrive in any order. BarAligner rejects bad ones,
groups them by minute and emits one Bar per minute, in time order, with every asset present.
A missing asset is forward-filled for at most config.MAX_FFILL_BARS minutes and flagged.
"""
import logging
import math
from collections import deque
from dataclasses import dataclass, field

import numpy as np

from . import config

MINUTE = 60_000
log = logging.getLogger("lockstep.pipeline")


@dataclass
class Bar:
    ts: int                                      # bar OPEN time, ms UTC (Binance convention)
    prices: np.ndarray                           # close prices in basket order
    filled: list = field(default_factory=list)   # symbols forward-filled in this bar
    warnings: list = field(default_factory=list)


def clean(symbol, ts, price, known):
    """Validate one observation -> ((symbol, ts, price), None) or (None, reason)."""
    if symbol not in known:
        return None, f"unknown symbol {symbol!r}"
    try:
        ts, price = int(ts), float(price)
    except (TypeError, ValueError):
        return None, f"{symbol}: unparseable ts={ts!r} price={price!r}"
    if ts % MINUTE:
        return None, f"{symbol}: timestamp {ts} is not on a minute boundary"
    if not math.isfinite(price) or price <= 0:
        return None, f"{symbol}: invalid price {price!r} at {ts}"
    return (symbol, ts, price), None


class BarAligner:
    def __init__(self, symbols, max_ffill=config.MAX_FFILL_BARS):
        self.symbols = list(symbols)
        self.index = {s: i for i, s in enumerate(self.symbols)}
        self.max_ffill = max_ffill
        self.pending = {}                       # open_time -> {symbol: price}
        self.last_price = {}                    # symbol -> last good price
        self.stale = {s: 0 for s in self.symbols}
        self.last_ts = None                     # open time of the last emitted bar
        self.rejected = deque(maxlen=100)       # recent rejection reasons (shown in /api/health)

    def _reject(self, reason):
        self.rejected.append(reason)
        log.warning("rejected: %s", reason)

    def add(self, symbol, ts, price):
        """Feed one observation. Returns the Bars that became complete (often an empty list)."""
        obs, reason = clean(symbol, ts, price, self.index)
        if reason:
            self._reject(reason)
            return []
        symbol, ts, price = obs
        if self.last_ts is not None and ts <= self.last_ts:
            self._reject(f"{symbol}: late or duplicate bar {ts} (emitted up to {self.last_ts})")
            return []
        self.pending.setdefault(ts, {})[symbol] = price       # duplicate in pending: latest wins
        out = []
        while self.pending:
            first = min(self.pending)
            if len(self.pending[first]) < len(self.symbols):
                break
            out += self._emit(first)
        return out

    def flush(self, upto_ts):
        """Emit every pending minute with open time <= upto_ts, forward-filling missing assets."""
        out = []
        while self.pending and min(self.pending) <= upto_ts:
            out += self._emit(min(self.pending))
        return out

    def _emit(self, ts):
        row = self.pending.pop(ts)
        bars = []
        if self.last_ts is not None:
            gap = (ts - self.last_ts) // MINUTE - 1
            if gap > self.max_ffill:
                self._reject(f"gap of {gap} minutes before {ts}: not filled")
            else:
                for _ in range(gap):
                    bars.append(self._make_bar(self.last_ts + MINUTE, {}))
        bars.append(self._make_bar(ts, row))
        return bars

    def _make_bar(self, ts, row):
        prices = np.empty(len(self.symbols))
        filled, warnings = [], []
        for s, i in self.index.items():
            if s in row:
                prices[i] = row[s]
                self.last_price[s] = row[s]
                self.stale[s] = 0
            elif s in self.last_price:
                prices[i] = self.last_price[s]
                self.stale[s] += 1
                filled.append(s)
                if self.stale[s] > self.max_ffill:
                    warnings.append(f"{s}: no fresh price for {self.stale[s]} min")
            else:
                prices[i] = np.nan
                filled.append(s)
                warnings.append(f"{s}: no price received yet")
        self.last_ts = ts
        return Bar(ts, prices, filled, warnings)


def run_minute(engine, aligner, ts, observations):
    """Feed one minute of observations through aligner + engine -> list of snapshots.
    Used by replay, live backfill, gap-fill and the validation harness (one code path)."""
    bars = []
    for symbol, price in observations:
        bars += aligner.add(symbol, ts, price)
    bars += aligner.flush(ts)
    snaps = (engine.push(bar) for bar in bars)
    return [s for s in snaps if s is not None]