"""Lockstep risk engine (pure NumPy). The same object serves live and replay.

prices -> log returns -> correlation (120-minute window) -> eigen-decomposition
-> absorption ratio + effective bets -> z-score vs the basket's previous 24 hours
-> status (dual rule + hysteresis) -> suggested exposure -> leading assets.
Formulas and thresholds: lockstep-script-v5, technical reference.
"""
import math
from collections import deque
from datetime import datetime, timezone

import numpy as np

from . import config

MINUTE = 60_000
LEVEL_NAME = {0: "NORMAL", 1: "WARNING", 2: "CONTAGION"}


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def num(x, digits=4):
    """JSON-safe rounding: None for missing or non-finite values (browsers cannot parse NaN)."""
    if x is None:
        return None
    x = float(x)
    return round(x, digits) if math.isfinite(x) else None


def short(symbol):
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def log_returns(prices):
    """(T+1, N) prices -> (T, N) log returns."""
    return np.diff(np.log(prices), axis=0)


def correlation_matrix(returns):
    """Standardise each column, then C = X^T X / W (Round 1 definition)."""
    x = (returns - returns.mean(axis=0)) / (returns.std(axis=0) + 1e-12)
    return x.T @ x / returns.shape[0]


def eigen_spectrum(corr):
    """Eigenvalues (descending, clipped at 0) and matching eigenvectors of a symmetric matrix."""
    lam, vec = np.linalg.eigh(corr)
    return np.clip(lam[::-1], 0.0, None), vec[:, ::-1]


def absorption_ratio(lam):
    """Share of all movement explained by the strongest common factor: lambda_1 / sum(lambda)."""
    return float(lam[0] / lam.sum())


def effective_bets(lam):
    """exp(entropy of eigenvalue shares): N when assets are independent, 1 when they move as one."""
    p = lam / lam.sum()
    p = p[p > 0]
    return float(np.exp(-(p * np.log(p)).sum()))


def z_score(value, history):
    """How unusual the current absorption ratio is versus this basket's previous 24 hours."""
    h = np.asarray(history, dtype=float)
    return float((value - h.mean()) / (h.std() + 1e-9))


def suggested_exposure(z):
    """1 - sigmoid((z - 2) / 0.5): ~100% when calm, 50% at z = 2, towards 0% as z rises."""
    if z is None:
        return 1.0
    x = max(-50.0, min(50.0, (z - config.EXPOSURE_MID) / config.EXPOSURE_SCALE))
    return 1.0 - 1.0 / (1.0 + math.exp(-x))


def leading_assets(vec1, symbols, k=config.N_LEADERS):
    """Top-k assets by |loading| on the first eigenvector (the shared move)."""
    order = np.argsort(-np.abs(vec1))[:k]
    return [{"symbol": symbols[i], "short": short(symbols[i]), "loading": num(abs(vec1[i]), 3)}
            for i in order]


def triggers(z, bets):
    active = []
    if z > config.WARN_Z:
        active.append("unusual_comovement")
    if bets < config.WARN_BETS:
        active.append("few_bets")
    return active


def explain(status, bets, n):
    b = "—" if bets is None else f"{bets:.1f}"
    if status == "NORMAL":
        return f"Your {n} assets are behaving like {b} independent bets — normal for this basket. Diversification is working."
    if status == "WARNING":
        return f"Your {n} assets are starting to move together: they now behave like {b} independent bets. Diversification is weakening."
    if status == "CONTAGION":
        return f"Your {n} assets are moving almost as one ({b} independent bets). Diversification has largely stopped working."
    return "Collecting history: Lockstep needs 26 hours of 1-minute prices to learn what is normal for this basket."


class StatusMachine:
    """Dual rule with hysteresis, identical to the Round 1 engine."""

    def __init__(self):
        self.level = 0

    def step(self, z, bets):
        c = config
        if z > c.PANIC_Z or bets < c.PANIC_BETS:
            lvl = 2
        elif z > c.WARN_Z or bets < c.WARN_BETS:
            lvl = 1
        else:
            lvl = 0
        s = self.level
        if lvl >= s:
            s = lvl
        elif s == 2 and (z > c.EXIT_Z or bets < c.WARN_BETS):
            s = 1                                   # easing from CONTAGION, still elevated
        elif s == 1 and (z > c.EXIT_Z or bets < c.WARN_BETS + c.BETS_EXIT_MARGIN):
            s = 1                                   # hold WARNING until clearly calm
        else:
            s = lvl
        self.level = s
        return s


class RiskEngine:
    def __init__(self, symbols, window=config.WINDOW, baseline=config.BASELINE):
        self.symbols = list(symbols)
        self.shorts = [short(s) for s in self.symbols]
        self.n = len(self.symbols)
        self.window, self.baseline = window, baseline
        self.warmup_need = window + baseline + 1
        self.prices = deque(maxlen=max(window, config.BASKET_HORIZON) + 1)
        self.ar_history = deque(maxlen=baseline)
        self.machine = StatusMachine()
        self.status = "WARMING_UP"
        self.bars = 0

    def push(self, bar):
        """Process one aligned 1-minute bar -> snapshot dict (None if the bar is unusable)."""
        prices = np.asarray(bar.prices, dtype=float)
        if prices.shape != (self.n,) or not np.all(np.isfinite(prices)) or np.any(prices <= 0):
            return None
        self.prices.append(prices)
        self.bars += 1
        previous = self.status if self.bars > 1 else None
        close_ts = int(bar.ts) + MINUTE                     # label = bar CLOSE time
        P = np.array(self.prices)
        snap = {
            "ts": close_ts, "time": iso(close_ts), "bar_open": int(bar.ts),
            "n_assets": self.n, "symbols": self.symbols, "short_symbols": self.shorts,
            "effective_bets": None, "absorption_ratio": None, "z_score": None, "exposure": 1.0,
            "avg_correlation": None, "basket_return_60m": None, "leaders": [], "correlation": None,
            "triggers": [], "filled": list(bar.filled), "data_warnings": list(bar.warnings),
            "warmup": {"have": min(self.bars, self.warmup_need), "need": self.warmup_need},
        }
        if len(P) > config.BASKET_HORIZON:                  # equal-weight basket, last 60 minutes
            snap["basket_return_60m"] = num(np.mean(np.log(P[-1] / P[-1 - config.BASKET_HORIZON])), 5)
        if len(P) >= self.window + 1:
            corr = correlation_matrix(log_returns(P[-(self.window + 1):]))
            lam, vec = eigen_spectrum(corr)
            ar, bets = absorption_ratio(lam), effective_bets(lam)
            z = z_score(ar, self.ar_history) if len(self.ar_history) == self.baseline else None
            self.ar_history.append(ar)
            snap.update({
                "effective_bets": num(bets), "absorption_ratio": num(ar), "z_score": num(z),
                "avg_correlation": num((corr.sum() - self.n) / max(1, self.n * (self.n - 1))),
                "leaders": leading_assets(vec[:, 0], self.symbols),
                "correlation": np.round(corr, 2).tolist(),
            })
            if z is not None:
                level = self.machine.step(z, bets)
                self.status = LEVEL_NAME[level]
                snap["exposure"] = num(suggested_exposure(z))
                snap["triggers"] = triggers(z, bets) or (["holding_until_clear"] if level else [])
        snap["status"] = self.status
        snap["previous_status"] = previous
        snap["explanation"] = explain(self.status, snap["effective_bets"], self.n)
        return snap