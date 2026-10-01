"""Validation harness (the Round 1 'self-check'): replays each saved dataset through the SAME
aligner + engine and scores it with the Round 1 definitions. pandas holds the per-minute table.

Crash start = first minute (within 6 h before the worst hour) where the equal-weight basket is
down more than 3% over 60 minutes. Lead = crash start minus the start of the WARNING episode that
is active at crash start (if none is active: the first WARNING after it, giving a negative lead). Round 1 names that minute by bar OPEN time, status changes by
bar CLOSE time; both conventions are kept so leads match Round 1 (25 / 129 / -244 minutes).
"""
import numpy as np
import pandas as pd

from . import config
from .feeds import load_replay
from .pipeline import MINUTE, BarAligner, run_minute
from .risk_engine import RiskEngine, iso

LEVEL = {"WARMING_UP": 0, "NORMAL": 0, "WARNING": 1, "CONTAGION": 2}


def run_dataset(dataset_id):
    """Whole replay file -> DataFrame, one row per minute (ts = bar close time)."""
    symbols, minutes = load_replay(dataset_id)
    engine, aligner = RiskEngine(symbols), BarAligner(symbols)
    rows = []
    for ts, observations in minutes:
        for s in run_minute(engine, aligner, ts, observations):
            rows.append((s["ts"], s["status"], s["effective_bets"], s["exposure"], s["z_score"],
                         s["basket_return_60m"]))
    df = pd.DataFrame(rows, columns=["ts", "status", "bets", "exposure", "z", "basket_60m"])
    df["level"] = df["status"].map(LEVEL)
    return df


def episodes(level, at_least):
    """[(start_index, end_index_exclusive), ...] of consecutive minutes with level >= at_least."""
    on = np.asarray(level) >= at_least
    if len(on) == 0:
        return []
    prev = np.r_[False, on[:-1]]
    starts = np.flatnonzero(on & ~prev)
    ends = np.flatnonzero(~on & prev)
    if len(ends) < len(starts):
        ends = np.r_[ends, len(on)]
    return list(zip(starts.tolist(), ends.tolist()))


def score(dataset_id, df):
    meta = config.DATASETS[dataset_id]
    ts = df["ts"].to_numpy(dtype=np.int64)
    basket = df["basket_60m"].to_numpy(dtype=float)
    warn_ep, panic_ep = episodes(df["level"], 1), episodes(df["level"], 2)
    out = {"dataset": dataset_id, "label": meta["label"], "role": meta["role"],
           "warning_episodes": len(warn_ep), "contagion_episodes": len(panic_ep)}
    if not np.isfinite(basket).any():
        return out
    worst = int(np.nanargmin(basket))
    out["worst_60m"] = round(float(basket[worst]), 4)
    out["worst_at"] = iso(int(ts[worst]) - MINUTE)
    if meta["role"] != "crash":
        return out
    lo = max(0, worst - 360)
    cs = next((i for i in range(lo, worst + 1) if basket[i] < config.CRASH_THRESHOLD), worst)
    crash_ts = int(ts[cs]) - MINUTE
    # Round 1 rule: the WARNING episode active at crash start counts; otherwise the first one after it.
    active = [a for a, b in warn_ep if ts[a] <= crash_ts and (b >= len(ts) or ts[b] > crash_ts)]
    first = active[0] if active else next((a for a, _ in warn_ep if ts[a] > crash_ts), None)
    out["crash_start"] = iso(crash_ts)
    if first is None:
        out.update({"first_warning": None, "lead_minutes": None, "outcome": "never warned"})
    else:
        lead = (crash_ts - int(ts[first])) // MINUTE
        out.update({"first_warning": iso(int(ts[first])), "lead_minutes": int(lead),
                    "outcome": "warned before" if lead > 0 else "late — a miss"})
    return out


def summarize_all():
    rows = []
    for dataset_id, meta in config.DATASETS.items():
        if not (config.DATA_DIR / f"{dataset_id}.json").exists():
            rows.append({"dataset": dataset_id, "label": meta["label"], "role": meta["role"],
                         "error": f"missing data file — run: python -m backend.fetch_replay {dataset_id}"})
            continue
        rows.append(score(dataset_id, run_dataset(dataset_id)))
    return rows