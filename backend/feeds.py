"""Where prices come from: saved replay files and the Binance public market-data API.

Both paths produce plain (symbol, open_time_ms, close) observations for pipeline.BarAligner,
so live and replay share every step after this file. No API key is used anywhere.
"""
import json
import ssl
import urllib.request
from datetime import datetime

from . import config

MINUTE = 60_000

try:                                    # macOS python.org builds often lack system CA certificates
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CONTEXT = ssl.create_default_context()


def parse_iso(text):
    """'2024-08-04T23:30:00Z' -> epoch milliseconds (UTC)."""
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def load_replay(dataset_id):
    """Read data/<id>.json (Round 1 format: {"data": {symbol: [[open_ms, close], ...]}})
    -> (symbols, [(open_time_ms, [(symbol, close), ...]), ...]) sorted by time."""
    path = config.DATA_DIR / f"{dataset_id}.json"
    with open(path) as f:
        raw = json.load(f)
    series = raw["data"]
    symbols = [s for s in config.REPLAY_SYMBOLS if s in series]
    symbols += [s for s in series if s not in config.REPLAY_SYMBOLS]
    by_minute = {}
    for s in symbols:
        for ts, close in series[s]:
            by_minute.setdefault(int(ts), []).append((s, close))
    return symbols, sorted(by_minute.items())


def get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "lockstep/1.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
        return json.loads(resp.read().decode())


def fetch_klines(symbol, start_ms, end_ms):
    """Closed 1-minute klines with open time in [start_ms, end_ms] -> [(open_time_ms, close_str)].
    GET /api/v3/klines returns at most 1000 rows per call, so we page forward."""
    out, cursor = [], start_ms
    while cursor <= end_ms:
        url = (f"{config.REST_BASE}/api/v3/klines?symbol={symbol}&interval=1m"
               f"&startTime={cursor}&endTime={end_ms}&limit=1000")
        rows = get_json(url)
        if not rows:
            break
        out.extend((int(r[0]), r[4]) for r in rows)      # r[0] open time, r[4] close
        cursor = int(rows[-1][0]) + MINUTE
        if len(rows) < 1000:
            break
    return out


def stream_url(symbols):
    """Combined-stream URL: one WebSocket carrying the 1-minute klines of every symbol."""
    streams = "/".join(f"{s.lower()}@kline_1m" for s in symbols)
    return f"{config.WS_BASE}/stream?streams={streams}"


def parse_kline_message(text):
    """Binance kline message -> (symbol, open_time_ms, close) when the candle is CLOSED ("x": true),
    else None. Combined streams wrap the payload as {"stream": ..., "data": {...}}."""
    try:
        msg = json.loads(text)
    except (TypeError, ValueError):
        return None
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    k = data.get("k") if isinstance(data, dict) else None
    if not isinstance(k, dict) or not k.get("x"):
        return None
    return k.get("s"), k.get("t"), k.get("c")
