"""Lockstep configuration: every threshold, symbol, dataset and endpoint in one place.

SOURCE-BACKED values come from lockstep-script-v5 (technical reference) and the Round 1 deck.
ENGINEERING ASSUMPTION values are marked and can be changed here without touching other code.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FRONTEND_DIR = ROOT / "frontend"
DB_PATH = Path(os.environ.get("LOCKSTEP_DB", str(DATA_DIR / "lockstep.db")))

# SOURCE-BACKED: the 12 Binance USDT pairs used in the Round 1 replays.
REPLAY_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "LTCUSDT",
    "BCHUSDT", "EOSUSDT", "XLMUSDT", "TRXUSDT", "LINKUSDT", "ETCUSDT",
]
# ENGINEERING ASSUMPTION: EOSUSDT may no longer trade on Binance, so the live basket uses SOLUSDT.
LIVE_SYMBOLS = ["SOLUSDT" if s == "EOSUSDT" else s for s in REPLAY_SYMBOLS]

# SOURCE-BACKED method parameters (script v5).
WINDOW = 120                 # 1-minute returns in the correlation window
BASELINE = 1440              # previous absorption-ratio values for the z-score (24 hours)
WARMUP_BARS = WINDOW + BASELINE + 1   # prices needed before the first status
WARN_Z, PANIC_Z, EXIT_Z = 1.5, 2.5, 1.0
WARN_BETS, PANIC_BETS = 2.0, 1.7
BETS_EXIT_MARGIN = 0.2       # Round 1 engine: WARNING holds while bets < 2.0 + 0.2
EXPOSURE_MID, EXPOSURE_SCALE = 2.0, 0.5   # exposure = 1 - sigmoid((z - 2) / 0.5)
N_LEADERS = 3
BASKET_HORIZON = 60          # minutes; basket return and crash definition
CRASH_THRESHOLD = -0.03      # Round 1 crash start: basket 60-min return below -3%

# ENGINEERING ASSUMPTIONS: data hygiene and runtime behaviour.
MAX_FFILL_BARS = 5           # forward-fill a missing asset for at most 5 minutes
LIVE_FLUSH_GRACE_S = 8       # wait this long after a minute closes before forward-filling
HISTORY_KEEP = 720           # snapshots kept in memory for the timeline (12 hours)
SEED_HISTORY = 120           # warm-up snapshots shown on the timeline at replay start
REPLAY_SPEEDS = [1, 2, 5, 10, 30, 60]   # bars per second
DEFAULT_SPEED = 2
REPLAY_AUTOPAUSE = True      # demo control: replay pauses at every status change (toggle in the UI)

# ENGINEERING ASSUMPTION: Binance market-data-only hosts (no API key needed).
REST_BASE = os.environ.get("LOCKSTEP_REST", "https://data-api.binance.vision")
WS_BASE = os.environ.get("LOCKSTEP_WS", "wss://data-stream.binance.vision")
ARCHIVE_URL = "https://data.binance.vision/data/spot/daily/klines/{s}/1m/{s}-1m-{d}.zip"

# Replay datasets: files cover [file_start, file_end); the dashboard shows [start, end].
DATASETS = {
    "aug2024": {"label": "5 Aug 2024 — global sell-off", "role": "crash",
                "file_start": "2024-08-03", "file_end": "2024-08-07",
                "start": "2024-08-04T23:30:00Z", "end": "2024-08-05T03:00:00Z"},
    "may2021": {"label": "19 May 2021 — crypto crash", "role": "crash",
                "file_start": "2021-05-17", "file_end": "2021-05-21",
                "start": "2021-05-19T04:30:00Z", "end": "2021-05-19T14:00:00Z"},
    "mar2020": {"label": "12 Mar 2020 — COVID crash (a miss)", "role": "crash",
                "file_start": "2020-03-10", "file_end": "2020-03-14",
                "start": "2020-03-12T06:00:00Z", "end": "2020-03-12T11:30:00Z"},
    "calm2024": {"label": "8–11 Jul 2024 — quieter days (false-alarm check)", "role": "calm",
                 "file_start": "2024-07-08", "file_end": "2024-07-12",
                 "start": "2024-07-09T03:00:00Z", "end": "2024-07-12T00:00:00Z"},
}
DEFAULT_DATASET = "aug2024"