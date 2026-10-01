"""Download real Binance 1-minute closes for the replay windows into data/<id>.json.

Run from the project root:
    python -m backend.fetch_replay            # all four datasets
    python -m backend.fetch_replay aug2024    # one dataset
Primary source: Binance public data archive (one zip per symbol per day).
Fallback: Binance market-data REST API (/api/v3/klines).
"""
import csv
import io
import json
import sys
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone

from . import config
from .feeds import MINUTE, SSL_CONTEXT, fetch_klines


def archive_day(symbol, day):
    """One UTC day of 1-minute klines from data.binance.vision -> [[open_time_ms, close], ...]."""
    url = config.ARCHIVE_URL.format(s=symbol, d=day.isoformat())
    req = urllib.request.Request(url, headers={"User-Agent": "lockstep/1.0"})
    with urllib.request.urlopen(req, timeout=30, context=SSL_CONTEXT) as resp:
        blob = resp.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        text = zf.read(zf.namelist()[0]).decode()
    rows = []
    for rec in csv.reader(io.StringIO(text)):
        if not rec or not rec[0].strip().isdigit():
            continue                                  # skip a header line if present
        ts = int(rec[0])
        if ts > 10**14:                               # newer archive files use microseconds
            ts //= 1000
        rows.append([ts, float(rec[4])])              # column 4 = close
    return rows


def day_ms(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def download(dataset_id):
    meta = config.DATASETS[dataset_id]
    first = date.fromisoformat(meta["file_start"])
    stop = date.fromisoformat(meta["file_end"])
    data = {}
    for symbol in config.REPLAY_SYMBOLS:
        rows, source = [], "archive"
        try:
            day = first
            while day < stop:
                rows += archive_day(symbol, day)
                day += timedelta(days=1)
        except Exception as archive_error:
            try:
                klines = fetch_klines(symbol, day_ms(first), day_ms(stop) - MINUTE)
                rows = [[t, float(c)] for t, c in klines]
                source = f"REST (archive failed: {archive_error})"
            except Exception as rest_error:
                rows, source = [], f"archive: {archive_error}; REST: {rest_error}"
        if rows:
            data[symbol] = rows
            print(f"  {symbol}: {len(rows)} bars via {source}")
        else:
            print(f"  ! {symbol}: no data — left out ({source})")
    out = {"name": dataset_id, "start": meta["file_start"] + "T00:00",
           "end": meta["file_end"] + "T00:00", "data": data}
    config.DATA_DIR.mkdir(exist_ok=True)
    path = config.DATA_DIR / f"{dataset_id}.json"
    path.write_text(json.dumps(out))
    print(f"wrote {path} ({len(data)} symbols)")


if __name__ == "__main__":
    for dataset_id in (sys.argv[1:] or list(config.DATASETS)):
        print(f"== {dataset_id}")
        download(dataset_id)