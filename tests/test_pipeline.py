"""Data tests: malformed values, unsorted arrival, duplicates, late bars, missing assets, gaps."""
import numpy as np

from backend.pipeline import BarAligner

S = ["AUSDT", "BUSDT"]


def test_malformed_observations_are_rejected():
    a = BarAligner(S)
    assert a.add("AUSDT", 60_000, "abc") == []          # unparseable price
    assert a.add("AUSDT", 60_000, float("nan")) == []   # NaN price
    assert a.add("AUSDT", 60_000, -5) == []             # negative price
    assert a.add("AUSDT", 60_001, 10) == []             # not on a minute boundary
    assert a.add("XYZUSDT", 60_000, 10) == []           # unknown symbol
    assert len(a.rejected) == 5 and a.pending == {}


def test_unsorted_arrival_emits_in_time_order():
    a = BarAligner(S)
    assert a.add("AUSDT", 120_000, 10) == []
    assert a.add("AUSDT", 60_000, 9) == []
    bars = a.add("BUSDT", 60_000, 20)
    assert [b.ts for b in bars] == [60_000]
    bars = a.add("BUSDT", 120_000, 21)
    assert [b.ts for b in bars] == [120_000] and list(bars[0].prices) == [10.0, 21.0]


def test_duplicates_and_late_bars_are_handled():
    a = BarAligner(S)
    a.add("AUSDT", 60_000, 1)
    a.add("AUSDT", 60_000, 2)                  # duplicate before emission: latest wins
    bars = a.add("BUSDT", 60_000, 3)
    assert list(bars[0].prices) == [2.0, 3.0]
    assert a.add("AUSDT", 60_000, 5) == []     # late: minute already emitted
    assert any("late" in r for r in a.rejected)


def test_missing_asset_is_forward_filled_and_flagged():
    a = BarAligner(S)
    a.add("AUSDT", 60_000, 1)
    a.add("BUSDT", 60_000, 2)
    a.add("AUSDT", 120_000, 1.1)
    bars = a.flush(120_000)
    assert bars[0].filled == ["BUSDT"] and bars[0].prices[1] == 2.0


def test_asset_never_seen_gives_nan_and_warning():
    a = BarAligner(S)
    a.add("AUSDT", 60_000, 1)
    bars = a.flush(60_000)
    assert np.isnan(bars[0].prices[1]) and bars[0].warnings


def test_short_gap_is_filled_minute_by_minute():
    a = BarAligner(S)
    a.add("AUSDT", 60_000, 1)
    a.add("BUSDT", 60_000, 2)
    a.add("AUSDT", 240_000, 3)
    bars = a.add("BUSDT", 240_000, 4)
    assert [b.ts for b in bars] == [120_000, 180_000, 240_000]