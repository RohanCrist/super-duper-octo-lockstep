"""Replay determinism + Round 1 regression on the real 5 Aug 2024 Binance data."""
import pytest

from backend import config
from backend.feeds import load_replay, parse_iso
from backend.validation import run_dataset

needs_data = pytest.mark.skipif(not (config.DATA_DIR / "aug2024.json").exists(),
                                reason="run: python -m backend.fetch_replay aug2024")


@needs_data
def test_replay_is_deterministic():
    assert run_dataset("aug2024").equals(run_dataset("aug2024"))


@needs_data
def test_aug2024_matches_round1():
    symbols, _ = load_replay("aug2024")
    if symbols != config.REPLAY_SYMBOLS:
        pytest.skip(f"replay file has {len(symbols)} of the 12 Round 1 symbols")
    df = run_dataset("aug2024").set_index("ts")
    at = lambda t: df.loc[parse_iso(t)]
    assert at("2024-08-05T00:18:00Z")["status"] == "NORMAL"
    warning = at("2024-08-05T00:19:00Z")
    assert warning["status"] == "WARNING"
    assert abs(warning["bets"] - 2.7) < 0.06 and abs(warning["exposure"] - 0.68) < 0.01
    contagion = at("2024-08-05T01:11:00Z")
    assert contagion["status"] == "CONTAGION"
    assert abs(contagion["bets"] - 1.72) < 0.02 and abs(contagion["exposure"] - 0.26) < 0.01


@pytest.mark.skipif(not all((config.DATA_DIR / f"{d}.json").exists() for d in config.DATASETS),
                    reason="run: python -m backend.fetch_replay")
def test_self_check_matches_round1():
    from backend.validation import summarize_all
    rows = {r["dataset"]: r for r in summarize_all()}
    assert rows["aug2024"]["lead_minutes"] == 25
    assert rows["may2021"]["lead_minutes"] == 129
    assert rows["mar2020"]["lead_minutes"] == -244          # the miss: 4 h after
    assert (rows["calm2024"]["warning_episodes"], rows["calm2024"]["contagion_episodes"]) == (4, 0)