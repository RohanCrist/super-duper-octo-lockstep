"""Unit tests for the Round 1 formulas, status rules, hysteresis and warm-up."""
import numpy as np

from backend.pipeline import Bar
from backend.risk_engine import (RiskEngine, StatusMachine, absorption_ratio, correlation_matrix,
                                 effective_bets, eigen_spectrum, suggested_exposure)


def test_independent_assets_give_about_twelve_bets():
    returns = np.random.default_rng(0).standard_normal((5000, 12))
    lam, _ = eigen_spectrum(correlation_matrix(returns))
    assert effective_bets(lam) > 11.5
    assert absorption_ratio(lam) < 0.15


def test_identical_assets_give_one_bet():
    rng = np.random.default_rng(1)
    returns = rng.standard_normal((500, 1)) + 1e-6 * rng.standard_normal((500, 12))
    lam, _ = eigen_spectrum(correlation_matrix(returns))
    assert effective_bets(lam) < 1.05
    assert absorption_ratio(lam) > 0.99


def test_exposure_curve():
    assert abs(suggested_exposure(2.0) - 0.5) < 1e-9
    assert suggested_exposure(0.0) > 0.95 and suggested_exposure(4.0) < 0.05
    assert suggested_exposure(None) == 1.0
    assert 0.0 <= suggested_exposure(-1e6) <= 1.0 and 0.0 <= suggested_exposure(1e6) <= 1.0


def test_dual_rule_and_hysteresis():
    m = StatusMachine()
    assert m.step(0.0, 5.0) == 0      # NORMAL
    assert m.step(1.6, 5.0) == 1      # z > 1.5 -> WARNING
    assert m.step(1.2, 5.0) == 1      # above exit level 1.0 -> WARNING holds
    assert m.step(0.5, 5.0) == 0      # clearly calm -> NORMAL
    assert m.step(0.0, 1.9) == 1      # fewer than 2 bets -> WARNING
    assert m.step(2.6, 3.0) == 2      # z > 2.5 -> CONTAGION
    assert m.step(1.2, 3.0) == 1      # eases to WARNING, not straight to NORMAL


def test_engine_warms_up_then_classifies():
    rng = np.random.default_rng(2)
    engine = RiskEngine([f"A{i}USDT" for i in range(4)], window=20, baseline=30)
    price, snaps = np.full(4, 100.0), []
    for k in range(80):
        price = price * np.exp(0.001 * rng.standard_normal(4))
        snaps.append(engine.push(Bar(ts=k * 60_000, prices=price.copy())))
    assert snaps[10]["status"] == "WARMING_UP" and snaps[10]["exposure"] == 1.0
    assert snaps[-1]["status"] in ("NORMAL", "WARNING", "CONTAGION")
    assert snaps[-1]["z_score"] is not None and 1.0 <= snaps[-1]["effective_bets"] <= 4.0
    assert snaps[-1]["ts"] == 80 * 60_000          # labelled with the bar close time