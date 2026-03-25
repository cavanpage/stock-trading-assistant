import pytest
from backend.ml.signal_generator import SignalInput, generate_signal


def test_buy_signal():
    inp = SignalInput(symbol="AAPL", quant_score=0.8, sentiment_score=0.6, congress_modifier=0.5)
    out = generate_signal(inp)
    assert out.signal_type == "BUY"
    assert out.confidence > 0


def test_sell_signal():
    inp = SignalInput(symbol="AAPL", quant_score=-0.9, sentiment_score=-0.7, congress_modifier=-0.5)
    out = generate_signal(inp)
    assert out.signal_type == "SELL"


def test_hold_signal():
    inp = SignalInput(symbol="AAPL", quant_score=0.1, sentiment_score=0.05, congress_modifier=0.0)
    out = generate_signal(inp)
    assert out.signal_type == "HOLD"


def test_high_vix_dampens_signal():
    inp_normal = SignalInput(symbol="AAPL", quant_score=0.8, sentiment_score=0.0, congress_modifier=0.0, vix=20)
    inp_high_vix = SignalInput(symbol="AAPL", quant_score=0.8, sentiment_score=0.0, congress_modifier=0.0, vix=35)
    out_normal = generate_signal(inp_normal)
    out_high_vix = generate_signal(inp_high_vix)
    assert abs(out_high_vix.composite_score) < abs(out_normal.composite_score)
