import pandas as pd

from baseline_engine.baseline.engine import compute_baseline
from baseline_engine.utils.calendar import DayType

def make_series(start: str, end: str, base: float = 100.0) -> pd.Series:
    """
    Create a synthetic 15 min consumption series with constant base value.
    """
    idx = pd.date_range(start=start, end=end, freq='15min')
    return pd.Series(base, index=idx, dtype=float)

def test_engine_mbma_direct():
    """
    If the requested method is MBMA, the dispatcher should execute MBMA directly and return a constant baseline based on the past ISP. 
    """
    series = make_series("2026-02-10 00:00", "2026-02-10 23:45", base=100.0)
    series.loc[pd.Timestamp("2026-02-10 17:45")] = 120.0

    result = compute_baseline(
        series=series,
        T_str=pd.Timestamp("2026-02-10 18:00"),
        T_end=pd.Timestamp("2026-02-10 18:45"),
        method="mbma",
    )

    assert result.method_used == "mbma"
    assert (result.baseline == 120.0).all()  # baseline should be constant at 120.0

def test_engine_high_xy_fallback_to_mbma():
    """
    If High X/Y is requested but historical requirements are not satisfied, the dispatcher should automatically fall back to MBMA.
    """
    # not enough history for HighXY, should fall back to MBMA
    series = make_series("2026-02-10 00:00", "2026-02-10 23:45", base=100.0)
    series.loc[pd.Timestamp("2026-02-10 17:45")] = 150.0

    result = compute_baseline(
        series=series,
        T_str=pd.Timestamp("2026-02-10 18:00"),
        T_end=pd.Timestamp("2026-02-10 18:45"),
        method="high_xy",
        day_type= DayType.WEEKDAY,
        invalid_days=set(),
        dr_event_days=set(),
    )

    assert result.method_used == "mbma"  # should fall back to MBMA
    assert (result.baseline == 150.0).all()  # baseline should be constant at 150.0 from the last past ISP