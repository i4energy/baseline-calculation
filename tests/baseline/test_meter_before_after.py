import pandas as pd
import pytest

from baseline_engine.baseline.meter_before_after import compute_mbma

def make_data(start: str, end: str, base: float = 100.0) -> pd.DataFrame:
    idx = pd.date_range(start=start, end=end, freq="15min")
    return pd.DataFrame(
        {
            "consumption": base,
            "dr_participation": False,
        },
        index=idx,
    )


def test_mbma_constant_baseline_from_last_past_isp() -> None:
    """
    The baseline should be equal to the most recent eligible past ISP before the target window.
    """
    data = make_data("2026-02-10 00:00", "2026-02-10 23:45", base=100.0)
    data.loc[pd.Timestamp("2026-02-10 17:45"), "consumption"] = 120.0

    T_str = pd.Timestamp("2026-02-10 18:00")
    T_end = pd.Timestamp("2026-02-10 19:00")

    baseline, meta = compute_mbma(data, T_str, T_end)

    assert meta.reference_timestamp == pd.Timestamp("2026-02-10 17:45")
    assert meta.reference_value == 120.0
    assert len(baseline) == 4
    assert (baseline == 120.0).all()


def test_mbma_skips_participated_true() -> None:
    """
    If the most recent past ISP has dr_participation=True,
    the algorithm should continue searching backwards.
    """
    data = make_data("2026-02-10 00:00", "2026-02-10 23:45", base=100.0)
    data.loc[pd.Timestamp("2026-02-10 17:45"), "consumption"] = 120.0
    data.loc[pd.Timestamp("2026-02-10 17:30"), "consumption"] = 110.0

    data.loc[pd.Timestamp("2026-02-10 17:45"), "dr_participation"] = True
    data.loc[pd.Timestamp("2026-02-10 17:30"), "dr_participation"] = False

    T_str = pd.Timestamp("2026-02-10 18:00")
    T_end = pd.Timestamp("2026-02-10 19:00")

    baseline, meta = compute_mbma(data, T_str, T_end)

    assert meta.reference_timestamp == pd.Timestamp("2026-02-10 17:30")
    assert meta.reference_value == 110.0
    assert (baseline == 110.0).all()


def test_mbma_raises_when_no_reference_exists() -> None:
    """
    If no past ISP exists before the target window, the algorithm should raise ValueError.
    """
    data = make_data("2026-02-10 18:00", "2026-02-10 23:45", base=100.0)

    T_str = pd.Timestamp("2026-02-10 18:00")
    T_end = pd.Timestamp("2026-02-10 19:00")

    with pytest.raises(ValueError):
        compute_mbma(data, T_str, T_end)


def test_mbma_raises_if_no_eligible_reference_due_to_participation() -> None:
    """
    If all past ISPs are marked as participated=True,
    the algorithm should raise ValueError.
    """
    data = make_data("2026-02-10 00:00", "2026-02-10 23:45", base=100.0)
    data["dr_participation"] = True

    T_str = pd.Timestamp("2026-02-10 18:00")
    T_end = pd.Timestamp("2026-02-10 19:00")

    with pytest.raises(ValueError):
        compute_mbma(data, T_str, T_end)