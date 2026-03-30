import pandas as pd
import pytest

from baseline_engine.baseline.high_xy import (
    _daily_average,
    _event_index,
    _find_correction_window_start,
    _resolve_candidate_days,
    _score_days,
    _window_offsets,
    compute_high_xy,
)
from baseline_engine.utils.calendar import DayType, get_day_type

def _build_base_frame(start: str, end: str, value: float = 100.0) -> pd.DataFrame:
    """
    Build a simple 15-minute canonical dataframe with:
    - constant consumption
    - dr_participation=False
    """
    idx = pd.date_range(start=start, end=end, freq="15min")
    return pd.DataFrame(
        {
            "consumption": value,
            "dr_participation": False,
        },
        index=idx,
    )


def test_event_index_is_left_closed_right_open() -> None:
    """
    The event interval uses [T_str, T_end) semantics.
    """
    idx = _event_index(
        pd.Timestamp("2026-03-20 15:00:00"),
        pd.Timestamp("2026-03-20 16:00:00"),
    )

    assert list(idx) == [
        pd.Timestamp("2026-03-20 15:00:00"),
        pd.Timestamp("2026-03-20 15:15:00"),
        pd.Timestamp("2026-03-20 15:30:00"),
        pd.Timestamp("2026-03-20 15:45:00"),
    ]


def test_weekend_candidate_resolution_uses_most_recent_days() -> None:
    """
    For weekend/holiday logic, candidate resolution must keep
    the most recent matching clean days.
    """
    series = pd.Series(dtype=float)
    clean_days = [
        pd.Timestamp("2026-01-03"),
        pd.Timestamp("2026-01-10"),
        pd.Timestamp("2026-01-17"),
        pd.Timestamp("2026-01-24"),
    ]

    resolved = _resolve_candidate_days(
        series=series,
        clean_days=clean_days,
        dr_days=[],
        day_type=DayType.SATURDAY,
        y_required=3,
        x_required=2,
    )

    assert resolved == [
        pd.Timestamp("2026-01-10"),
        pd.Timestamp("2026-01-17"),
        pd.Timestamp("2026-01-24"),
    ]


def test_daily_average_uses_valid_present_data() -> None:
    """
    Daily average for weekday DR-day refill is computed from valid/present data.
    A single missing quarter should not invalidate the whole day.
    """
    df = _build_base_frame("2026-02-01 00:00:00", "2026-02-01 23:45:00", value=20.0)
    df.loc[pd.Timestamp("2026-02-01 03:15:00"), "consumption"] = float("nan")

    avg = _daily_average(df["consumption"], pd.Timestamp("2026-02-01"))
    assert avg == 20.0


def test_weekday_dr_refill_uses_daily_average() -> None:
    """
    When weekday clean days are fewer than 5, refill must use DR days
    ranked by daily average.
    """
    df = _build_base_frame("2026-02-01 00:00:00", "2026-02-07 23:45:00", value=10.0)

    df.loc["2026-02-03", "consumption"] = 20.0
    df.loc["2026-02-04", "consumption"] = 30.0
    df.loc[pd.Timestamp("2026-02-04 03:15:00"), "consumption"] = float("nan")

    series = df["consumption"]
    clean_days = [
        pd.Timestamp("2026-02-05"),
        pd.Timestamp("2026-02-06"),
        pd.Timestamp("2026-02-07"),
    ]
    dr_days = [
        pd.Timestamp("2026-02-03"),
        pd.Timestamp("2026-02-04"),
    ]

    resolved = _resolve_candidate_days(
        series=series,
        clean_days=clean_days,
        dr_days=dr_days,
        day_type=DayType.WEEKDAY,
        y_required=10,
        x_required=5,
    )

    assert pd.Timestamp("2026-02-04") in resolved
    assert pd.Timestamp("2026-02-03") in resolved
    assert len(resolved) == 5


def test_correction_window_search_skips_nan_block() -> None:
    """
    If the immediate previous 3-hour block contains NaN,
    the correction-window search must move backwards.
    """
    df = _build_base_frame("2026-03-19 00:00:00", "2026-03-20 23:45:00", value=100.0)
    series = df["consumption"]
    participated = df["dr_participation"].astype("boolean")

    series.loc[pd.Timestamp("2026-03-20 13:15:00")] = float("nan")

    corr_start = _find_correction_window_start(
        series=series,
        participated=participated,
        T_str=pd.Timestamp("2026-03-20 15:00:00"),
    )

    assert corr_start == pd.Timestamp("2026-03-20 10:15:00")


def test_score_days_skips_nan_in_event_window() -> None:
    """
    A candidate day with NaN inside the aligned event window
    must be skipped from ranking.
    """
    df = _build_base_frame("2026-03-01 00:00:00", "2026-03-10 23:45:00", value=100.0)
    series = df["consumption"]
    calc_day = pd.Timestamp("2026-03-10").normalize()

    event_offsets = _window_offsets(
        pd.Timestamp("2026-03-10 15:00:00"),
        pd.Timestamp("2026-03-10 16:00:00"),
        calc_day,
    )

    candidate_days = [pd.Timestamp("2026-03-08"), pd.Timestamp("2026-03-09")]
    series.loc[pd.Timestamp("2026-03-08 15:15:00")] = float("nan")

    scores = _score_days(series, candidate_days, event_offsets)

    assert [d for d, _ in scores] == [pd.Timestamp("2026-03-09")]


def test_compute_high_xy_end_to_end_weekday() -> None:
    """
    End-to-end weekday High X/Y test:
    - build candidate pool
    - rank selected days
    - compute initial baseline
    - apply correction
    - return final event-window baseline
    """
    df = _build_base_frame("2026-01-15 00:00:00", "2026-03-20 23:45:00", value=100.0)

    chosen_days = [
        "2026-03-03",
        "2026-03-09",
        "2026-03-11",
        "2026-03-16",
        "2026-03-19",
    ]
    chosen_levels = {
        "2026-03-03": [123.0, 122.0, 124.0, 123.0],
        "2026-03-09": [128.0, 127.0, 129.0, 128.0],
        "2026-03-11": [121.0, 122.0, 123.0, 121.0],
        "2026-03-16": [125.0, 126.0, 124.0, 127.0],
        "2026-03-19": [118.0, 120.0, 121.0, 119.0],
    }

    # Event-window values for the intended top-X days
    for day, vals in chosen_levels.items():
        for ts, value in zip(
            _event_index(pd.Timestamp(f"{day} 15:00:00"), pd.Timestamp(f"{day} 16:00:00")),
            vals,
        ):
            df.loc[ts, "consumption"] = value

    # Historical correction-window-aligned values for selected days
    for day in chosen_days:
        corr_idx = _event_index(
            pd.Timestamp(f"{day} 12:00:00"),
            pd.Timestamp(f"{day} 15:00:00"),
        )
        df.loc[corr_idx, "consumption"] = [116, 117, 116, 118, 117, 118, 119, 118, 120, 121, 120, 121]

    # Actual correction-window values for the calculation day
    actual_corr_idx = _event_index(
        pd.Timestamp("2026-03-20 12:00:00"),
        pd.Timestamp("2026-03-20 15:00:00"),
    )
    df.loc[actual_corr_idx, "consumption"] = [121, 122, 121, 123, 122, 123, 124, 123, 124, 125, 124, 125]

    baseline, meta = compute_high_xy(
        data=df,
        T_str=pd.Timestamp("2026-03-20 15:00:00"),
        T_end=pd.Timestamp("2026-03-20 16:00:00"),
        invalid_days={pd.Timestamp("2026-03-05"), pd.Timestamp("2026-03-12")},
    )

    assert len(baseline) == 4
    assert list(baseline.index) == list(
        _event_index(pd.Timestamp("2026-03-20 15:00:00"), pd.Timestamp("2026-03-20 16:00:00"))
    )
    assert meta.day_type == DayType.WEEKDAY
    assert meta.y_required == 10
    assert meta.x_required == 5
    assert len(meta.selected_days) == 5
    assert meta.correction > 0.0


def test_compute_high_xy_skips_candidate_day_with_nan_in_required_horizon() -> None:
    """
    If a candidate day is strong on the event window but contains NaN
    in the required full horizon, it must be skipped and replaced by another usable day.
    """
    df = _build_base_frame("2026-01-15 00:00:00", "2026-03-20 23:45:00", value=100.0)

    strong_days = [
        "2026-03-03",
        "2026-03-04",
        "2026-03-06",
        "2026-03-09",
        "2026-03-11",
        "2026-03-16",
        "2026-03-18",
        "2026-03-19",
    ]

    for i, day in enumerate(strong_days, start=1):
        vals = [100 + i, 101 + i, 102 + i, 103 + i]

        for ts, value in zip(
            _event_index(pd.Timestamp(f"{day} 15:00:00"), pd.Timestamp(f"{day} 16:00:00")),
            vals,
        ):
            df.loc[ts, "consumption"] = value

        corr_idx = _event_index(
            pd.Timestamp(f"{day} 12:00:00"),
            pd.Timestamp(f"{day} 15:00:00"),
        )
        df.loc[corr_idx, "consumption"] = 110.0 + i

    # This day is strong for ranking but unusable in the required horizon
    df.loc[pd.Timestamp("2026-03-18 13:15:00"), "consumption"] = float("nan")

    baseline, meta = compute_high_xy(
        data=df,
        T_str=pd.Timestamp("2026-03-20 15:00:00"),
        T_end=pd.Timestamp("2026-03-20 16:00:00"),
    )

    assert pd.Timestamp("2026-03-18") not in meta.selected_days
    assert len(meta.selected_days) == 5
    assert len(baseline) == 4