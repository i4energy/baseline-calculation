from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from baseline_engine.utils.calendar import DayType, get_day_type
from baseline_engine.utils.validation import (
    validate_input_dataframe,
    validate_interval_exists_in_data,
)

@dataclass(frozen=True)
class HighXYMetadata:
    """
    Metadata returned by the High X/Y baseline algorithm.

    Attributes:
    calc_day : pd.Timestamp
        Calculation day for which the baseline is being computed.

    day_type : DayType
        Day classification of calc_day (weekday, saturday, sunday_or_holiday).

    y_required : int
        Number of historical days initially considered according to the methodology.

    x_required : int
        Number of highest-ranked days finally used for the baseline calculation.

    candidate_days : list[pd.Timestamp]
        Final candidate day pool used for event-window scoring.

    selected_days : list[pd.Timestamp]
        Historical days finally selected after ranking.

    correction : float
        Additive correction factor applied to the initial baseline.
    
    correction_window_start : pd.Timestamp
        Start timestamp of the correction window finally used.    
    """
    calc_day: pd.Timestamp
    day_type: DayType
    y_required: int
    x_required: int
    candidate_days: list[pd.Timestamp]
    selected_days: list[pd.Timestamp]
    correction: float
    correction_window_start: pd.Timestamp

def _params_for_day_type(day_type: DayType) -> tuple[int, int]:
    """
    Returns (Y_required, X_required) based on the day type according to IPTO's High X/Y method.

    Rules:
    - Weekday: Y=10, X=5
    - Saturday: Y=3, X=2
    - Sunday/Holiday: Y=3, X=2
    """
    if day_type == DayType.WEEKDAY:
        return 10, 5
    if day_type in (DayType.SATURDAY, DayType.SUNDAY_OR_HOLIDAY):
        return 3, 2
    raise ValueError(f"Unsupported day type: {day_type}")

def _event_index(T_str: pd.Timestamp, T_end: pd.Timestamp) -> pd.DatetimeIndex:
    """
    Build the canonical 15-minute ISP index for the interval [T_str, T_end).

    Example:
    T_str=15:00, T_end=16:00 returns:
    - 15:00
    - 15:15
    - 15:30
    - 15:45
    """
    T_str = pd.Timestamp(T_str)
    T_end = pd.Timestamp(T_end)

    if T_str >= T_end:
        raise ValueError("T_str must be earlier than T_end.")

    return pd.date_range(
        start=T_str,
        end=T_end - pd.Timedelta(minutes=15),
        freq="15min",
    )

def _window_offsets(
    start_ts: pd.Timestamp, 
    end_ts: pd.Timestamp, 
    calc_day: pd.Timestamp,
) -> pd.TimedeltaIndex:
    """
    Calculate the offsets of [start_ts, end_ts) relative to calc_day.

    These offsets are then applied to historical candidate days in order
    to locate the aligned timestamps used for scoring / baseline construction.
    """
    return _event_index(start_ts, end_ts) - pd.Timestamp(calc_day)


def _history_slice(series: pd.Series, calc_day: pd.Timestamp, days: int = 45) -> pd.Series:
    """
    Extracts the historical slice of the series for the past `days` calendar days up to (but not including) `calc_day`.
    
    Example:
    If calc_day = 2026-03-20 00:00:00 and days=45,
    the slice starts at 2026-02-04 00:00:00
    and ends at 2026-03-19 23:45:00.
    """
    # past 45 days starting from prior day (i.e., up to calc_day-1 23:45)
    hist_start = calc_day - pd.Timedelta(days=days)
    hist_end = calc_day - pd.Timedelta(minutes=15) # exclude calc_day itself
    return series.loc[hist_start:hist_end]

def _derive_dr_event_days(participated: pd.Series) -> set[pd.Timestamp]:
    """
    Return the set of normalized calendar days that contain at least one
    DR participation ISP.

    Notes:
    This implements the day-level interpretation:
    if a day contains at least one DR quarter, it is treated as a DR-event day.
    """
    true_idx = participated.index[participated.fillna(False).astype(bool)]
    return {pd.Timestamp(ts).normalize() for ts in true_idx}

def _split_matching_days(
    history: pd.Series,
    target_day_type: DayType,
    invalid_days: set[pd.Timestamp] | None = None,
    dr_event_days: set[pd.Timestamp] | None = None,
    classify_day: Callable[[pd.Timestamp], DayType] = get_day_type,
) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    """
    Split matching-type historical days into:
    - clean_days: matching days that are not invalid and do not contain DR events
    - dr_days: matching days that are not invalid and do contain DR events
    Invalid days are always excluded.

    Notes:
    This is the day-level eligibility step corresponding to the
    construction of the reference window.
    """
    invalid_days = {pd.Timestamp(d).normalize() for d in (invalid_days or set())}
    dr_event_days = {pd.Timestamp(d).normalize() for d in (dr_event_days or set())}

    days = sorted(history.index.normalize().unique())

    clean_days: list[pd.Timestamp] = []
    dr_days: list[pd.Timestamp] = []

    for d in days:
        if d in invalid_days:
            continue

        if classify_day(d) != target_day_type:
            continue

        if d in dr_event_days:
            dr_days.append(d)
        else:
            clean_days.append(d)

    return clean_days, dr_days

def _daily_average(series: pd.Series, day: pd.Timestamp) -> float | None:
    """
    Compute the average consumption over the full day.
    
    This is used only for weekday DR-day refill logic.

    Notes:
    - The calculation is day-based.
    - Daily aggregation is computed from valid/present data.
    - Returns None only if the day has no valid values at all.
    """
    day = pd.Timestamp(day).normalize()
    day_idx = pd.date_range(day, day + pd.Timedelta(hours=23, minutes=45), freq="15min")
    vals = series.reindex(day_idx).dropna()

    if len(vals) == 0:
        return None

    return float(vals.mean())

def _resolve_candidate_days(
    series: pd.Series,
    clean_days: list[pd.Timestamp],
    dr_days: list[pd.Timestamp],
    day_type: DayType,
    y_required: int,
    x_required: int,
) -> list[pd.Timestamp]:
    """
    Resolve the final candidate pool according to the clarified rules.

    Weekday:
    - use up to 10 clean days
    - if clean days are 5..9, use those
    - if clean days are <5, refill from DR-event days ordered by daily average
      until total reaches 5
    - if still <5 -> ValueError

    Saturday / Sunday-Holiday:
    - use up to 3 clean days
    - if 2 or 3 exist, use them
    - if <2 -> ValueError
    """
    recent_clean = clean_days[-y_required:] if len(clean_days) > y_required else clean_days

    if day_type == DayType.WEEKDAY:
        if len(recent_clean) >= x_required:
            return recent_clean

        needed = x_required - len(recent_clean)

        scored_dr_days: list[tuple[pd.Timestamp, float]] = []
        for d in dr_days:
            avg_val = _daily_average(series, d)
            if avg_val is None:
                continue
            scored_dr_days.append((d, avg_val))

        # descending by daily average, tie-break by recency
        scored_dr_days.sort(key=lambda t: (t[1], t[0]), reverse=True)
        refill_days = [d for d, _ in scored_dr_days[:needed]]

        final_days = recent_clean + refill_days

        if len(final_days) < x_required:
            raise ValueError(
                f"Insufficient weekday candidate days for High X/Y even after DR-day refill: "
                f"required at least {x_required}, found {len(final_days)}"
            )

        return final_days

    # Saturday / Sunday-Holiday
    if len(recent_clean) >= x_required:
        return recent_clean

    raise ValueError(
        f"Insufficient weekend/holiday candidate days for High X/Y: "
        f"required at least {x_required}, found {len(recent_clean)}"
    )


def _has_complete_data_for_offsets(
    series: pd.Series,
    day: pd.Timestamp,
    offsets: pd.TimedeltaIndex,
) -> bool:
    """
    Return True if the given historical day has complete data
    for all timestamps defined by the aligned offsets.
    """
    vals = series.reindex(pd.Timestamp(day) + offsets)
    return not vals.isna().any()

def _filter_days_with_complete_offsets(
    series: pd.Series,
    candidate_days: list[pd.Timestamp],
    offsets: pd.TimedeltaIndex,
) -> list[pd.Timestamp]:
    """
    Keep only the candidate days that have complete data
    on the required aligned timestamps.

    Notes:
    This is the place where we effectively implement:
    \"if a candidate day has NaN in the required window, skip it and continue
    with the next available day\".
    """
    usable: list[pd.Timestamp] = []

    for d in candidate_days:
        if _has_complete_data_for_offsets(series, d, offsets):
            usable.append(d)

    return usable

def _score_days(
    series: pd.Series,
    candidate_days: list[pd.Timestamp],
    event_offsets: pd.TimedeltaIndex,
) -> list[tuple[pd.Timestamp, float]]:
    """
    For each candidate day, calculate the mean value of the series at the timestamps corresponding to the offsets.
    Only include candidate days that have complete data for all offsets (no NaNs).
    Returns a list of tuples (candidate_day, score) where score is the mean value.
    """
    scores: list[tuple[pd.Timestamp, float]] = []
    for d in candidate_days:
        vals = series.reindex(pd.Timestamp(d) + event_offsets)
        # phase 1 strict requirement for data availability; if any values are missing -> exclude this candidate day 
        if vals.isna().any():
            continue
        scores.append((pd.Timestamp(d), float(vals.mean())))
    return scores

def _select_top_x(
    scores: list[tuple[pd.Timestamp, float]], 
    x_required: int
) -> list[pd.Timestamp]:
    """
    Sort candidate days by score in descending order and select the top X.

    Tie-break rule:
    If two days have the same score, the more recent day wins.
    """
    if len(scores) < x_required:
        raise ValueError(
            f"Insufficient candidate days for High X/Y: required {x_required}, "
            f"found {len(scores)} after window-level filtering."
        )

    scores_sorted = sorted(scores, key=lambda t: (t[1], t[0]), reverse=True)
    return [d for d, _ in scores_sorted[:x_required]]

def _find_correction_window_start(
    series: pd.Series,
    participated: pd.Series,
    T_str: pd.Timestamp,
    duration: pd.Timedelta = pd.Timedelta(hours=3),
) -> pd.Timestamp:
    """
    Find the most recent previous continuous 3-hour block before T_str that:
    - contains no DR participation
    - contains no NaN in actual consumption

    Notes:
    If the immediate previous 3-hour block is not usable,
    the search moves backwards one ISP at a time until a clean block is found.
    """
    T_str = pd.Timestamp(T_str)
    candidate_end = T_str
    earliest = max(series.index.min(), participated.index.min())

    while True:
        candidate_start = candidate_end - duration
        corr_idx = pd.date_range(
            start=candidate_start,
            end=candidate_end - pd.Timedelta(minutes=15),
            freq="15min",
        )

        if corr_idx.min() < earliest:
            raise ValueError("Unable to find a clean 3-hour correction window in the available history.")

        part_vals = participated.reindex(corr_idx)
        cons_vals = series.reindex(corr_idx)

        # Missing participation data is treated as an error here
        if part_vals.isna().any():
            raise ValueError("Missing participation data in correction-window search.")

        # If actual consumption has NaN in this 3-hour block, move backwards
        if cons_vals.isna().any():
            candidate_end = candidate_end - pd.Timedelta(minutes=15)
            continue

        # If DR participation exists in this 3-hour block, move backwards
        if part_vals.fillna(False).astype(bool).any():
            candidate_end = candidate_end - pd.Timedelta(minutes=15)
            continue

        return candidate_start
    
def _initial_baseline(
    series: pd.Series,
    selected_days: list[pd.Timestamp],
    offsets: pd.TimedeltaIndex,
    calc_day: pd.Timestamp,
) -> pd.Series:
    """
    Calculate the initial baseline as the mean of the aligned values
    from the selected historical days.

    The resulting series is indexed by:
    calc_day + offsets

    Notes:
    The offsets may cover both:
    - correction window
    - event window

    therefore this function constructs the initial baseline over the full required horizon.
    """
    out_idx = pd.Timestamp(calc_day) + offsets

    rows = []
    for d in selected_days:
        vals = series.reindex(pd.Timestamp(d) + offsets)

        # Safety check: selected days must be complete over the required horizon
        if vals.isna().any():
            raise ValueError(
                f"Selected historical day {pd.Timestamp(d).date()} has incomplete data "
                "for the required baseline horizon."
            )

        rows.append(vals.to_numpy())

    mat = pd.DataFrame(rows, columns=out_idx)
    init = mat.mean(axis=0)
    init.index = out_idx
    return init.astype(float)

def _apply_correction(
    series: pd.Series,
    initial_baseline: pd.Series,
    corr_start: pd.Timestamp,
    T_str: pd.Timestamp,
) -> tuple[pd.Series, float]:
    """
    Calculate the additive correction over the correction window [corr_start, T_str).

    Steps:
    - compute the actual mean over the correction window
    - compute the initial-baseline mean over the correction window
    - correction = actual_mean - baseline_mean
    - final_baseline = max(initial_baseline + correction, 0)
    """
    corr_index = pd.date_range(
        start=pd.Timestamp(corr_start),
        end=pd.Timestamp(T_str) - pd.Timedelta(minutes=15),
        freq="15min",
    )

    actual_corr = series.reindex(corr_index)
    base_corr = initial_baseline.reindex(corr_index)

    aligned = pd.concat([actual_corr, base_corr], axis=1).dropna()
    if len(aligned) != len(corr_index):
        raise ValueError("Insufficient data points for correction calculation.")

    correction = float(aligned.iloc[:, 0].mean() - aligned.iloc[:, 1].mean())
    final_baseline = (initial_baseline + correction).clip(lower=0.0)
    return final_baseline, correction

def compute_high_xy(
    data: pd.DataFrame,
    T_str: pd.Timestamp,
    T_end: pd.Timestamp,
    invalid_days: set[pd.Timestamp] | None = None, 
    classify_day: Callable[[pd.Timestamp], DayType] = get_day_type,
) -> tuple[pd.Series, HighXYMetadata]:
    """
    Deterministic implementation of IPTO High X/Y method over a target window [T_str, T_end].

    Main steps:
    1. Validate canonical input and request interval.
    2. Determine calculation day and day type.
    3. Retrieve the previous 45-day historical window.
    4. Split matching historical days into clean days and DR-event days.
    5. Resolve the final candidate day pool according to the methodology.
    6. Find the most recent usable correction window.
    7. Filter candidate days that have complete data over the required horizon.
    8. Score candidate days using event-window mean consumption.
    9. Select the top X historical days.
    10. Build the initial baseline over the required horizon.
    11. Apply additive correction.
    12. Return the final baseline only over [T_str, T_end).

    V1 assumptions:
    - Input dataframe contains:
        - consumption
        - dr_participation
    - invalid_days is an optional whole-day exclusion feature
    - NaN handling is window-based:
        - correction window search skips DR / NaN blocks
        - historical days with NaN in required aligned timestamps are skipped
    """
    invalid_days = {pd.Timestamp(d).normalize() for d in (invalid_days or set())}

    validate_input_dataframe(data)
    validate_interval_exists_in_data(data, T_str, T_end)

    series = data["consumption"].astype(float)
    participated = data["dr_participation"].astype("boolean")

    T_str = pd.Timestamp(T_str)
    T_end = pd.Timestamp(T_end)
    calc_day = T_str.normalize()

    day_type = classify_day(calc_day)
    y_required, x_required = _params_for_day_type(day_type)

    # Check that the dataset contains the required 45-day historical horizon
    series_start = series.index.min().normalize()
    required_start = calc_day - pd.Timedelta(days=45)

    if series_start > required_start:
        raise ValueError("Series does not contain the required 45-day historical window for High X/Y.")

    history = _history_slice(series, calc_day, days=45)
    history_days = sorted(history.index.normalize().unique())

    # Optional operational safeguard
    if len(history_days) < 15:
        raise ValueError("Insufficient history (<15 days) for High X/Y.")

    dr_event_days = _derive_dr_event_days(participated)

    clean_days, dr_days = _split_matching_days(
        history=history,
        target_day_type=day_type,
        invalid_days=invalid_days,
        dr_event_days=dr_event_days,
        classify_day=classify_day,
    )

    candidate_days = _resolve_candidate_days(
        series=series,
        clean_days=clean_days,
        dr_days=dr_days,
        day_type=day_type,
        y_required=y_required,
        x_required=x_required,
    )

    # Find the correction window before final ranking/selection
    # because it determines the required horizon of aligned timestamps
    corr_start = _find_correction_window_start(
        series=series,
        participated=participated,
        T_str=T_str,
    )

    event_offsets = _window_offsets(T_str, T_end, calc_day)
    full_offsets = _window_offsets(corr_start, T_end, calc_day)

    # Skip candidate days with NaN in the required full horizon
    usable_candidate_days = _filter_days_with_complete_offsets(
        series=series,
        candidate_days=candidate_days,
        offsets=full_offsets,
    )

    # Rank only on the event window
    scores = _score_days(
        series=series,
        candidate_days=usable_candidate_days,
        event_offsets=event_offsets,
    )

    selected_days = _select_top_x(scores, x_required)

    initial_baseline = _initial_baseline(
        series=series,
        selected_days=selected_days,
        offsets=full_offsets,
        calc_day=calc_day,
    )

    final_baseline, correction = _apply_correction(
        series=series,
        initial_baseline=initial_baseline,
        corr_start=corr_start,
        T_str=T_str,
    )

    metadata = HighXYMetadata(
        calc_day=calc_day,
        day_type=day_type,
        y_required=y_required,
        x_required=x_required,
        candidate_days=usable_candidate_days,
        selected_days=selected_days,
        correction=correction,
        correction_window_start=corr_start,
    )

    return final_baseline.loc[_event_index(T_str, T_end)], metadata