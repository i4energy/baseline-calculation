from __future__ import annotations

from dataclasses import dataclass   
import pandas as pd

from baseline_engine.utils.validation import (
    validate_input_dataframe,
    validate_interval_exists_in_data,
)

@dataclass(frozen=True)
class MBMAMetadata:
    """
    Metadata returnes by the MBMA algorithm.

    Attributes:
    - reference_timestamp: The most recent past ISP used as reference for a baseline.
    - reference_value: The consumption value used as the baseline level.
    """
    reference_timestamp: pd.Timestamp
    reference_value: float

def _event_index(T_str: pd.Timestamp, T_end: pd.Timestamp) -> pd.DatetimeIndex:
    """
    Build the canonical 15-minute ISP index for [T_str, T_end).
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

def compute_mbma(
        data: pd.DataFrame, # check that the history data start before T_str
        T_str: pd.Timestamp,
        T_end: pd.Timestamp,
) -> tuple[pd.Series, MBMAMetadata]:
    """
    Compute the Meter Before/After (MBMA) baseline for the target window [T_str, T_end).
    
    Baseline rule:
    - Find the most recent past ISP before T_str
    - The ISP must have:
        * valid consumption value
        * dr_participation == False
    - Use that value as the MB baseline level for all ISPs in the target interval [T_str, T_end).

    Parameters:
    - data : pd.DataFrame
        Canonical input dataframe indexed by 15-minute timestamps and containing:
        - consumption
        - dr_participation

    - T_str : pd.Timestamp
        Start timestamp of the target window (inclusive).

    - T_end : pd.Timestamp
        End timestamp of the target window (exclusive).

    Returns:
    tuple[pd.Series, MBMAMetadata]
        A constant baseline series for the target window and the
        reference metadata used to compute it.

    Raises:
    ValueError
        If:
        - input data is invalid
        - the requested interval is invalid or not covered
        - no valid reference ISP exists before T_str
    """

    validate_input_dataframe(data)
    validate_interval_exists_in_data(data, T_str, T_end)

    # canonicalization step
    series = data["consumption"].astype(float)
    participated = data["dr_participation"].astype("boolean")

    T_str = pd.Timestamp(T_str)
    T_end = pd.Timestamp(T_end)

    window_idx = _event_index(T_str, T_end)
    
    # Search backwards for the most recent valid value before T_str
    # we step by 15 minutes because ISPs are 15-min
    ts = T_str - pd.Timedelta(minutes=15)

    # Safety cap: don't loop forever; stop at series start
    series_start = series.index.min()

    while ts >= series_start:
        val = series.get(ts, None)

        # consumption must exist and be non-NaN
        if val is not None and pd.notna(val):
            # participation info must exist and be False
            if ts not in participated.index:
                ts -= pd.Timedelta(minutes=15)
                continue

            part = participated.loc[ts]
            if pd.isna(part):
                ts -= pd.Timedelta(minutes=15)
                continue

            if bool(part) is True:
                ts -= pd.Timedelta(minutes=15)
                continue
            
            ref_ts = ts
            ref_val = float(val)
            baseline = pd.Series(ref_val, index=window_idx, dtype=float)
            return baseline, MBMAMetadata(reference_timestamp=ref_ts, reference_value=ref_val)
        
        ts -= pd.Timedelta(minutes=15)
    
    raise ValueError("No valid reference value found before T_str for MBMA computation.")   
                    
                
        