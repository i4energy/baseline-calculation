from __future__ import annotations
import pandas as pd

REQUIRED_COLUMNS = { "consumption", "dr_participation"}

def _is_15min_aligned(ts: pd.Timestamp) -> bool:
    """
    Return True if the timestamp lies exactly on a 15-min ISP boundary
    """
    ts = pd.Timestamp(ts)
    return ts == ts.floor("15min")
    
def validate_input_dataframe(data: pd.DataFrame) -> None:
    """
    Validate the canonical baseline input dataframe.

    Expected shape:
    - DatetimeIndex at 15-min granularity
    - required columns:
        - consumption
        - dr_participation

    Notes:
    This func validates only structural assumptions of the input dataset.
    It does not decide wether individual days are usable for High X/Y.
    That decision belongs to the algorithm layer.
    """
    if data.empty:
        raise ValueError("Input dataframe is empty.")

    if not isinstance(data.index, pd.DatetimeIndex):
        raise ValueError("Input dataframe index must be a DatetimeIndex.")
    
    missing = REQUIRED_COLUMNS - set(data.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    if data.index.has_duplicates:
        raise ValueError("Input dataframe contains duplicate timestamps.")
    
    if not data.index.is_monotonic_increasing:
        raise ValueError("Input dataframe must be sorted in increasing order.")
    
    aligned = data.index == data.index.floor("15min")
    if not aligned.all():
        raise ValueError("Input dataframe contains timestamps not aligned to the 15-minute ISP grid.")
    
def validate_request_interval(req_start: pd.Timestamp, req_end: pd.Timestamp) -> None:
    """
    Validate a request interval using interval semantics: [req_start, req_end).

    Rules:
    - req_start must be earlier than req_end
    - both endpoints must be aligned to the 15-min ISP grid
    """
    req_start = pd.Timestamp(req_start)
    req_end = pd.Timestamp(req_end)

    if req_start >= req_end:
        raise ValueError("req_start  must be earlier than req_end.")
    
    if not _is_15min_aligned(req_start):
        raise ValueError("req_start must be aligned to the 15-minute ISP grid.")
    
    if not _is_15min_aligned(req_end):
        raise ValueError("req_end must be aligned to the 15-minute ISP grid.")

def validate_interval_exists_in_data(
    data: pd.DataFrame,
    req_start: pd.Timestamp,
    req_end: pd.Timestamp,
) -> None:
    """
    Validate that the requested interval [req_start, req_end) is covered by the dataset.

    This means that:
    - req_start exists within the available timestamp horizon.
    - the last ISP of the interval (req_end - 15 min) also exist within the data

    Example:
    For req_start= 15:00 and req_end= 16:00, the interval requires the ISP labels:
    15:00, 15:15, 15:30, 15:45
    """
    validate_input_dataframe(data) 
    validate_request_interval(req_start, req_end)

    req_start = pd.Timestamp(req_start)
    req_end = pd.Timestamp(req_end)

    last_required_isp = req_end - pd.Timedelta(minutes=15)

    if req_start < data.index.min():
        raise ValueError("Requested interval starts before the available data.")
    
    if last_required_isp > data.index.max():
        raise ValueError("Requested interval ends after the available data.")
    
    required_index = pd.date_range(
        start=req_start,
        end=last_required_isp,
        freq="15min",
    )

    if not required_index.isin(data.index).all():
        raise ValueError("Requested interval is not fully covered by the available timestamps.")
    