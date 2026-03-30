from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional
import pandas as pd

from .high_xy import compute_high_xy #HighXYMetadata
from .meter_before_after import compute_mbma #MBMAMetadata
from baseline_engine.utils.calendar import DayType

BaselineMethod = Literal["mbma", "high_xy"]

@dataclass(frozen=True)
class BaselineResult:
    """
    Result container returned by the baseline dispatcher.

    Attributes:
        baseline: Baseline series computed for the target window.
        method_used: The algorithm actually used to produce the result. This may differ from the requested method in case of fallback.
        metadata: Algorithm-specific metadata object.
            -MBMA returns MBMAMetadata
            -High X/Y returns HighXYMetadata
    """
    baseline: pd.Series
    method_used: BaselineMethod
    metadata: object #MBMAMetadata | HighXYMetadata

def compute_baseline(
        series: pd.Series,
        T_str: pd.Timestamp,
        T_end: pd.Timestamp,
        method: BaselineMethod,
        *,
        day_type: Optional[DayType] = None,
        invalid_days: Optional[set[pd.Timestamp]] = None,
        dr_event_days: Optional[set[pd.Timestamp]] = None,
        participated: Optional[pd.Series] = None,
) -> BaselineResult:
    """ 
    Dispatch function to compute baseline using the specified method.
    
    Supported methods:
    - "mbma": directly compute the Meter Before Meter After baseline
    - "high_xy": attempt the High X/Y baseline first

    Fallback behavior:
    If "high_xy" fails because the historical conditions are not sufficient
    (e.g. insufficient history or insufficient candidate days), the dispatcher
    automatically falls back to "mbma".

    Parameters:
        series : pd.Series
            Consumption time series.

        T_str : pd.Timestamp
            Start timestamp of the target window.

        T_end : pd.Timestamp
            End timestamp of the target window.

        method : BaselineMethod
            Requested baseline method ("mbma" or "high_xy").

        day_type : Optional[DayType]
            Day classification required only by High X/Y.

        invalid_days : Optional[set[pd.Timestamp]]
            Historical days with erroneous data or operational problems.
            These are always excluded from High X/Y candidate selection.

        dr_event_days : Optional[set[pd.Timestamp]]
            Historical days that contain DR participation.
            These are initially excluded from High X/Y candidate selection,
            but may be reused in the weekday fallback case.

        participated : Optional[pd.Series]
            Participation flags used by MBMA to skip ISPs with DR participation.

    Returns:
        BaselineResult
            Computed baseline, method actually used, and algorithm-specific metadata.

    Raises:
        ValueError
            If the requested method is unsupported or if High X/Y is requested
            without a valid day_type.
    """
    if invalid_days is None:
        invalid_days = set()

    if dr_event_days is None:
        dr_event_days = set()


    # MBMA execution
    if method == "mbma":
        baseline, meta = compute_mbma(
            series, 
            T_str,
            T_end,
            participated=participated,
        )
        return BaselineResult(
            baseline=baseline,
            method_used="mbma",
            metadata=meta,
        )
    
    # High X/Y execution with automatic fallback to MBMA
    if method == "high_xy":
        if day_type is None:
            raise ValueError("day_type is required for High X/Y")
        
        try:
            baseline, meta = compute_high_xy(
                series,
                T_str,
                T_end,
                day_type=day_type,
                invalid_days=invalid_days,
                dr_event_days=dr_event_days,
            )
            return BaselineResult(
                baseline=baseline,
                method_used="high_xy",
                metadata=meta,
            )
        except ValueError:
            # If HighXY fails (e.g., not enough candidate days), fall back to MBMA
            baseline, meta = compute_mbma(
                series, 
                T_str,
                T_end,
                participated=participated,
            )
            return BaselineResult(
                baseline=baseline,
                method_used="mbma",
                metadata=meta,
            )
    raise ValueError(f"Unsupported baseline method: {method}")
