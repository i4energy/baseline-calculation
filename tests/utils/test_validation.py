import pandas as pd
import pytest

from baseline_engine.utils.validation import (
    validate_input_dataframe,
    validate_request_interval,
    validate_interval_exists_in_data
)

def make_df(
    start: str,
    periods: int
    ) -> pd.DataFrame:
    idx = pd.date_range(start=start, periods=periods, freq="15min")

    return pd.DataFrame(
        {
            "consumption": [100.0] * periods,
            "dr_participation": [False] * periods,
        }, 
        index=idx,
    )

def test_validate_input_dataframe_accepts_valid_dataframe():
    data = make_df("2026-02-10 00:00", 8)
    validate_input_dataframe(data)

def test_validate_input_dataframe_rejects_empty_dataframe():
    data = pd.DataFrame(columns=["consumption", "dr_participation"])

    with pytest.raises(ValueError, match="empty"):
        validate_input_dataframe(data)

def test_validate_input_dataframe_rejects_missing_columns():
    idx = pd.date_range("2026-02-10 00:00", periods=4, freq="15min")
    data = pd.DataFrame({"consumption": [100.0] * 4}, index=idx)

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_input_dataframe(data)

def test_validate_input_dataframe_rejects_non_datetime_index():
    data = pd.DataFrame(
        {
            "consumption": [100.0, 101.0],
            "dr_participation": [False, False],
        },
        index=[0, 1],
    )

    with pytest.raises(ValueError, match="DatetimeIndex"):
        validate_input_dataframe(data)

def test_validate_input_dataframe_rejects_duplicate_timestamps():
    idx =pd.DatetimeIndex(
        [
            pd.Timestamp("2026-02-10 00:00"),
            pd.Timestamp("2026-02-10 00:00"),
        ]
    )
    data = pd.DataFrame(
        {
            "consumption": [100.0, 101.0],
            "dr_participation": [False, False],
        },
        index=idx
    )

    with pytest.raises(ValueError, match="duplicate"):
        validate_input_dataframe(data)

def test_validate_input_dataframe_rejects_unsorted_index():
    idx = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-02-10 00:15"),
            pd.Timestamp("2026-02-10 00:00"),
        ]
    )
    data = pd.DataFrame(
        {
            "consumption": [100.0, 101.0],
            "dr_participation": [False, False],
        },
        index = idx,
    )

    with pytest.raises(ValueError, match="sorted"):
        validate_input_dataframe(data)

def test_validate_input_dataframe_rejects_non_15min_grid():
    idx = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-02-10 00:00"),
            pd.Timestamp("2026-02-10 00:10"),
        ]
    )
    data = pd.DataFrame(
        {
            "consumption": [100.0, 101.0],
            "dr_participation": [False, False],
        },
        index = idx,
    )

    with pytest.raises(ValueError, match="15-minute ISP grid"):
        validate_input_dataframe(data)

def test_validate_request_interval_accepts_valid_interval():
    validate_request_interval(
        pd.Timestamp("2026-02-10 10:00"),
        pd.Timestamp("2026-02-10 11:00"),
    )

def test_validate_request_interval_rejects_invalid_order():
    with pytest.raises(ValueError, match="earlier"):
        validate_request_interval(
            pd.Timestamp("2026-02-10 11:00"),
            pd.Timestamp("2026-02-10 10:00"),
        )

def test_validate_request_interval_rejects_non_aligned_start():
    with pytest.raises(ValueError, match="req_start"):
        validate_request_interval(
            pd.Timestamp("2026-02-10 10:10"),
            pd.Timestamp("2026-02-10 11:00"),
        )

def test_validate_request_interval_rejects_non_aligned_end():
    with pytest.raises(ValueError, match="req_end"):
        validate_request_interval(
            pd.Timestamp("2026-02-10 10:00"),
            pd.Timestamp("2026-02-10 11:10"),
        )

def test_validate_interval_exists_in_data_accepts_existing_interval():
    data = make_df("2026-02-10 09:00", 12) # up to 11:45

    validate_interval_exists_in_data(
        data,
        pd.Timestamp("2026-02-10 10:00"),
        pd.Timestamp("2026-02-10 11:00"),
    ) 

def test_validate_interval_exists_in_data_rejects_interval_start_before_data():
    data = make_df("2026-02-10 10:00", 8) # up to 11:45

    with pytest.raises(ValueError, match="starts before"):
        validate_interval_exists_in_data(
            data,
            pd.Timestamp("2026-02-10 09:45"),
            pd.Timestamp("2026-02-10 10:30"),
        )

def test_validate_interval_exists_in_data_rejects_interval_end_after_data():
    data = make_df("2026-02-10 10:00", 4) # up to 10:45

    with pytest.raises(ValueError, match="ends after"):
        validate_interval_exists_in_data(
            data,
            pd.Timestamp("2026-02-10 10:00"),
            pd.Timestamp("2026-02-10 11:15"),
        )
