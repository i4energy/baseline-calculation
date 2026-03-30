from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd
import holidays 

class DayType(str, Enum):
    """
    Supported calendar day types used by the baseline methodology
    """
    WEEKDAY = "weekday"
    SATURDAY = "saturday"
    SUNDAY_OR_HOLIDAY = "sunday_or_holiday"

@dataclass(frozen=True)
class CalendarConfig:
    """
    Calendar configuration for determining day types.
    
    - country: The country code for which to check holidays (default is "GR" for Greece).
    - subdiv: optional subdivision code for more specific holiday checks (e.g., state or province).
    - observed: whether to consider observed holidays (default is True).
    """
    country: str = "GR"
    subdiv: Optional[str] = None
    observed: bool = True

def _to_date(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Normalize a timestamp-like value to midnight.
    This ensures that day-level comparisons and holiday checks are performed using calendar dates rather than full timestamps.
    """
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    return ts.normalize()  # set time to 00:00:00

def get_holidays(
       year: int,
       cfg: CalendarConfig = CalendarConfig(),       
) -> holidays.HolidayBase:
    """
    Build a holidays object for the specified year and calendar configuration.
    
    Parameters:
        year: int
            Calendar year for which holidays are requested.
        cfg: CalendarConfig
            Holiday configuration (country, subdivision, etc.)
    
    Returns:
        holidays.HolidayBase
            Holiday calendar object used for holiday lookups.
    """
    return holidays.country_holidays(
        country=cfg.country,
        subdiv=cfg.subdiv,
        observed=cfg.observed,
        years=[year],
    )
        
def is_holiday(
    day: pd.Timestamp,
    cfg: CalendarConfig = CalendarConfig(),
    holiday_calendar: Optional[holidays.HolidayBase] = None,
) -> bool:
    """
    Check if a given day is a holiday based on the provided calendar configuration.
    
    Parameters:
    - day: A datetime object representing the day to check.
    - cfg: CalendarConfig object containing country and holiday settings.
    - holiday_calendar: Optional pre-built holidays object to use for checking.
    
    Returns:
    - bool: True if the day is a holiday, False otherwise.
    """
    d = _to_date(day).date()  # convert to date for holiday lookup
    cal = holiday_calendar or get_holidays(_to_date(day).year, cfg)
    return d in cal

def get_day_type(
    day: pd.Timestamp,
    cfg: CalendarConfig = CalendarConfig(),
    holiday_calendar: Optional[holidays.HolidayBase] = None,
) -> DayType:
    """
    Classify a day into one of three categories: 
    - weekday
    - saturday
    - sunday_or_holiday (sunday and holidays are treated the same)
    
    Rule:
    - if holiday: sunday_or_holiday
    - else if sunday: sunday_or_holiday
    - else if saturday: saturday
    - else: weekday

    Returns:
        The classified day type used by the baseline algorithms.
    """

    d = _to_date(day)

    # Holidays are grouped together with Sundays in the methodology
    if is_holiday(d, cfg, holiday_calendar=holiday_calendar):
        return DayType.SUNDAY_OR_HOLIDAY
    
    weekday = d.weekday()  # Monday=0, ...,  Sunday=6
    if weekday == 6:
        return DayType.SUNDAY_OR_HOLIDAY
    if weekday == 5:
        return DayType.SATURDAY
    return DayType.WEEKDAY

        