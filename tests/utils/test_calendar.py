import pandas as pd

from baseline_engine.utils.calendar import CalendarConfig, DayType, get_day_type, get_holidays, is_holiday

def test_weekday_classification():
    """
    A normal weekday should be classified as WEEKDAY.
    """
    # Test a regular weekday
    day = pd.Timestamp("2026-02-10")  # Tuesday
    assert get_day_type(day) == DayType.WEEKDAY

def test_saturday_classification():
    """
    A Saturday should be classified as SARYRDAY.
    """
    # Test a saturday
    day = pd.Timestamp("2026-02-07")  # Saturday
    assert get_day_type(day) == DayType.SATURDAY

def test_sunday_classification():
    """
    A Sunday should be classified as SUNDAY_OR_HOLIDAY.
    """
    # Test a sunday
    day = pd.Timestamp("2026-02-08")  # Sunday
    assert get_day_type(day) == DayType.SUNDAY_OR_HOLIDAY

def test_holiday_classification_gr():
    """
    A fixed Greek public holiday should be identified as a holiday and classified as SUNDAY_OR_HOLIDAY.
    """
    # Test a Greek Holiday (Independence Day on March 25th)
    day = pd.Timestamp("2026-03-25")  # Greek Independence Day
    cfg = CalendarConfig(country="GR")

    cal = get_holidays(day.year, cfg)
    assert is_holiday(day, cfg, holiday_calendar=cal) == True
    assert get_day_type(day, cfg, holiday_calendar=cal) == DayType.SUNDAY_OR_HOLIDAY

def test_movable_holiday_classification_gr():
    """
    A movable Greek holiday (i.e. Easter Monday) should be identified correctly.
    """
    # Test a movable Greek Holiday (Easter Monday)
    day = pd.Timestamp("2026-04-13")  # Easter Monday in 2026
    cfg = CalendarConfig(country="GR")

    cal = get_holidays(day.year, cfg)
    assert is_holiday(day, cfg, holiday_calendar=cal) == True
    assert get_day_type(day, cfg, holiday_calendar=cal) == DayType.SUNDAY_OR_HOLIDAY 

def test_non_holiday_classification_gr():
    """
    A non-holiday weekday in Greece should not be marked as a holiday.
    """
    # Test a non-holiday in Greece
    day = pd.Timestamp("2026-02-10")  # Regular Tuesday
    cfg = CalendarConfig(country="GR")

    cal = get_holidays(day.year, cfg)
    assert is_holiday(day, cfg, holiday_calendar=cal) == False
    assert get_day_type(day, cfg, holiday_calendar=cal) == DayType.WEEKDAY

