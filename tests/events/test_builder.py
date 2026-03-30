import pandas as pd

from baseline_engine.events.builder import build_events 

def test_single_continuous_event():
    '''
    Concecutive participation ISPs should be merged into one interval event.
    '''
    index = pd.date_range("2026-01-01 12:00", periods=4, freq="15min")
    df = pd.DataFrame({"dr_participation": [False, True, True, False]}, index=index)

    events = build_events(df)

    assert len(events) == 1
    assert events[0].start == index[1]
    assert events[0].end == index[3]

def test_two_separate_events():
    '''
    A gap of even one non-participating ISP should split participation into two distinct events.
    '''
    index = pd.date_range("2026-01-01 12:00", periods=6, freq="15min")
    df = pd.DataFrame({"dr_participation": [False, True, True, False, True, False]}, index=index)

    events = build_events(df)

    assert len(events) == 2
    assert events[0].start == index[1]
    assert events[0].end == index[3]
    assert events[1].start == index[4]
    assert events[1].end == index[5]


def test_event_until_last_slot():
    '''
    If participation continues until the final availiable ISP, the event should be extended by one ISP.
    '''
    index = pd.date_range("2026-01-01 23:00", periods=4, freq="15min")
    df = pd.DataFrame({"dr_participation": [False, True, True, True]}, index=index)

    events = build_events(df)

    assert len(events) == 1
    assert events[0].start == index[1]
    assert events[0].end == index[3] + pd.Timedelta(minutes=15)
    

def test_no_events():
    '''
    If no ISP has dr_participation=True, the builder should return an empty event list.
    '''
    index = pd.date_range("2026-01-01 12:00", periods=4, freq="15min")
    df = pd.DataFrame({"dr_participation": [False, False, False, False]}, index=index)

    events = build_events(df)

    assert events == []
