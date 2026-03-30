from dataclasses import dataclass
import pandas as pd

@dataclass
class Event:
    '''
    Represents a joined DR participation event a time interval.
    Attributes:
    start: pd.Timestamp        
        Timestamp where the joined DR event starts.
    end: pd.Timestamp
        Timestamp where the joined DR event ends.
    
    Notes:
    The event uses interval semantics: [start, end).

    Example:
    Event(start=15:00, end=16:00) represents the active ISP periods:
    - 15:00-15:15
    - 15:15-15:30
    - 15:30-15:45
    - 15:45-16:00

    '''
    start: pd.Timestamp
    end: pd.Timestamp

def build_events(df: pd.DataFrame) -> list[Event]:
    '''
    Build joined DR events from 15-minute participation flags.
    Consecutive ISPs (15-minute intervals) with dr_participation=True are merged into a single event.
    
    Example:
    Input participation flags:
        11:00 True
        11:15 True
        11:30 True
        11:45 False
        12:00 True
        12:15 True
        12:30 False
    Output events:
        event(11:00 - 11:45)
        event(12:00 - 12:30)

    Parameters:
    df: pd.DataFrame
        DataFrame indexed by timestamps containing a boolean column named 'dr_participation'.

    Returns:
    list[Event]
        List of joined DR events represented as intervals [start, end).
    '''
    # Ensure required column exists
    if "dr_participation" not in df.columns:
        raise ValueError("The 'dr_participation' column is required in the DataFrame.")
    
    events: list[Event] = []
    in_event = False # indicates wether we are currently inside a DR evnt
    start = None  # start timestamp of the current event
    prev_timestamp = None # previous ISP timestamp (used to close events) 

    #Iterate over participation flag
    for timestamp, row in df.iterrows(): #participated in df["dr_participation"].items():
        # Start a new event
        # Condition: participation becomes True and we are not already in an event
        if row["dr_participation"] and not in_event: 
            in_event = True
            start = timestamp
        
        # End of event
        # Condition: participationn becomes False while we are inside an event
        elif not row["dr_participation"] and in_event:
            in_event = False
            end = timestamp # last active interval
            events.append(Event(start=start, end=end))
        
        # Keep track of of the last timestamp seen, needed when an event reaches
        # the end of the dataframe without an explicit trailing False
        prev_timestamp = timestamp

    # Edge case: if the dataset ends while still inside an event close it by extending the last active ISP by one 15-minute interval
    if in_event:
        events.append(Event(start=start, end=prev_timestamp + pd.Timedelta(minutes=15)))
    return events
