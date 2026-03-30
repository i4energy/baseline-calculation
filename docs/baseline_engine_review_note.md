# Baseline Engine – Core Review Note

## 1. Purpose

Το παρόν έργο υλοποιεί τον **πυρήνα υπολογισμού baseline** για mFRR demand response, σύμφωνα με τη μεθοδολογία IPTO/ADMIE, για τους δύο βασικούς αλγορίθμους:

- **Meter Before / Meter Before After (MB / MBMA)**
- **High X/Y**

Ο στόχος της τρέχουσας δουλειάς είναι η υλοποίηση του **core algorithm layer** και των απαραίτητων βοηθητικών utilities, έτσι ώστε ο ίδιος πυρήνας να μπορεί αργότερα να χρησιμοποιηθεί τόσο στο:

- **Ex-post baseline calculation**
- **Forecast baseline calculation**

---

## 2. Design goal

Η παρούσα υλοποίηση στοχεύει σε έναν **καθαρό, testable και reviewable baseline core**, χωρίς να κλειδώνει από τώρα την τελική public API / service μορφή.

Πρακτικά, αυτό σημαίνει ότι το παρόν scope καλύπτει:

- canonical input validation
- calendar/day-type handling
- DR joined-event construction
- implementation of **MBMA**
- implementation of **High X/Y**

και **όχι** την πλήρη service/orchestration υλοποίηση για ex-post / forecast flows.

---

## 3. Scope of the current work

### Υλοποιείται στον παρόντα κύκλο:
- canonical input validation
- derivation / handling of day types
- DR joined-event construction
- `compute_mbma(...)`
- `compute_high_xy(...)`
- algorithm-level tests

### Δεν αποτελεί μέρος του παρόντος κύκλου:
- τελικό public API contract
- service boundary behavior
- persistence / storage
- visualization
- full ex-post runner
- full forecast runner
- batch execution / orchestration

Αυτά παραμένουν ως **επόμενο βήμα / wrapper layer**.

---

## 4. Core design principle

Ο βασικός σχεδιαστικός κανόνας είναι ότι ο baseline core πρέπει να δουλεύει με **canonical internal inputs** και να μην εκθέτει αλγοριθμικές εσωτερικές έννοιες προς τον εξωτερικό caller.

Ο caller δεν πρέπει να χρειάζεται να παρέχει έννοιες όπως:
- `day_type`
- `candidate_days`
- `selected_days`
- `dr_event_days`
- fallback flags

Αυτά είναι **internal baseline-engine concepts**.

---

## 5. What is currently implemented

## `src/baseline_engine/utils/validation.py`

Το αρχείο αυτό λειτουργεί ως **validation / preprocessing helper layer**.

### Responsibilities
- validate non-empty dataframe
- validate DatetimeIndex
- validate required columns:
  - `consumption`
  - `dr_participation`
- reject duplicate timestamps
- require monotonically increasing timestamps
- enforce 15-minute ISP alignment
- validate request interval semantics `[req_start, req_end)`
- validate that a requested interval is fully covered by the available timestamps

### Current role
Δεν υλοποιεί baseline logic.  
Υποστηρίζει την ασφαλή τροφοδότηση των baseline algorithms με canonical 15-minute inputs.

---

## `src/baseline_engine/utils/calendar.py`

Το αρχείο αυτό χειρίζεται την ημερολογιακή ταξινόμηση που απαιτεί το High X/Y.

### Responsibilities
- define the supported `DayType` categories:
  - `WEEKDAY`
  - `SATURDAY`
  - `SUNDAY_OR_HOLIDAY`
- provide configurable holiday lookup through `CalendarConfig`
- normalize timestamp-like inputs to calendar dates
- determine whether a given day is a holiday
- classify a given day into the methodology-relevant day types

### Current role
Το `calendar.py` δεν υλοποιεί baseline logic by itself.  
Παρέχει τη day-type classification που απαιτείται κυρίως από το `High X/Y` algorithm, όπου οι historical candidate days πρέπει να ανήκουν στην ίδια day-type category με την ημέρα υπολογισμού.

### Important methodological point
Στην παρούσα λογική:
- holidays and Sundays are grouped together
- Saturdays are treated separately
- weekdays form the third category

This matches the day grouping used by the High X/Y methodology.

---

## `src/baseline_engine/events/builder.py`

Το αρχείο αυτό υλοποιεί τη λογική δημιουργίας **joined DR events** από 15-minute participation flags.

### Responsibilities
- scan 15-minute  `dr_participation` values
- detect transitions between participation and non-participation
- merge consecutive `True` ISPs into a single event
- produce joined events with interval semantics `[start, end)`

### Event representation
Each event is represented by the `Event` dataclass:
- `start`: timestamp of the first ISP with participation
- `end`: timestamp of the first ISP without participation after the event

The interval follows `[start, end)` semantics, meaning:
- `start` is included
- `end` is excluded

### Example
Input:
- 11:00 → True
- 11:15 → True
- 11:30 → False

Output:
- `Event(11:00, 11:30)`

### Edge case handling
If the dataset ends while still inside an event, the event is closed by extending the last observed timestamp by one ISP (15 minutes), preserving the `[start, end)` semantics.

### Current role
Το module αυτό αποτελεί helper για την κατασκευή event intervals, τα οποία χρησιμοποιούνται κυρίως στο ex-post use case, όπου το baseline υπολογίζεται σε επίπεδο event window.

Δεν υλοποιεί baseline logic ούτε validation.

---

## `src/baseline_engine/baseline/meter_before_after.py`

Το αρχείο αυτό υλοποιεί τον **MB / MBMA baseline algorithm core**.

### Core rule
Για ένα target interval `[T_str, T_end)`, ο baseline level ισούται με την κατανάλωση του **πιο πρόσφατου προηγούμενου ISP** που:
- έχει valid consumption
- δεν έχει DR participation

### Current behavior
The algorithm:
1. validates the canonical input dataframe
2. validates that the requested interval `[T_str, T_end)` is fully covered by the dataset
3. extracts canonical consumption and participation series
4. searches backwards from `T_str - 15min`
5. finds the most recent eligible ISP with:
   - non-missing consumption
   - non-missing participation flag
   - `dr_participation == False`
6. uses its consumption value as the MB baseline level for all ISPs in the target interval

### Output
- constant baseline series over the target interval
- metadata with:
  - reference timestamp
  - reference value

### Failure behavior
If no eligible past ISP exists before `T_str`, the algorithm raises `ValueError`.

### Current role
Το module αυτό αποτελεί τον algorithm core για MB / MBMA.  
Δεν υλοποιεί fallback logic, ex-post orchestration, forecast orchestration, ή service-boundary behavior.

---

## `src/baseline_engine/baseline/high_xy.py`

Το αρχείο αυτό υλοποιεί τον **High X/Y baseline algorithm core**.

### Current role
Το module αποτελεί τον κύριο algorithm core για τον υπολογισμό High X/Y baseline.  
Δεν υλοποιεί orchestration, wrapper logic, ή service-boundary behavior.

### Main implemented steps
The algorithm currently performs the following steps:

1. validate canonical input and request interval
2. derive the calculation day from `T_str`
3. classify the day into:
   - `WEEKDAY`
   - `SATURDAY`
   - `SUNDAY_OR_HOLIDAY`
4. determine the nominal High X/Y parameters:
   - weekday → `Y=10`, `X=5`
   - saturday / sunday-holiday → `Y=3`, `X=2`
5. construct the previous 45-day historical window
6. enforce the minimum 15-day viability constraint
7. derive DR-event days from participation flags
8. split matching historical days into:
   - clean days
   - DR-event days
9. build the candidate day pool according to methodology rules
10. for weekdays, apply refill from DR days if clean days are fewer than 5
11. determine the most recent usable 3-hour correction window before the event
12. define:
   - event-window offsets
   - full required horizon offsets
13. skip candidate days that have missing data in the required aligned horizon
14. rank candidate days using event-window average consumption
15. select the top X historical days
16. build the initial baseline over the required horizon
17. compute and apply the additive correction
18. clip negative values to zero
19. return the final baseline only over `[T_str, T_end)`

### Important methodological points reflected in the implementation
- the algorithm uses a 45-day historical lookback
- it requires at least 15 days of minimum viability
- day type matching is enforced through the calendar utility
- invalid/problematic days can be excluded via `invalid_days`
- days containing DR participation are initially excluded from the clean pool
- weekday refill from DR days is supported when fewer than 5 clean weekdays exist
- weekday DR refill uses **daily average consumption**
- final ranking uses **event-window average consumption**
- the correction window is searched backwards until a clean usable 3-hour block is found

### Missing-data handling
The current implementation handles missing data in a window-based manner:
- daily refill averages are computed from valid/present data
- candidate historical days with missing values in the required aligned horizon are skipped
- event-window ranking skips days with missing values in the event window
- correction-window search skips 3-hour blocks containing missing actual consumption data

### Metadata
The module returns both:
- the final baseline series
- a `HighXYMetadata` object containing trace/debug information such as:
  - calculation day
  - day type
  - candidate days
  - selected days
  - correction value
  - correction-window start

### Current boundary
Το module αυτό δεν αποφασίζει fallback προς MBMA.  
If High X/Y cannot be applied, it raises `ValueError`, and a higher-level engine/wrapper can later decide whether to fallback to MBMA.

---

## 6. Missing-data handling

Η τρέχουσα λογική είναι:

- daily averages are computed from valid/present data
- missing ISPs are excluded from the specific downstream computation where they are required
- whole-day exclusion is applied only through `invalid_days`

---

## 7. Fallback policy

- `compute_high_xy(...)`:
  - either returns a valid XY baseline
  - or raises `ValueError` when XY cannot be applied

- fallback to MBMA:
  - is not performed inside the `compute_high_xy(...)` algorithm itself
  - is expected to be handled by a higher-level wrapper / dispatch layer

---

## 8. Ex-post and forecast (conceptual design)

### Ex-post (concept)
- build joined DR events
- baseline = actual for non-event ISPs
- run MB or XY for event windows
- combine results into full-day baseline

### Forecast (concept)
- assume a single synthetic DR event spanning the requested forecast window
- run MB or XY once for that window
- return forecast baseline only for the requested future interval

---

## 9. What is intentionally left out

- ex-post runner implementation
- forecast runner implementation
- dispatch / fallback wrapper
- public API layer
- persistence
- batch execution
- service-boundary exception mapping

---

## 10. Current architectural position

Το έργο αποτελεί:

> **baseline computation core**

και όχι πλήρη service.

---

## 11. Next steps (conceptual)

### Ex-post wrapper
- build events
- apply baseline per event using MB or XY
- fallback from XY to MB where needed
- fill rest with actual consumption

### Forecast wrapper
- define synthetic event
- compute baseline using MB or XY
- fallback from XY to MB where needed
- return forecast window

---

## 12. Final summary

Η υλοποίηση καλύπτει:

- MBMA
- High X/Y
- validation
- calendar logic
- event builder
- tests

Τα ex-post και forecast flows έχουν αποσαφηνιστεί σε επίπεδο σχεδιασμού και αποτελούν το επόμενο βήμα.