# REVIEW.md

Defects found in the starter `app/main.py` and how each was fixed (all in the final file).

| # | Where | What is wrong | How you'd notice it | Fix |
|---|---|---|---|---|
| 1 | `client = MongoClient(...)` | Not `tz_aware`: PyMongo returns naive datetimes, so every later conversion silently treats UTC values as local time. | Punch-in at 09:28 IST read back and re-used as local time gives dates/late minutes shifted by 5h30. | `MongoClient(..., tz_aware=True, tzinfo=UTC)`; `dt_to_ms` also defends against naive values. |
| 2 | `/health` | Always returns 200, never touches MongoDB. | Stop MongoDB: still `200`. Spec requires 503. | `admin.command("ping")`, 503 on failure. |
| 3 | `compute_late_minutes` | Uses the punch-in's own (UTC) date for the shift start, no IST, no overnight shift, no second truncation; floors *then* tests `> 10`, so 10:00:30 (10 min 30 s) is treated as 10 -> not late. | Punch 09:40:01 IST gives 0 instead of 10; overnight 21:55 for a 22:00 shift is computed against the wrong day. | `shift_bounds()` builds the IST shift start on the attendance date; late only if seconds > 600, then `seconds // 60`. |
| 4 | `compute_work_hours` | Python `round()` on a float: banker's rounding and float noise. | 3 h 0 m 18 s = 3.005 h returns 3.0, expected 3.01. | Exact `Decimal` division, `ROUND_HALF_UP`. |
| 5 | `compute_overtime` | No 30-minute minimum (R3), shift end always on `date`'s calendar day (wrong for overnight), `max(0, ...)` leaks 1-29 minute overtime, naive/IST mixing. | Punch-out 5 min after shift end reports 5 min overtime; overnight 06:40 punch-out gets a negative -> 0. | `shift_bounds()` (end on next day when overnight) and `>= 30` rule. |
| 6 | `compute_half_day` / punch-out | Punch-out, `half_day`, `work_hours` and overtime were not implemented at all; also no 24-hour and `<= punch_in` guards. | `POST /attendance/punch-out` is 404/405. | Implemented with conditional atomic update (`punch_out: null`) -> exactly one 200, others 409. |
| 7 | `EmployeeIn` | No validation: bad `emp_code` format, `shift_start == shift_end`, impossible dates, malformed email/time, unbounded lengths. | `joined_on: "2026-02-30"`, `shift_start: "25:99"` are accepted. | Pydantic patterns and validators, 422. |
| 8 | `create_employee` | Check-then-insert race, no unique index; `created_at` is `datetime.now()` (naive local) and the response leaks a raw datetime instead of epoch ms. | 12 parallel POSTs create duplicates; response `created_at` is a string. | Unique index on `emp_code`, `DuplicateKeyError` -> 409; UTC time, ms in the response. |
| 9 | `list_employees` | `skip = page * page_size` skips the first page; `total` ignores the filter; no sort; unbounded `page_size`/`page`. | `page=1` misses the first rows; `total` != filtered count. | `(page-1)*size`, `count_documents(filter)`, sort `emp_code`, `page>=1`, `1<=page_size<=100`. |
| 10 | `punch_in` | Unknown employee -> `None["shift_start"]` -> 500; date taken from server-local time, ignoring IST and overnight shifts; `if body.punched_at` treats a valid value as falsy edge and accepts seconds/floats/strings; any `status` accepted (ABSENT, junk). | `emp_code=EMP9999` -> 500; `punched_at` in seconds is accepted. | 404; R1 `attendance_date()`; `StrictInt` within ms range; status limited to PRESENT/WFH/ON_DUTY. |
| 11 | `punch_in` | Check-then-insert race (two parallel requests both pass `find_one`); response exposes `id` and raw datetimes; `punched_at` not truncated to whole seconds. | 16 parallel punch-ins give several 201. | Unique `(emp_code, date)` index and `insert_one` -> `DuplicateKeyError` -> 409; `record_out()` returns ms and no id. |
| 12 | `list_attendance` | Loads the whole collection into memory, sorts in Python, no tie-break on `emp_code`, no index, accepts invalid dates / `date_from > date_to` / bad status, exposes `id`, leaks `_id`-style shape, `total` computed from the loaded list. | Slow and memory-heavy at 100k docs; `date_from=banana` accepted. | DB-side filter + sort + skip/limit on a matching index, `count_documents`, 422 validation. |
| 13 | startup | No indexes at all. | `explain` shows COLLSCAN. | Idempotent `create_index` calls in the lifespan hook. |
| 14 | serialisation | Datetimes returned raw (naive, wrong format) and missing `half_day`/`history` on legacy documents are not defaulted. | JSON contains ISO strings; legacy record lacks `history`. | `record_out()` normalises instants to epoch ms and defaults missing fields. |

## Looked at and found fine
- `load_dotenv()` defaults to `override=False`, so real environment variables already take priority.
- `IST = timezone(timedelta(hours=5, minutes=30))` is correct: IST has no DST, a fixed offset is enough.
- Stripping `_id` from responses was already done in `create_employee`.
