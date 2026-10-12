"""
Employee Attendance & Analytics API (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (real environment variables win over a local .env)

The assignment asks for a single file, so the layers live in one module and depend only downwards:

    1. Settings & constants
    2. Domain rules          pure functions for R1-R5, R8 (no I/O, unit-testable)
    3. Errors                framework-free domain exceptions, mapped to HTTP once
    4. Serialisation         BSON documents -> API shapes (epoch ms, defaults)
    5. Schemas & validation  request models and query parsing
    6. Pipelines             aggregation / query builders (shared by analytics and /admin/explain)
    7. Persistence           Database (client, indexes) and one repository per collection
    8. Services              use-cases; orchestrate rules + repositories
    9. API                   dependencies and thin routers
   10. Application factory
"""
import calendar
import json
import os
import re
import time as _time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, StrictInt, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, PyMongoError

# =========================================================================== #
# 1. Settings & constants
# =========================================================================== #
load_dotenv()  # override=False: variables already set in the environment win


@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_db: str
    server_selection_timeout_ms: int = 5000
    max_pool_size: int = 100
    query_max_time_ms: int = 15000  # server-side cap on any single aggregation

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            mongo_db=os.getenv("MONGO_DB", "attendance_db"),
        )


UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))  # fixed offset: IST has no DST

PRESENT_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]
StatusT = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatusT = Literal["PRESENT", "WFH", "ON_DUTY"]

MS_MIN, MS_MAX = 100_000_000_000, 4_102_444_800_000
EpochMs = Annotated[StrictInt, Field(ge=MS_MIN, le=MS_MAX)]

GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_HOURS = Decimal("4.50")
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92
MAX_REGULARIZE_RETRIES = 5

# index key patterns; each query shape is also hinted so plans are deterministic
IDX_EMP_DATE = [("emp_code", ASCENDING), ("date", DESCENDING)]
IDX_DATE_EMP = [("date", DESCENDING), ("emp_code", ASCENDING)]
IDX_STATUS_DATE = [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)]
IDX_EMP_PUNCH = [("emp_code", ASCENDING), ("punch_in", DESCENDING)]
IDX_DEPT_JOINED = [("department", ASCENDING), ("joined_on", ASCENDING)]
IDX_JOINED = [("joined_on", ASCENDING)]
ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


# =========================================================================== #
# 2. Domain rules (pure)
# =========================================================================== #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: treat naive values as UTC
        dt = dt.replace(tzinfo=UTC)
    return calendar.timegm(dt.utctimetuple()) * 1000 + dt.microsecond // 1000


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    return dt.replace(tzinfo=UTC) if dt is not None and dt.tzinfo is None else dt


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def now_ms_precision() -> datetime:
    now = datetime.now(UTC)  # BSON dates hold milliseconds
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def _hm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares lexicographically


def shift_bounds(day: date, shift_start: str, shift_end: str) -> tuple[datetime, datetime]:
    """IST start and end of the shift belonging to attendance date `day` (R1)."""
    sh, sm = _hm(shift_start)
    eh, em = _hm(shift_end)
    start = datetime(day.year, day.month, day.day, sh, sm, tzinfo=IST)
    end = datetime(day.year, day.month, day.day, eh, em, tzinfo=IST)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    return start, end


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date:
    """R1: IST date of the punch-in; an overnight early-morning punch belongs to the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(shift_start, shift_end):
        eh, em = _hm(shift_end)
        if (local.hour, local.minute, local.second) < (eh, em, 0):
            day -= timedelta(days=1)
    return day


def compute_late_minutes(punch_in: datetime, start: datetime) -> int:
    """R2: strictly more than 10:00 after shift start -> whole minutes since shift start."""
    seconds = int((punch_in - start).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_overtime(punch_out: datetime, end: datetime) -> int:
    """R3: whole minutes after shift end, only when at least 30."""
    minutes = int((punch_out - end).total_seconds()) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 to 2 decimals, half-up (exact decimal arithmetic)."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_half_day(work_hours: Optional[float]) -> bool:
    """R5: rounded work_hours below 4.50."""
    return work_hours is not None and Decimal(repr(work_hours)) < HALF_DAY_HOURS


def derived_fields(status: str, punch_in, punch_out, shift_start: str, shift_end: str, day: date) -> dict:
    """The four server-computed fields for a record in its final state."""
    if status not in PRESENT_STATUSES or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    start, end = shift_bounds(day, shift_start, shift_end)
    out = {"work_hours": None, "late_minutes": compute_late_minutes(punch_in, start),
           "overtime_minutes": 0, "half_day": False}
    if punch_out is not None:
        out["work_hours"] = compute_work_hours(punch_in, punch_out)
        out["overtime_minutes"] = compute_overtime(punch_out, end)
        out["half_day"] = compute_half_day(out["work_hours"])
    return out


def weekdays_between(first: date, last: date) -> int:
    """Mon-Fri days in [first, last] (R7)."""
    return sum(1 for i in range((last - first).days + 1) if (first + timedelta(days=i)).weekday() < 5)


def half_up(value: Decimal, places: int) -> float:
    """R8: half-up rounding of an exact Decimal."""
    return float(value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


# =========================================================================== #
# 3. Errors (framework-free; mapped to HTTP in one place)
# =========================================================================== #
class DomainError(Exception):
    status_code = 400

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class NotFound(DomainError):
    status_code = 404


class Conflict(DomainError):
    status_code = 409


class Unprocessable(DomainError):
    status_code = 422


# =========================================================================== #
# 4. Serialisation (BSON -> API)
# =========================================================================== #
def _ms_if_dt(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def history_out(entry: dict) -> dict:
    changes = {k: {"from": _ms_if_dt(c.get("from")), "to": _ms_if_dt(c.get("to"))}
               for k, c in (entry.get("changes") or {}).items()}
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
    """Missing legacy fields default to 0 / false / []; _id never leaves the service."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start", "09:30"),
        "shift_end": doc.get("shift_end", "18:30"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


# =========================================================================== #
# 5. Schemas & validation
# =========================================================================== #
HM_PATTERN = r"^([01]\d|2[0-3]):[0-5]\d$"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

PageQ = Annotated[int, Query(ge=1, le=1_000_000_000)]
PageSizeQ = Annotated[int, Query(ge=1, le=100)]
MonthQ = Annotated[str, Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")]
LimitQ = Annotated[int, Query(ge=1, le=50)]


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HM_PATTERN)
    shift_end: str = Field(default="18:30", pattern=HM_PATTERN)
    joined_on: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")

    @field_validator("joined_on")
    @classmethod
    def _real_date(cls, v: str) -> str:
        date.fromisoformat(v)
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatusT = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[StatusT] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


def _invalid(where: str, name: str, msg: str, value: Any = None):
    raise RequestValidationError([{"type": "value_error", "loc": (where, name), "msg": msg, "input": value}])


def parse_date(value: str, name: str, where: str = "query") -> date:
    if not DATE_RE.match(value):
        _invalid(where, name, "date must be YYYY-MM-DD", value)
    try:
        return date.fromisoformat(value)
    except ValueError:
        _invalid(where, name, "not a real calendar date", value)


def parse_month(value: str, name: str = "month") -> tuple[date, date]:
    if not MONTH_RE.match(value):
        _invalid("query", name, "month must be YYYY-MM", value)
    year, month = int(value[:4]), int(value[5:])
    if year < 1:
        _invalid("query", name, "year out of range", value)
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def parse_trend_range(from_: str, to: str) -> tuple[date, date]:
    d_from, d_to = parse_date(from_, "from"), parse_date(to, "to")
    if d_to < d_from:
        _invalid("query", "to", "to must not be before from", to)
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        _invalid("query", "to", f"range must not exceed {MAX_TREND_DAYS} days", to)
    return d_from, d_to


def require(value: Any, name: str):
    if value is None or value == "":
        _invalid("query", name, "field required", value)
    return value


@dataclass(frozen=True)
class AttendanceQuery:
    """Validated filter for GET /attendance, also reused by the explain endpoint."""
    emp_code: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    status: Optional[str] = None

    @classmethod
    def parse(cls, emp_code, date_from, date_to, status) -> "AttendanceQuery":
        d_from = parse_date(date_from, "date_from") if date_from else None
        d_to = parse_date(date_to, "date_to") if date_to else None
        if d_from and d_to and d_from > d_to:
            _invalid("query", "date_from", "date_from must not be after date_to", date_from)
        return cls(emp_code or None, date_from or None, date_to or None, status or None)

    def to_filter(self) -> dict:
        q: dict = {}
        if self.emp_code:
            q["emp_code"] = self.emp_code
        if self.date_from or self.date_to:
            q["date"] = {}
            if self.date_from:
                q["date"]["$gte"] = self.date_from
            if self.date_to:
                q["date"]["$lte"] = self.date_to
        if self.status:
            q["status"] = self.status
        return q

    def hint(self) -> list:
        if self.emp_code:
            return IDX_EMP_DATE
        if self.status:
            return IDX_STATUS_DATE
        return IDX_DATE_EMP


# =========================================================================== #
# 6. Pipelines (pure builders; stored derived values are authoritative)
# =========================================================================== #
_DAY_OF_WEEK = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "onError": None}}}
_IS_WEEKDAY = {"$and": [{"$gte": [_DAY_OF_WEEK, 2]}, {"$lte": [_DAY_OF_WEEK, 6]}]}  # Mon(2)..Fri(6)
# presence in half-day units (full day 2, half day 1): integer sums stay exact
_PRESENT_UNITS = {"$cond": [{"$in": ["$status", PRESENT_STATUSES]},
                            {"$cond": [{"$ifNull": ["$half_day", False]}, 1, 2]}, 0]}
_WEEKDAY_UNITS = {"$cond": [_IS_WEEKDAY, _PRESENT_UNITS, 0]}
_LATE = {"$ifNull": ["$late_minutes", 0]}
_IS_LATE = {"$cond": [{"$gt": [_LATE, 0]}, 1, 0]}
_OVERTIME = {"$ifNull": ["$overtime_minutes", 0]}
_HAS_HOURS = {"$and": [{"$in": ["$status", PRESENT_STATUSES]}, {"$isNumber": "$work_hours"}]}
_WH_CENTS = {"$cond": [_HAS_HOURS, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}
_WH_COUNT = {"$cond": [_HAS_HOURS, 1, 0]}


def _month_match(first: date, last: date) -> dict:
    return {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}


def monthly_pipeline(emp_code: str, first: date, last: date) -> list:
    return [
        {"$match": {"emp_code": emp_code, **_month_match(first, last)}},
        {"$group": {
            "_id": None,
            "present_units": {"$sum": _WEEKDAY_UNITS},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": _IS_LATE},
            "total_late_minutes": {"$sum": _LATE},
            "total_overtime_minutes": {"$sum": _OVERTIME},
        }},
    ]


def department_summary_pipeline(first: date, last: date, department: Optional[str]) -> tuple[list, list]:
    """Start from employees so people with zero logs still count; fold each person's logs in via $lookup."""
    emp_match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department:
        emp_match["department"] = department
    per_employee = [
        {"$match": _month_match(first, last)},
        {"$group": {
            "_id": None,
            "present_units": {"$sum": _WEEKDAY_UNITS},
            "wh_cents": {"$sum": _WH_CENTS},
            "wh_n": {"$sum": _WH_COUNT},
            "late_count": {"$sum": _IS_LATE},
            "late_minutes": {"$sum": _LATE},
            "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
        }},
    ]

    def of(field: str) -> dict:
        return {"$ifNull": [{"$first": f"$l.{field}"}, 0]}

    pipeline = [
        {"$match": emp_match},
        {"$lookup": {"from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
                     "pipeline": per_employee, "as": "l"}},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_units": {"$sum": of("present_units")},
            "wh_cents": {"$sum": of("wh_cents")},
            "wh_n": {"$sum": of("wh_n")},
            "late_count": {"$sum": of("late_count")},
            "total_late_minutes": {"$sum": of("late_minutes")},
            "leave_count": {"$sum": of("leave_count")},
            "on_duty_count": {"$sum": of("on_duty_count")},
        }},
        {"$sort": {"_id": 1}},
    ]
    return pipeline, (IDX_DEPT_JOINED if department else IDX_JOINED)


def leaderboard_pipeline(first: date, last: date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": _month_match(first, last)},
        {"$group": {"_id": "$emp_code", "total": {"$sum": _LATE}, "late_count": {"$sum": _IS_LATE}}},
        {"$match": {"total": {"$gt": 0}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},  # logs of unknown employees drop out here
    ]
    if department:
        pipeline.append({"$match": {"e.department": department}})  # ranking happens inside the department
    pipeline += [
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},  # 1,2,2,4
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking
        {"$sort": {"total": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$e.name", "department": "$e.department",
                      "total_late_minutes": "$total", "late_count": 1}},
    ]
    return pipeline


def trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    """One row per calendar day in [d_from, d_to]; integer arithmetic keeps half-up rounding exact."""
    lo = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    hi = datetime(d_to.year, d_to.month, d_to.day, tzinfo=UTC) + timedelta(days=1)  # upper bound exclusive
    window = {"documents": [-6, 0]}
    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code", "as": "l",
            "pipeline": [{"$match": {"date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
                         {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}}],
        }},
        {"$unwind": "$l"},
        {"$replaceRoot": {"newRoot": "$l"}},
        {"$group": {"_id": "$date", "units": {"$sum": _PRESENT_UNITS}, "late_count": {"$sum": _IS_LATE}}},
        {"$project": {"_id": 0, "units": 1, "late_count": 1,
                      "day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        # zero row on day one so $densify has input even when the range holds no records
        {"$unionWith": {"pipeline": [{"$documents": [{"day": lo, "units": 0, "late_count": 0}]}]}},
        {"$group": {"_id": "$day", "units": {"$sum": "$units"}, "late_count": {"$sum": "$late_count"}}},
        {"$densify": {"field": "_id", "range": {"step": 1, "unit": "day", "bounds": [lo, hi]}}},
        {"$set": {"day": "$_id", "units": {"$ifNull": ["$units", 0]}, "late_count": {"$ifNull": ["$late_count", 0]}}},
        {"$lookup": {
            "from": "employees",
            "let": {"day_str": {"$dateToString": {"format": "%Y-%m-%d", "date": "$day"}}},
            "pipeline": [{"$match": {"department": department}},
                         {"$match": {"$expr": {"$lte": ["$joined_on", "$$day_str"]}}},
                         {"$count": "n"}],
            "as": "hc",
        }},
        {"$set": {"headcount": {"$ifNull": [{"$first": "$hc.n"}, 0]}, "dow": {"$dayOfWeek": "$day"}}},
        {"$set": {"is_working_day": {"$and": [{"$gte": ["$dow", 2]}, {"$lte": ["$dow", 6]}]}}},
        # attendance_rate in 1/10000ths, half-up: floor((2n + d) / 2d), n = 10000*units, d = 2*headcount
        {"$set": {"rate_int": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            {"$floor": {"$divide": [{"$add": [{"$multiply": ["$units", 20000]}, {"$multiply": ["$headcount", 2]}]},
                                    {"$multiply": ["$headcount", 4]}]}},
            None]}}},
        {"$set": {"rate_present": {"$cond": [{"$eq": ["$rate_int", None]}, 0, 1]}}},
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "win_sum": {"$sum": {"$ifNull": ["$rate_int", 0]}, "window": window},
            "win_n": {"$sum": "$rate_present", "window": window}}}},
        {"$project": {
            "_id": 0, "day": 1, "is_working_day": 1, "headcount": 1, "units": 1, "late_count": 1, "rate_int": 1,
            "moving_int": {"$cond": [
                {"$gt": ["$win_n", 0]},
                {"$floor": {"$divide": [{"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]},
                                        {"$multiply": ["$win_n", 2]}]}},
                None]}}},
        {"$sort": {"day": 1}},
    ]


# =========================================================================== #
# 7. Persistence
# =========================================================================== #
class Database:
    """Owns the MongoClient (created once at startup, closed at shutdown) and the index definitions."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: MongoClient = MongoClient(
            settings.mongo_uri,
            tz_aware=True,  # aware datetimes, never naive ones
            tzinfo=UTC,
            serverSelectionTimeoutMS=settings.server_selection_timeout_ms,
            maxPoolSize=settings.max_pool_size,
            retryWrites=True,
        )
        self.db = self.client[settings.mongo_db]
        self.employees: Collection = self.db["employees"]
        self.attendance: Collection = self.db["attendance_logs"]

    def ping(self) -> None:
        self.client.admin.command("ping")

    def ensure_indexes(self) -> None:
        """Idempotent; safe to run on every start against an already-seeded database."""
        self.employees.create_index("emp_code", unique=True, name="emp_code_unique")
        self.employees.create_index(IDX_DEPT_JOINED, name="department_joined_on")
        self.employees.create_index(IDX_JOINED, name="joined_on")
        self.attendance.create_index(IDX_EMP_DATE, unique=True, name="emp_code_date_unique")
        self.attendance.create_index(IDX_DATE_EMP, name="date_emp_code")
        self.attendance.create_index(IDX_STATUS_DATE, name="status_date_emp_code")
        self.attendance.create_index(IDX_EMP_PUNCH, name="emp_code_punch_in")

    def close(self) -> None:
        self.client.close()


class EmployeeRepository:
    def __init__(self, coll: Collection):
        self.coll = coll

    def insert(self, doc: dict) -> None:
        try:
            self.coll.insert_one(doc)  # the unique index arbitrates races
        except DuplicateKeyError:
            raise Conflict("emp_code already exists")

    def get(self, emp_code: str) -> Optional[dict]:
        return self.coll.find_one({"emp_code": emp_code})

    def list(self, department: Optional[str], page: int, page_size: int) -> tuple[list, int]:
        q = {"department": department} if department else {}
        cursor = self.coll.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
        if department:
            cursor = cursor.hint(IDX_DEPT_JOINED)
        return list(cursor), self.coll.count_documents(q)

    def department_exists(self, department: str) -> bool:
        return self.coll.count_documents({"department": department}, limit=1) > 0


class AttendanceRepository:
    def __init__(self, coll: Collection):
        self.coll = coll

    def insert(self, doc: dict) -> None:
        try:
            self.coll.insert_one(doc)  # unique (emp_code, date): exactly one concurrent insert wins
        except DuplicateKeyError:
            raise Conflict("already punched in for this date")

    def get(self, emp_code: str, day: str) -> Optional[dict]:
        return self.coll.find_one({"emp_code": emp_code, "date": day})

    def latest_punch_at_or_before(self, emp_code: str, ts: datetime) -> Optional[dict]:
        return self.coll.find_one({"emp_code": emp_code, "punch_in": {"$lte": ts}},
                                  sort=[("punch_in", DESCENDING)], hint=IDX_EMP_PUNCH)

    def close_if_open(self, rec_id: Any, fields: dict) -> bool:
        res = self.coll.update_one({"_id": rec_id, "punch_out": None}, {"$set": fields})
        return res.matched_count == 1

    def regularize_if_unchanged(self, cur: dict, new_vals: dict, entry: dict) -> bool:
        """Optimistic concurrency: only applies if status, punches and history length are as we read them,
        so a concurrent correction can never overwrite or drop another's history entry."""
        res = self.coll.update_one(
            {"_id": cur["_id"], "status": cur["status"], "punch_in": cur.get("punch_in"),
             "punch_out": cur.get("punch_out"),
             "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(cur.get("history") or [])]}},
            {"$set": new_vals, "$push": {"history": entry}},
        )
        return res.matched_count == 1

    def list(self, query: AttendanceQuery, page: int, page_size: int) -> tuple[list, int]:
        f, hint = query.to_filter(), query.hint()
        cursor = (self.coll.find(f, {"_id": 0}).hint(hint).sort(ATTENDANCE_SORT)
                  .skip((page - 1) * page_size).limit(page_size))
        return list(cursor), self.coll.count_documents(f, hint=hint)


class AnalyticsRepository:
    """Runs the aggregation pipelines; every analytic is computed inside MongoDB."""

    def __init__(self, database: Database):
        self.employees = database.employees
        self.attendance = database.attendance
        self.max_time_ms = database.settings.query_max_time_ms

    def _run(self, coll: Collection, pipeline: list, hint: list) -> list:
        return list(coll.aggregate(pipeline, hint=dict(hint), allowDiskUse=True, maxTimeMS=self.max_time_ms))

    def monthly(self, emp_code: str, first: date, last: date) -> dict:
        rows = self._run(self.attendance, monthly_pipeline(emp_code, first, last), IDX_EMP_DATE)
        return rows[0] if rows else {}

    def department_summary(self, first: date, last: date, department: Optional[str]) -> list:
        pipeline, hint = department_summary_pipeline(first, last, department)
        return self._run(self.employees, pipeline, hint)

    def late_leaderboard(self, first: date, last: date, limit: int, department: Optional[str]) -> list:
        return self._run(self.attendance, leaderboard_pipeline(first, last, limit, department), IDX_DATE_EMP)

    def department_trend(self, department: str, d_from: date, d_to: date) -> list:
        return self._run(self.employees, trend_pipeline(department, d_from, d_to), IDX_DEPT_JOINED)


# =========================================================================== #
# 8. Services (use-cases)
# =========================================================================== #
class EmployeeService:
    def __init__(self, employees: EmployeeRepository):
        self.employees = employees

    def create(self, body: EmployeeIn) -> dict:
        doc = body.model_dump()
        doc["created_at"] = now_ms_precision()
        self.employees.insert(doc)
        return employee_out(doc)

    def list(self, department: Optional[str], page: int, page_size: int) -> dict:
        items, total = self.employees.list(department, page, page_size)
        return {"items": [employee_out(d) for d in items], "total": total, "page": page, "page_size": page_size}


class AttendanceService:
    def __init__(self, employees: EmployeeRepository, attendance: AttendanceRepository):
        self.employees = employees
        self.attendance = attendance

    def _employee(self, emp_code: str) -> dict:
        emp = self.employees.get(emp_code)
        if emp is None:
            raise NotFound("employee not found")
        return emp

    @staticmethod
    def _shift(emp: dict) -> tuple[str, str]:
        return emp.get("shift_start", "09:30"), emp.get("shift_end", "18:30")

    def punch_in(self, body: PunchInIn) -> dict:
        emp = self._employee(body.emp_code)
        ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
        start, end = self._shift(emp)
        day = attendance_date(ts, start, end)
        doc = {
            "emp_code": body.emp_code, "date": day.isoformat(), "status": body.status,
            "punch_in": ts, "punch_out": None,
            **derived_fields(body.status, ts, None, start, end, day),
            "history": [],
        }
        self.attendance.insert(doc)
        return record_out(doc)

    def punch_out(self, body: PunchOutIn) -> dict:
        emp = self._employee(body.emp_code)
        ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
        rec = self.attendance.latest_punch_at_or_before(body.emp_code, ts)
        if rec is None:
            raise NotFound("no punch-in found")
        if rec.get("punch_out") is not None:
            raise Conflict("already punched out")
        punched_in = aware(rec["punch_in"]).replace(microsecond=0)
        if ts <= punched_in:
            raise Unprocessable("punched_at must be after punch_in")
        if ts - punched_in > MAX_SHIFT:
            raise Unprocessable("punch-out is more than 24 hours after punch-in")
        start, end = self._shift(emp)
        d = derived_fields(rec["status"], punched_in, ts, start, end, date.fromisoformat(rec["date"]))
        fields = {"punch_out": ts, "work_hours": d["work_hours"],
                  "overtime_minutes": d["overtime_minutes"], "half_day": d["half_day"]}
        if not self.attendance.close_if_open(rec["_id"], fields):
            raise Conflict("already punched out")  # lost the race to a concurrent punch-out
        rec.update(fields)
        return record_out(rec)

    def list(self, query: AttendanceQuery, page: int, page_size: int) -> dict:
        items, total = self.attendance.list(query, page, page_size)
        return {"items": [record_out(d) for d in items], "total": total, "page": page, "page_size": page_size}

    def regularize(self, emp_code: str, day: str, body: RegularizeIn) -> dict:
        emp = self._employee(emp_code)
        start, end = self._shift(emp)
        rec_day = date.fromisoformat(day)

        for _ in range(MAX_REGULARIZE_RETRIES):
            cur = self.attendance.get(emp_code, day)
            if cur is None:
                raise NotFound("no attendance record for that date")

            cur_in, cur_out = aware(cur.get("punch_in")), aware(cur.get("punch_out"))
            status = body.status or cur["status"]
            if status in ("ABSENT", "LEAVE"):
                if body.punch_in is not None or body.punch_out is not None:
                    raise Unprocessable(f"{status} records cannot carry punch times")
                new_in = new_out = None
            else:
                new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else cur_in
                new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else cur_out
                if new_in is None:
                    raise Unprocessable("a presence status requires a punch_in")
                if body.punch_in is not None and attendance_date(new_in, start, end) != rec_day:
                    raise Unprocessable("punch_in must fall on the record's attendance date")
                if new_out is not None and new_out <= new_in:
                    raise Unprocessable("punch_out must be after punch_in")
                if new_out is not None and new_out - new_in > MAX_SHIFT:
                    raise Unprocessable("punch_out is more than 24 hours after punch_in")

            if (status, new_in, new_out) == (cur["status"], cur_in, cur_out):
                raise Unprocessable("the request changes nothing")

            new_vals = {"status": status, "punch_in": new_in, "punch_out": new_out,
                        **derived_fields(status, new_in, new_out, start, end, rec_day)}
            old_vals = {
                "status": cur["status"], "punch_in": cur_in, "punch_out": cur_out,
                "work_hours": cur.get("work_hours"), "late_minutes": cur.get("late_minutes") or 0,
                "overtime_minutes": cur.get("overtime_minutes") or 0, "half_day": bool(cur.get("half_day") or False),
            }
            entry = {
                "at": now_ms_precision(), "by": body.regularized_by, "reason": body.reason,
                "changes": {k: {"from": old_vals[k], "to": v} for k, v in new_vals.items() if old_vals[k] != v},
            }
            if self.attendance.regularize_if_unchanged(cur, new_vals, entry):
                cur.update(new_vals)
                cur["history"] = list(cur.get("history") or []) + [entry]
                return record_out(cur)
        raise Conflict("concurrent modification, please retry")


class AnalyticsService:
    def __init__(self, employees: EmployeeRepository, analytics: AnalyticsRepository):
        self.employees = employees
        self.analytics = analytics

    def employee_monthly(self, emp_code: str, month: str) -> dict:
        first, last = parse_month(month)
        emp = self.employees.get(emp_code)
        if emp is None:
            raise NotFound("employee not found")
        agg = self.analytics.monthly(emp_code, first, last)
        units = agg.get("present_units", 0)
        joined = date.fromisoformat(emp["joined_on"]) if emp.get("joined_on") else first
        working_days = weekdays_between(max(first, joined), last) if joined <= last else 0
        pct = half_up(Decimal(units) * 50 / Decimal(working_days), 2) if working_days > 0 else None
        return {
            "emp_code": emp_code, "month": month, "working_days": working_days,
            "present_days": units / 2, "leave_days": agg.get("leave_days", 0),
            "late_count": agg.get("late_count", 0), "total_late_minutes": agg.get("total_late_minutes", 0),
            "total_overtime_minutes": agg.get("total_overtime_minutes", 0), "attendance_pct": pct,
        }

    def department_summary(self, month: str, department: Optional[str]) -> dict:
        first, last = parse_month(month)
        items = []
        for r in self.analytics.department_summary(first, last, department):
            avg = half_up(Decimal(r["wh_cents"]) / Decimal(r["wh_n"]) / Decimal(100), 2) if r["wh_n"] else None
            items.append({
                "department": r["_id"], "headcount": r["headcount"], "present_days": r["present_units"] / 2,
                "avg_work_hours": avg, "late_count": r["late_count"], "total_late_minutes": r["total_late_minutes"],
                "leave_count": r["leave_count"], "on_duty_count": r["on_duty_count"],
            })
        return {"month": month, "items": items}

    def late_leaderboard(self, month: str, limit: int, department: Optional[str]) -> dict:
        first, last = parse_month(month)
        rows = self.analytics.late_leaderboard(first, last, limit, department)
        keys = ("rank", "emp_code", "name", "department", "total_late_minutes", "late_count")
        return {"month": month, "items": [{k: r[k] for k in keys} for r in rows]}

    def department_trend(self, department: str, from_: str, to: str) -> dict:
        d_from, d_to = parse_trend_range(from_, to)
        if not self.employees.department_exists(department):
            raise NotFound("unknown department")
        items = [{
            "date": r["day"].strftime("%Y-%m-%d"),
            "is_working_day": bool(r["is_working_day"]),
            "headcount": r["headcount"],
            "present_count": r["units"] / 2,
            "late_count": r["late_count"],
            "attendance_rate": None if r.get("rate_int") is None else r["rate_int"] / 10000,
            "moving_avg_7d": None if r.get("moving_int") is None else r["moving_int"] / 10000,
        } for r in self.analytics.department_trend(department, d_from, d_to)]
        return {"department": department, "items": items}


ExplainName = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


class ExplainService:
    """Builds each endpoint's main query with the same builders the endpoint uses, then explains it."""

    def __init__(self, database: Database):
        self.database = database

    def explain(self, endpoint: str, p: dict) -> dict:
        db = self.database
        if endpoint == "attendance_list":
            q = AttendanceQuery.parse(p["emp_code"], p["date_from"], p["date_to"], p["status"])
            collection = db.attendance.name
            command = {"find": collection, "filter": q.to_filter(), "sort": dict(ATTENDANCE_SORT),
                       "skip": (p["page"] - 1) * p["page_size"], "limit": p["page_size"], "hint": dict(q.hint())}
        else:
            if endpoint == "employee_monthly":
                require(p["emp_code"], "emp_code")
                first, last = parse_month(require(p["month"], "month"))
                collection, pipeline, hint = db.attendance.name, monthly_pipeline(p["emp_code"], first, last), IDX_EMP_DATE
            elif endpoint == "department_summary":
                first, last = parse_month(require(p["month"], "month"))
                pipeline, hint = department_summary_pipeline(first, last, p["department"])
                collection = db.employees.name
            elif endpoint == "late_leaderboard":
                first, last = parse_month(require(p["month"], "month"))
                pipeline = leaderboard_pipeline(first, last, p["limit"], p["department"])
                collection, hint = db.attendance.name, IDX_DATE_EMP
            else:  # department_trend
                require(p["department"], "department")
                d_from, d_to = parse_trend_range(require(p["from"], "from"), require(p["to"], "to"))
                pipeline = trend_pipeline(p["department"], d_from, d_to)
                collection, hint = db.employees.name, IDX_DEPT_JOINED
            command = {"aggregate": collection, "pipeline": pipeline, "cursor": {},
                       "hint": dict(hint), "allowDiskUse": True}

        raw = db.db.command({"explain": command, "verbosity": "executionStats"})
        explain = json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))
        return {"endpoint": endpoint, "collection": collection, "explain": explain}


# =========================================================================== #
# 9. API: dependencies and thin routers
# =========================================================================== #
def get_database(request: Request) -> Database:
    return request.app.state.database


DbDep = Annotated[Database, Depends(get_database)]


def get_employee_service(database: DbDep) -> EmployeeService:
    return EmployeeService(EmployeeRepository(database.employees))


def get_attendance_service(database: DbDep) -> AttendanceService:
    return AttendanceService(EmployeeRepository(database.employees), AttendanceRepository(database.attendance))


def get_analytics_service(database: DbDep) -> AnalyticsService:
    return AnalyticsService(EmployeeRepository(database.employees), AnalyticsRepository(database))


EmployeesDep = Annotated[EmployeeService, Depends(get_employee_service)]
AttendanceDep = Annotated[AttendanceService, Depends(get_attendance_service)]
AnalyticsDep = Annotated[AnalyticsService, Depends(get_analytics_service)]

system_router = APIRouter(tags=["system"])
employees_router = APIRouter(prefix="/employees", tags=["employees"])
attendance_router = APIRouter(prefix="/attendance", tags=["attendance"])
analytics_router = APIRouter(prefix="/analytics", tags=["analytics"])
admin_router = APIRouter(prefix="/admin", tags=["admin"])


@system_router.get("/health")
def health(database: DbDep):
    try:
        database.ping()
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@employees_router.post("", status_code=201)
def create_employee(body: EmployeeIn, service: EmployeesDep):
    return service.create(body)


@employees_router.get("")
def list_employees(service: EmployeesDep, department: Optional[str] = None,
                   page: PageQ = 1, page_size: PageSizeQ = 20):
    return service.list(department, page, page_size)


@attendance_router.post("/punch-in", status_code=201)
def punch_in(body: PunchInIn, service: AttendanceDep):
    return service.punch_in(body)


@attendance_router.post("/punch-out")
def punch_out(body: PunchOutIn, service: AttendanceDep):
    return service.punch_out(body)


@attendance_router.get("")
def list_attendance(
    service: AttendanceDep,
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[StatusT] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    return service.list(AttendanceQuery.parse(emp_code, date_from, date_to, status), page, page_size)


@attendance_router.patch("/{emp_code}/{date}")
def regularize_attendance(emp_code: str, date: str, body: RegularizeIn, service: AttendanceDep):
    parse_date(date, "date", "path")
    return service.regularize(emp_code, date, body)


@analytics_router.get("/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthQ, service: AnalyticsDep):
    return service.employee_monthly(emp_code, month)


@analytics_router.get("/departments/summary")
def department_summary(month: MonthQ, service: AnalyticsDep, department: Optional[str] = None):
    return service.department_summary(month, department)


@analytics_router.get("/leaderboard/late")
def late_leaderboard(month: MonthQ, service: AnalyticsDep, limit: LimitQ = 10, department: Optional[str] = None):
    return service.late_leaderboard(month, limit, department)


@analytics_router.get("/departments/{department}/trend")
def department_trend(department: str, from_: Annotated[str, Query(alias="from")], to: str, service: AnalyticsDep):
    return service.department_trend(department, from_, to)


@admin_router.get("/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainName,
    database: DbDep,
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: LimitQ = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[StatusT] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    params = {"emp_code": emp_code, "month": month, "department": department, "limit": limit,
              "date_from": date_from, "date_to": date_to, "status": status, "from": from_, "to": to,
              "page": page, "page_size": page_size}
    return ExplainService(database).explain(endpoint, params)


# =========================================================================== #
# 10. Application factory
# =========================================================================== #
@asynccontextmanager
async def lifespan(app: FastAPI):
    database = Database(Settings.from_env())
    app.state.database = database
    deadline = _time.monotonic() + 15
    while True:  # tolerate MongoDB still starting; /health reports 503 until it answers
        try:
            database.ensure_indexes()
            break
        except PyMongoError:
            if _time.monotonic() > deadline:
                break
            _time.sleep(1)
    try:
        yield
    finally:
        database.close()


def create_app() -> FastAPI:
    application = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)

    @application.exception_handler(DomainError)
    async def domain_error_handler(_: Request, exc: DomainError):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @application.exception_handler(PyMongoError)
    async def mongo_error_handler(_: Request, exc: PyMongoError):
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})

    for router in (system_router, employees_router, attendance_router, analytics_router, admin_router):
        application.include_router(router)
    return application


app = create_app()
