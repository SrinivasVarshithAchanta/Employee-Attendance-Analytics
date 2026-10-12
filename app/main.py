"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (real environment variables win over a local .env)

Layout of this file: configuration -> time/rule helpers (R1-R5, R8) -> serialisation ->
validation helpers -> models -> query/pipeline builders (shared by endpoints and /admin/explain)
-> endpoints.
"""
import calendar
import json
import os
import re
import time as _time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, StrictInt, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # override=False: variables already set in the environment win

client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,  # PyMongo hands back aware (UTC) datetimes, never naive ones
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=100,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

PRESENT_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]
ALL_STATUSES = ["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
StatusT = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatusT = Literal["PRESENT", "WFH", "ON_DUTY"]

MS_MIN = 100_000_000_000
MS_MAX = 4_102_444_800_000
EpochMs = Annotated[StrictInt, Field(ge=MS_MIN, le=MS_MAX)]

GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_HOURS = Decimal("4.50")
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

# index used for each query shape; also passed as a hint so plans are deterministic
IDX_EMP_DATE = [("emp_code", ASCENDING), ("date", DESCENDING)]
IDX_DATE_EMP = [("date", DESCENDING), ("emp_code", ASCENDING)]
IDX_STATUS_DATE = [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)]
IDX_EMP_PUNCH = [("emp_code", ASCENDING), ("punch_in", DESCENDING)]
IDX_DEPT_JOINED = [("department", ASCENDING), ("joined_on", ASCENDING)]
IDX_JOINED = [("joined_on", ASCENDING)]


def ensure_indexes() -> None:
    employees.create_index("emp_code", unique=True, name="emp_code_unique")
    employees.create_index(IDX_DEPT_JOINED, name="department_joined_on")
    employees.create_index(IDX_JOINED, name="joined_on")
    logs.create_index(IDX_EMP_DATE, unique=True, name="emp_code_date_unique")
    logs.create_index(IDX_DATE_EMP, name="date_emp_code")
    logs.create_index(IDX_STATUS_DATE, name="status_date_emp_code")
    logs.create_index(IDX_EMP_PUNCH, name="emp_code_punch_in")


@asynccontextmanager
async def lifespan(_: FastAPI):
    deadline = _time.monotonic() + 15
    while True:
        try:
            ensure_indexes()
            break
        except PyMongoError:
            if _time.monotonic() > deadline:
                break  # /health keeps answering 503 until MongoDB is reachable
            _time.sleep(1)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# Time + business-rule helpers (R1 - R5, R8)
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: treat naive values as UTC
        dt = dt.replace(tzinfo=UTC)
    return calendar.timegm(dt.utctimetuple()) * 1000 + dt.microsecond // 1000


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def now_ms_precision() -> datetime:
    now = datetime.now(UTC)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _hm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return shift_end <= shift_start  # zero-padded HH:MM compares lexicographically


def shift_bounds(day: date, shift_start: str, shift_end: str) -> tuple[datetime, datetime]:
    """IST start and end of the shift that belongs to attendance date `day` (R1)."""
    sh, sm = _hm(shift_start)
    eh, em = _hm(shift_end)
    start = datetime(day.year, day.month, day.day, sh, sm, tzinfo=IST)
    end = datetime(day.year, day.month, day.day, eh, em, tzinfo=IST)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    return start, end


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date:
    """R1: IST calendar date of the punch-in; overnight early-morning punches belong to the previous day."""
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
    """R4: seconds / 3600, 2 decimals, half-up (exact decimal arithmetic)."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_half_day(work_hours: Optional[float]) -> bool:
    """R5: rounded work_hours below 4.50."""
    return work_hours is not None and Decimal(repr(work_hours)) < HALF_DAY_HOURS


def round_half_up(value: float, places: int) -> float:
    """R8 helper. Strips float noise first, then rounds half-up like a calculator would."""
    exp = Decimal(1).scaleb(-places)
    return float(Decimal(repr(round(value, 9))).quantize(exp, rounding=ROUND_HALF_UP))


def derived_fields(status: str, punch_in, punch_out, shift_start: str, shift_end: str, day: date) -> dict:
    """The four server-computed fields for a record in its final state."""
    if status not in PRESENT_STATUSES or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    start, end = shift_bounds(day, shift_start, shift_end)
    out = {
        "work_hours": None,
        "late_minutes": compute_late_minutes(punch_in, start),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        out["work_hours"] = compute_work_hours(punch_in, punch_out)
        out["overtime_minutes"] = compute_overtime(punch_out, end)
        out["half_day"] = compute_half_day(out["work_hours"])
    return out


def weekdays_between(first: date, last: date) -> int:
    """Number of Mon-Fri days in [first, last] (R7)."""
    count, day = 0, first
    while day <= last:
        if day.weekday() < 5:
            count += 1
        day += timedelta(days=1)
    return count


# --------------------------------------------------------------------------- #
# Serialisation (BSON -> API)
# --------------------------------------------------------------------------- #
def _ms_if_dt(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def history_out(entry: dict) -> dict:
    changes = {}
    for key, change in (entry.get("changes") or {}).items():
        changes[key] = {"from": _ms_if_dt(change.get("from")), "to": _ms_if_dt(change.get("to"))}
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
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


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


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


def require(value: Any, name: str):
    if value is None or value == "":
        _invalid("query", name, "field required", value)
    return value


PageQ = Annotated[int, Query(ge=1, le=1_000_000_000)]
PageSizeQ = Annotated[int, Query(ge=1, le=100)]
MonthQ = Annotated[str, Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")]
LimitQ = Annotated[int, Query(ge=1, le=50)]


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
HM_PATTERN = r"^([01]\d|2[0-3]):[0-5]\d$"


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


# --------------------------------------------------------------------------- #
# Query / pipeline builders (shared by the endpoints and /admin/explain)
# --------------------------------------------------------------------------- #
def attendance_filter(emp_code, date_from, date_to, status) -> dict:
    q: dict = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status:
        q["status"] = status
    return q


def attendance_hint(q: dict) -> list:
    if "emp_code" in q:
        return IDX_EMP_DATE
    if "status" in q:
        return IDX_STATUS_DATE
    return IDX_DATE_EMP


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


def validate_attendance_params(emp_code, date_from, date_to, status):
    d_from = parse_date(date_from, "date_from") if date_from else None
    d_to = parse_date(date_to, "date_to") if date_to else None
    if d_from and d_to and d_from > d_to:
        _invalid("query", "date_from", "date_from must not be after date_to", date_from)
    return attendance_filter(emp_code, date_from, date_to, status)


_DAY_OF_WEEK = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "onError": None}}}
_IS_WEEKDAY = {"$and": [{"$gte": [_DAY_OF_WEEK, 2]}, {"$lte": [_DAY_OF_WEEK, 6]}]}  # Mon(2)..Fri(6)
# present value in half-day units: full day 2, half day 1 (integers keep sums exact)
_PRESENT_UNITS = {
    "$cond": [
        {"$in": ["$status", PRESENT_STATUSES]},
        {"$cond": [{"$ifNull": ["$half_day", False]}, 1, 2]},
        0,
    ]
}
_WEEKDAY_UNITS = {"$cond": [_IS_WEEKDAY, _PRESENT_UNITS, 0]}
_LATE = {"$ifNull": ["$late_minutes", 0]}
_IS_LATE = {"$cond": [{"$gt": [_LATE, 0]}, 1, 0]}
_OVERTIME = {"$ifNull": ["$overtime_minutes", 0]}
_WH_CENTS = {
    "$cond": [
        {"$and": [{"$in": ["$status", PRESENT_STATUSES]}, {"$isNumber": "$work_hours"}]},
        {"$round": [{"$multiply": ["$work_hours", 100]}, 0]},
        0,
    ]
}
_WH_COUNT = {"$cond": [{"$and": [{"$in": ["$status", PRESENT_STATUSES]}, {"$isNumber": "$work_hours"}]}, 1, 0]}


def monthly_pipeline(emp_code: str, first: date, last: date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_units": {"$sum": _WEEKDAY_UNITS},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": _IS_LATE},
                "total_late_minutes": {"$sum": _LATE},
                "total_overtime_minutes": {"$sum": _OVERTIME},
            }
        },
    ]


def department_summary_query(first: date, last: date, department: Optional[str]) -> tuple[list, list]:
    """Employees first (so people with zero logs still count), logs folded in per employee."""
    emp_match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department:
        emp_match["department"] = department
    per_employee_logs = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_units": {"$sum": _WEEKDAY_UNITS},
                "wh_cents": {"$sum": _WH_CENTS},
                "wh_n": {"$sum": _WH_COUNT},
                "late_count": {"$sum": _IS_LATE},
                "late_minutes": {"$sum": _LATE},
                "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
            }
        },
    ]

    def first_of(field: str) -> dict:
        return {"$ifNull": [{"$first": f"$l.{field}"}, 0]}

    pipeline = [
        {"$match": emp_match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": per_employee_logs,
                "as": "l",
            }
        },
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_units": {"$sum": first_of("present_units")},
                "wh_cents": {"$sum": first_of("wh_cents")},
                "wh_n": {"$sum": first_of("wh_n")},
                "late_count": {"$sum": first_of("late_count")},
                "total_late_minutes": {"$sum": first_of("late_minutes")},
                "leave_count": {"$sum": first_of("leave_count")},
                "on_duty_count": {"$sum": first_of("on_duty_count")},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    hint = IDX_DEPT_JOINED if department else IDX_JOINED
    return pipeline, hint


def leaderboard_pipeline(first: date, last: date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {"_id": "$emp_code", "total": {"$sum": _LATE}, "late_count": {"$sum": _IS_LATE}}},
        {"$match": {"total": {"$gt": 0}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},
    ]
    if department:
        pipeline.append({"$match": {"e.department": department}})
    pipeline += [
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$e.name",
                "department": "$e.department",
                "total_late_minutes": "$total",
                "late_count": 1,
            }
        },
    ]
    return pipeline


def trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    """One row per calendar day in [d_from, d_to]; all arithmetic in integers so rounding is exact."""
    lo = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    hi = datetime(d_to.year, d_to.month, d_to.day, tzinfo=UTC) + timedelta(days=1)  # upper bound is exclusive
    return [
        {"$match": {"department": department}},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
                    {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
                ],
                "as": "l",
            }
        },
        {"$unwind": "$l"},
        {"$replaceRoot": {"newRoot": "$l"}},
        {
            "$group": {
                "_id": "$date",
                "units": {"$sum": _PRESENT_UNITS},
                "late_count": {"$sum": _IS_LATE},
            }
        },
        {
            "$project": {
                "_id": 0,
                "day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}},
                "units": 1,
                "late_count": 1,
            }
        },
        # a zero row on day one so $densify has data even when the range has no records at all
        {"$unionWith": {"pipeline": [{"$documents": [{"day": lo, "units": 0, "late_count": 0}]}]}},
        {"$group": {"_id": "$day", "units": {"$sum": "$units"}, "late_count": {"$sum": "$late_count"}}},
        {"$densify": {"field": "_id", "range": {"step": 1, "unit": "day", "bounds": [lo, hi]}}},
        {"$set": {"day": "$_id", "units": {"$ifNull": ["$units", 0]}, "late_count": {"$ifNull": ["$late_count", 0]}}},
        {
            "$lookup": {
                "from": "employees",
                "let": {"day_str": {"$dateToString": {"format": "%Y-%m-%d", "date": "$day"}}},
                "pipeline": [
                    {"$match": {"department": department}},
                    {"$match": {"$expr": {"$lte": ["$joined_on", "$$day_str"]}}},
                    {"$count": "n"},
                ],
                "as": "hc",
            }
        },
        {
            "$set": {
                "headcount": {"$ifNull": [{"$first": "$hc.n"}, 0]},
                "dow": {"$dayOfWeek": "$day"},
            }
        },
        {
            "$set": {
                "is_working_day": {"$and": [{"$gte": ["$dow", 2]}, {"$lte": ["$dow", 6]}]},
            }
        },
        {
            # attendance_rate in 1/10000ths, half-up: floor((2n + d) / 2d) with n = 10000*units, d = 2*headcount
            "$set": {
                "rate_int": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": ["$units", 20000]}, {"$multiply": ["$headcount", 2]}]},
                                    {"$multiply": ["$headcount", 4]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {"$set": {"rate_present": {"$cond": [{"$eq": ["$rate_int", None]}, 0, 1]}}},
        {
            "$setWindowFields": {
                "sortBy": {"day": 1},
                "output": {
                    "win_sum": {"$sum": {"$ifNull": ["$rate_int", 0]}, "window": {"documents": [-6, 0]}},
                    "win_n": {"$sum": "$rate_present", "window": {"documents": [-6, 0]}},
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "day": 1,
                "is_working_day": 1,
                "headcount": 1,
                "units": 1,
                "late_count": 1,
                "rate_int": 1,
                "moving_int": {
                    "$cond": [
                        {"$gt": ["$win_n", 0]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]},
                                    {"$multiply": ["$win_n", 2]},
                                ]
                            }
                        },
                        None,
                    ]
                },
            }
        },
        {"$sort": {"day": 1}},
    ]


def run_aggregate(collection, pipeline: list, hint: Optional[list] = None) -> list:
    kwargs: dict = {"allowDiskUse": True}
    if hint:
        kwargs["hint"] = dict(hint)
    return list(collection.aggregate(pipeline, **kwargs))


# --------------------------------------------------------------------------- #
# System + employees
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_ms_precision()
    try:
        employees.insert_one(doc)  # the unique index decides the winner of a race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: PageQ = 1, page_size: PageSizeQ = 20):
    q = {"department": department} if department else {}
    cursor = employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    if department:
        cursor = cursor.hint(IDX_DEPT_JOINED)
    items = [employee_out(d) for d in cursor]
    return {"items": items, "total": employees.count_documents(q), "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# Attendance
# --------------------------------------------------------------------------- #
def _get_employee(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = _get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    shift_start, shift_end = emp.get("shift_start", "09:30"), emp.get("shift_end", "18:30")
    day = attendance_date(ts, shift_start, shift_end)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derived_fields(body.status, ts, None, shift_start, shift_end, day),
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): exactly one concurrent request wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = _get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    rec = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}},
        sort=[("punch_in", DESCENDING)],
        hint=IDX_EMP_PUNCH,
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    punched_in = aware(rec["punch_in"]).replace(microsecond=0)
    if ts <= punched_in:
        raise HTTPException(422, "punched_at must be after punch_in")
    if ts - punched_in > MAX_SHIFT:
        raise HTTPException(422, "punch-out is more than 24 hours after punch-in")
    shift_start, shift_end = emp.get("shift_start", "09:30"), emp.get("shift_end", "18:30")
    derived = derived_fields(rec["status"], punched_in, ts, shift_start, shift_end, date.fromisoformat(rec["date"]))
    changed = {"punch_out": ts, **{k: derived[k] for k in ("work_hours", "overtime_minutes", "half_day")}}
    res = logs.update_one({"_id": rec["_id"], "punch_out": None}, {"$set": changed})
    if res.matched_count == 0:  # somebody else closed it between our read and write
        raise HTTPException(409, "already punched out")
    rec.update(changed)
    return record_out(rec)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[StatusT] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    q = validate_attendance_params(emp_code, date_from, date_to, status)
    hint = attendance_hint(q)
    cursor = (
        logs.find(q, {"_id": 0})
        .hint(hint)
        .sort(ATTENDANCE_SORT)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [record_out(d) for d in cursor]
    total = logs.count_documents(q, hint=hint)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{day}")
def regularize(emp_code: str, day: str, body: RegularizeIn):
    parse_date(day, "date", "path")
    emp = _get_employee(emp_code)
    shift_start, shift_end = emp.get("shift_start", "09:30"), emp.get("shift_end", "18:30")
    rec_day = date.fromisoformat(day)

    for _ in range(5):  # optimistic concurrency: retry when another writer got in first
        cur = logs.find_one({"emp_code": emp_code, "date": day})
        if cur is None:
            raise HTTPException(404, "no attendance record for that date")

        cur_in, cur_out = aware(cur.get("punch_in")), aware(cur.get("punch_out"))
        new_status = body.status or cur["status"]
        if new_status in ("ABSENT", "LEAVE"):
            if body.punch_in is not None or body.punch_out is not None:
                raise HTTPException(422, f"{new_status} records cannot carry punch times")
            new_in = new_out = None
        else:
            new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else cur_in
            new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else cur_out
            if new_in is None:
                raise HTTPException(422, "a presence status requires a punch_in")
            if body.punch_in is not None and attendance_date(new_in, shift_start, shift_end) != rec_day:
                raise HTTPException(422, "punch_in must fall on the record's attendance date")
            if new_out is not None:
                if new_out <= new_in:
                    raise HTTPException(422, "punch_out must be after punch_in")
                if new_out - new_in > MAX_SHIFT:
                    raise HTTPException(422, "punch_out is more than 24 hours after punch_in")

        if (new_status, new_in, new_out) == (cur["status"], cur_in, cur_out):
            raise HTTPException(422, "the request changes nothing")

        new_vals = {
            "status": new_status,
            "punch_in": new_in,
            "punch_out": new_out,
            **derived_fields(new_status, new_in, new_out, shift_start, shift_end, rec_day),
        }
        old_vals = {
            "status": cur["status"],
            "punch_in": cur_in,
            "punch_out": cur_out,
            "work_hours": cur.get("work_hours"),
            "late_minutes": cur.get("late_minutes") or 0,
            "overtime_minutes": cur.get("overtime_minutes") or 0,
            "half_day": bool(cur.get("half_day") or False),
        }
        changes = {k: {"from": old_vals[k], "to": v} for k, v in new_vals.items() if old_vals[k] != v}
        entry = {
            "at": now_ms_precision(),
            "by": body.regularized_by,
            "reason": body.reason,
            "changes": changes,
        }
        history_len = len(cur.get("history") or [])
        res = logs.update_one(
            {
                "_id": cur["_id"],
                "status": cur["status"],
                "punch_in": cur.get("punch_in"),
                "punch_out": cur.get("punch_out"),
                "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, history_len]},
            },
            {"$set": new_vals, "$push": {"history": entry}},
        )
        if res.matched_count:
            cur.update(new_vals)
            cur["history"] = list(cur.get("history") or []) + [entry]
            return record_out(cur)
    raise HTTPException(409, "concurrent modification, please retry")


# --------------------------------------------------------------------------- #
# Analytics
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthQ):
    first, last = parse_month(month)
    emp = _get_employee(emp_code)
    rows = run_aggregate(logs, monthly_pipeline(emp_code, first, last), IDX_EMP_DATE)
    agg = rows[0] if rows else {}
    units = agg.get("present_units", 0)

    joined = date.fromisoformat(emp["joined_on"]) if emp.get("joined_on") else first
    working_days = weekdays_between(max(first, joined), last) if joined <= last else 0
    attendance_pct = None
    if working_days > 0:
        pct = Decimal(units) * 50 / Decimal(working_days)  # units/2 / days * 100
        attendance_pct = float(pct.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": units / 2,
        "leave_days": agg.get("leave_days", 0),
        "late_count": agg.get("late_count", 0),
        "total_late_minutes": agg.get("total_late_minutes", 0),
        "total_overtime_minutes": agg.get("total_overtime_minutes", 0),
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthQ, department: Optional[str] = None):
    first, last = parse_month(month)
    pipeline, hint = department_summary_query(first, last, department)
    items = []
    for row in run_aggregate(employees, pipeline, hint):
        avg = None
        if row["wh_n"] > 0:
            avg = float(
                (Decimal(row["wh_cents"]) / Decimal(row["wh_n"]) / Decimal(100)).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            )
        items.append(
            {
                "department": row["_id"],
                "headcount": row["headcount"],
                "present_days": row["present_units"] / 2,
                "avg_work_hours": avg,
                "late_count": row["late_count"],
                "total_late_minutes": row["total_late_minutes"],
                "leave_count": row["leave_count"],
                "on_duty_count": row["on_duty_count"],
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthQ, limit: LimitQ = 10, department: Optional[str] = None):
    first, last = parse_month(month)
    rows = run_aggregate(logs, leaderboard_pipeline(first, last, limit, department), IDX_DATE_EMP)
    items = [
        {
            "rank": r["rank"],
            "emp_code": r["emp_code"],
            "name": r["name"],
            "department": r["department"],
            "total_late_minutes": r["total_late_minutes"],
            "late_count": r["late_count"],
        }
        for r in rows
    ]
    return {"month": month, "items": items}


def _trend_range(from_: str, to: str) -> tuple[date, date]:
    d_from, d_to = parse_date(from_, "from"), parse_date(to, "to")
    if d_to < d_from:
        _invalid("query", "to", "to must not be before from", to)
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        _invalid("query", "to", f"range must not exceed {MAX_TREND_DAYS} days", to)
    return d_from, d_to


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: Annotated[str, Query(alias="from")], to: str):
    d_from, d_to = _trend_range(from_, to)
    if employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")
    rows = run_aggregate(employees, trend_pipeline(department, d_from, d_to), IDX_DEPT_JOINED)
    items = []
    for r in rows:
        working = bool(r["is_working_day"])
        items.append(
            {
                "date": r["day"].strftime("%Y-%m-%d"),
                "is_working_day": working,
                "headcount": r["headcount"],
                "present_count": r["units"] / 2,
                "late_count": r["late_count"],
                "attendance_rate": None if r.get("rate_int") is None else r["rate_int"] / 10000,
                "moving_avg_7d": None if r.get("moving_int") is None else r["moving_int"] / 10000,
            }
        )
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# Part C: explain
# --------------------------------------------------------------------------- #
ExplainName = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainName,
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
    if endpoint == "attendance_list":
        q = validate_attendance_params(emp_code, date_from, date_to, status)
        command = {
            "find": logs.name,
            "filter": q,
            "sort": dict(ATTENDANCE_SORT),
            "skip": (page - 1) * page_size,
            "limit": page_size,
            "hint": dict(attendance_hint(q)),
        }
        collection = logs.name
    else:
        if endpoint == "employee_monthly":
            require(emp_code, "emp_code")
            first, last = parse_month(require(month, "month"))
            collection, pipeline, hint = logs.name, monthly_pipeline(emp_code, first, last), IDX_EMP_DATE
        elif endpoint == "department_summary":
            first, last = parse_month(require(month, "month"))
            pipeline, hint = department_summary_query(first, last, department)
            collection = employees.name
        elif endpoint == "late_leaderboard":
            first, last = parse_month(require(month, "month"))
            collection, pipeline, hint = logs.name, leaderboard_pipeline(first, last, limit, department), IDX_DATE_EMP
        else:  # department_trend
            require(department, "department")
            d_from, d_to = _trend_range(require(from_, "from"), require(to, "to"))
            collection, pipeline, hint = employees.name, trend_pipeline(department, d_from, d_to), IDX_DEPT_JOINED
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}, "hint": dict(hint), "allowDiskUse": True}

    raw = db.command({"explain": command, "verbosity": "executionStats"})
    explain = json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection, "explain": explain}
