"""Black-box tests: boots uvicorn against a throwaway database and drives it over HTTP.

Needs a MongoDB 6+ at MONGO_URI (default mongodb://localhost:27017). Run: pytest -q
"""
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from pymongo import MongoClient

IST = timezone(timedelta(hours=5, minutes=30))
URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB = "att_pytest"
PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"


def ist(y, mo, d, h=0, mi=0, s=0, ms=0) -> int:
    return int(datetime(y, mo, d, h, mi, s, tzinfo=IST).timestamp()) * 1000 + ms


@pytest.fixture(scope="session")
def server():
    env = {**os.environ, "MONGO_URI": URI, "MONGO_DB": DB}
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(PORT)],
        env=env,
        cwd=os.path.dirname(os.path.dirname(__file__)),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(60):
        try:
            if httpx.get(f"{BASE}/health", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.25)
    else:
        proc.kill()
        pytest.fail("server did not become healthy")
    yield
    proc.terminate()


@pytest.fixture()
def c(server):
    mongo = MongoClient(URI)
    mongo[DB].attendance_logs.delete_many({})
    mongo[DB].employees.delete_many({})
    with httpx.Client(base_url=BASE, timeout=20) as client:
        client.mongo = mongo[DB]
        yield client
    mongo.close()


def emp(c, code="EMP0001", **kw):
    body = {"emp_code": code, "name": "N " + code, "email": f"{code}@x.com", "department": "Eng",
            "joined_on": "2026-01-05", **kw}
    r = c.post("/employees", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def punch_in(c, code, at, **kw):
    return c.post("/attendance/punch-in", json={"emp_code": code, "punched_at": at, **kw})


def punch_out(c, code, at):
    return c.post("/attendance/punch-out", json={"emp_code": code, "punched_at": at})


def seed_log(c, code, day, status="PRESENT", late=0, ot=0, wh=None, half=None, pin=None, pout=None):
    doc = {"emp_code": code, "date": day, "status": status, "punch_in": pin, "punch_out": pout,
           "work_hours": wh, "late_minutes": late, "overtime_minutes": ot, "history": []}
    if half is not None:
        doc["half_day"] = half
    c.mongo.attendance_logs.insert_one(doc)


# ------------------------------------------------------------------ employees
def test_employee_crud_and_validation(c):
    r = emp(c)
    assert r["shift_start"] == "09:30" and isinstance(r["created_at"], int) and "_id" not in r
    assert c.post("/employees", json={**r, "created_at": 1}).status_code == 409
    bad = {"emp_code": "E1", "name": "x", "email": "a@b.co", "department": "d", "joined_on": "2026-01-01"}
    assert c.post("/employees", json=bad).status_code == 422
    ok = {**bad, "emp_code": "EMP0009"}
    assert c.post("/employees", json={**ok, "shift_start": "10:00", "shift_end": "10:00"}).status_code == 422
    assert c.post("/employees", json={**ok, "joined_on": "2026-02-30"}).status_code == 422
    assert c.post("/employees", json={**ok, "shift_start": "25:00"}).status_code == 422


def test_employee_list_pagination_total_and_sort(c):
    for i in range(5):
        emp(c, f"EMP000{i}", department="A" if i % 2 else "B")
    r = c.get("/employees", params={"department": "A", "page_size": 1, "page": 2}).json()
    assert r["total"] == 2 and r["items"][0]["emp_code"] == "EMP0003"
    assert c.get("/employees", params={"page_size": 101}).status_code == 422
    assert c.get("/employees", params={"page": 0}).status_code == 422
    assert [e["emp_code"] for e in c.get("/employees").json()["items"]] == sorted(f"EMP000{i}" for i in range(5))


# ------------------------------------------------------------------ punch-in
@pytest.mark.parametrize("h,mi,s,late", [(9, 40, 0, 0), (9, 40, 1, 10), (10, 15, 59, 45), (9, 25, 0, 0), (9, 40, 0.9, 0)])
def test_late_rule_r2(c, h, mi, s, late):
    emp(c)
    sec, ms = int(s), int(round((s - int(s)) * 1000))
    r = punch_in(c, "EMP0001", ist(2026, 7, 6, h, mi, sec, ms))
    assert r.status_code == 201, r.text
    assert r.json()["late_minutes"] == late
    assert r.json()["date"] == "2026-07-06" and r.json()["punch_in"] % 1000 == 0


def test_punch_in_errors_and_defaults(c):
    emp(c)
    assert punch_in(c, "EMP9999", ist(2026, 7, 6, 9)).status_code == 404
    for bad in (1783312500, 1783312500000.5, "1783312500000", 5, True):
        assert c.post("/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": bad}).status_code == 422
    assert c.post("/attendance/punch-in", json={"emp_code": "EMP0001", "status": "ABSENT",
                                                "punched_at": ist(2026, 7, 6, 9)}).status_code == 422
    r = c.post("/attendance/punch-in", json={"emp_code": "EMP0001", "status": "WFH", "punched_at": ist(2026, 7, 6, 9)})
    body = r.json()
    assert r.status_code == 201 and body["status"] == "WFH" and body["history"] == [] and body["half_day"] is False
    assert body["punch_out"] is None and body["work_hours"] is None and "id" not in body
    assert punch_in(c, "EMP0001", ist(2026, 7, 6, 11)).status_code == 409
    r = c.post("/attendance/punch-in", json={"emp_code": "EMP0001"})  # defaults to now
    assert r.status_code == 201


def test_ist_date_boundary(c):
    emp(c)
    r = punch_in(c, "EMP0001", ist(2026, 7, 6, 0, 30))  # 19:00Z on the 5th, but 00:30 IST on the 6th
    assert r.json()["date"] == "2026-07-06"


def test_overnight_shift(c):
    emp(c, shift_start="22:00", shift_end="06:00")
    r = punch_in(c, "EMP0001", ist(2026, 7, 6, 21, 55))
    assert r.json()["date"] == "2026-07-06" and r.json()["late_minutes"] == 0
    r = punch_out(c, "EMP0001", ist(2026, 7, 7, 6, 40))
    assert r.status_code == 200, r.text
    assert r.json()["overtime_minutes"] == 40 and r.json()["work_hours"] == 8.75
    emp(c, "EMP0002", shift_start="22:00", shift_end="06:00")
    r = punch_in(c, "EMP0002", ist(2026, 7, 7, 1, 0))  # before shift_end -> belongs to the previous day's shift
    assert r.json()["date"] == "2026-07-06" and r.json()["late_minutes"] == 180


# ------------------------------------------------------------------ punch-out
def test_punch_out_rules(c):
    emp(c)
    assert punch_out(c, "EMP0001", ist(2026, 7, 6, 18)).status_code == 404  # no punch-in
    punch_in(c, "EMP0001", ist(2026, 7, 6, 9, 28))
    assert punch_out(c, "EMP0001", ist(2026, 7, 6, 9, 28)).status_code == 422
    assert punch_out(c, "EMP0001", ist(2026, 7, 7, 9, 29)).status_code == 422  # > 24h
    r = punch_out(c, "EMP0001", ist(2026, 7, 6, 19, 10))
    assert r.status_code == 200
    b = r.json()
    assert b["work_hours"] == 9.7 and b["overtime_minutes"] == 40 and b["half_day"] is False
    assert punch_out(c, "EMP0001", ist(2026, 7, 6, 19, 30)).status_code == 409
    assert punch_out(c, "EMP9999", ist(2026, 7, 6, 19, 30)).status_code == 404


def test_overtime_threshold_and_half_day(c):
    emp(c)
    punch_in(c, "EMP0001", ist(2026, 7, 6, 9, 30))
    r = punch_out(c, "EMP0001", ist(2026, 7, 6, 18, 59, 59)).json()
    assert r["overtime_minutes"] == 0  # 29m59s
    emp(c, "EMP0002")
    punch_in(c, "EMP0002", ist(2026, 7, 6, 9, 30))
    assert punch_out(c, "EMP0002", ist(2026, 7, 6, 19, 0)).json()["overtime_minutes"] == 30
    emp(c, "EMP0003")
    punch_in(c, "EMP0003", ist(2026, 7, 6, 9, 0))
    r = punch_out(c, "EMP0003", ist(2026, 7, 6, 13, 29, 53)).json()  # 4.4981h -> 4.50 -> not a half day
    assert r["work_hours"] == 4.5 and r["half_day"] is False
    emp(c, "EMP0004")
    punch_in(c, "EMP0004", ist(2026, 7, 6, 9, 0))
    r = punch_out(c, "EMP0004", ist(2026, 7, 6, 13, 29, 40)).json()  # 4.4944h -> 4.49
    assert r["work_hours"] == 4.49 and r["half_day"] is True


def test_work_hours_rounds_half_up(c):
    emp(c)
    punch_in(c, "EMP0001", ist(2026, 7, 6, 9, 0))
    # 3 h 0 m 18 s = 3.005 h exactly -> half-up gives 3.01 (banker's / float round gives 3.0)
    r = punch_out(c, "EMP0001", ist(2026, 7, 6, 12, 0, 18)).json()
    assert r["work_hours"] == 3.01


# ------------------------------------------------------------------ list
def test_attendance_list(c):
    emp(c)
    emp(c, "EMP0002")
    for code in ("EMP0001", "EMP0002"):
        seed_log(c, code, "2026-07-01")
        seed_log(c, code, "2026-07-03", status="WFH")
    seed_log(c, "EMP0001", "2026-07-05", status="LEAVE")
    r = c.get("/attendance").json()
    assert r["total"] == 5
    assert [(i["date"], i["emp_code"]) for i in r["items"]] == [
        ("2026-07-05", "EMP0001"), ("2026-07-03", "EMP0001"), ("2026-07-03", "EMP0002"),
        ("2026-07-01", "EMP0001"), ("2026-07-01", "EMP0002")]
    r = c.get("/attendance", params={"status": "WFH", "page_size": 1, "page": 2}).json()
    assert r["total"] == 2 and r["items"][0]["emp_code"] == "EMP0002"
    r = c.get("/attendance", params={"emp_code": "EMP0001", "date_from": "2026-07-02", "date_to": "2026-07-03"}).json()
    assert r["total"] == 1
    assert c.get("/attendance", params={"date_from": "2026-07-09", "date_to": "2026-07-01"}).status_code == 422
    assert c.get("/attendance", params={"date_from": "07/01/2026"}).status_code == 422
    assert c.get("/attendance", params={"status": "NOPE"}).status_code == 422
    legacy = c.get("/attendance", params={"emp_code": "EMP0002", "date_to": "2026-07-01"}).json()["items"][0]
    assert legacy["half_day"] is False and legacy["history"] == []


# ------------------------------------------------------------------ patch
def test_regularize_flow(c):
    emp(c)
    punch_in(c, "EMP0001", ist(2026, 7, 7, 10, 5))
    punch_out(c, "EMP0001", ist(2026, 7, 7, 18, 30))
    new_in = ist(2026, 7, 7, 9, 28)
    r = c.patch("/attendance/EMP0001/2026-07-07", json={
        "punch_in": new_in, "reason": "biometric glitch", "regularized_by": "hr", "late_minutes": 99})
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["late_minutes"] == 0 and b["punch_in"] == new_in and len(b["history"]) == 1
    ch = b["history"][0]["changes"]
    assert set(ch) == {"punch_in", "late_minutes", "work_hours"}
    assert ch["punch_in"] == {"from": ist(2026, 7, 7, 10, 5), "to": new_in}
    assert ch["late_minutes"] == {"from": 35, "to": 0}
    assert b["history"][0]["by"] == "hr" and isinstance(b["history"][0]["at"], int)
    # nothing changes -> 422
    same = {"punch_in": new_in, "reason": "again again", "regularized_by": "hr"}
    assert c.patch("/attendance/EMP0001/2026-07-07", json=same).status_code == 422
    # absent clears times
    r = c.patch("/attendance/EMP0001/2026-07-07", json={"status": "ABSENT", "reason": "mistake!", "regularized_by": "hr"})
    b = r.json()
    assert r.status_code == 200 and b["punch_in"] is None and b["work_hours"] is None and len(b["history"]) == 2
    assert set(b["history"][1]["changes"]) == {"status", "punch_in", "punch_out", "work_hours"}
    # absent + punch time -> 422; presence without punch_in -> 422
    base = {"reason": "valid reason", "regularized_by": "hr"}
    assert c.patch("/attendance/EMP0001/2026-07-07", json={**base, "status": "LEAVE", "punch_in": new_in}).status_code == 422
    assert c.patch("/attendance/EMP0001/2026-07-07", json={**base, "status": "PRESENT"}).status_code == 422
    # punch_in must remain on the record's date
    assert c.patch("/attendance/EMP0001/2026-07-07",
                   json={**base, "status": "PRESENT", "punch_in": ist(2026, 7, 8, 9)}).status_code == 422
    assert c.patch("/attendance/EMP0001/2026-07-07",
                   json={**base, "status": "WFH", "punch_in": new_in, "punch_out": new_in - 1000}).status_code == 422
    ok = c.patch("/attendance/EMP0001/2026-07-07", json={**base, "status": "WFH", "punch_in": new_in})
    assert ok.status_code == 200 and ok.json()["status"] == "WFH" and ok.json()["late_minutes"] == 0
    # 404s, short reason
    assert c.patch("/attendance/EMP0001/2026-07-09", json=base).status_code == 404
    assert c.patch("/attendance/EMP0099/2026-07-07", json=base).status_code == 404
    assert c.patch("/attendance/EMP0001/2026-07-07", json={**base, "reason": "abc"}).status_code == 422
    assert c.patch("/attendance/EMP0001/not-a-date", json=base).status_code == 422


def test_regularize_legacy_record_without_history(c):
    emp(c)
    c.mongo.attendance_logs.insert_one({
        "emp_code": "EMP0001", "date": "2026-07-14", "status": "PRESENT",
        "punch_in": datetime(2026, 7, 14, 4, 2, tzinfo=timezone.utc),
        "punch_out": datetime(2026, 7, 14, 13, 1, tzinfo=timezone.utc),
        "work_hours": 8.98, "late_minutes": 0, "overtime_minutes": 0})
    r = c.patch("/attendance/EMP0001/2026-07-14", json={
        "punch_out": ist(2026, 7, 14, 18, 40), "reason": "forgot to tap", "regularized_by": "hr"})
    assert r.status_code == 200, r.text
    b = r.json()
    assert len(b["history"]) == 1 and b["half_day"] is False and b["overtime_minutes"] == 0
    assert b["work_hours"] == 9.13 and b["history"][0]["changes"]["punch_out"]["from"] == 1784034060000


# ------------------------------------------------------------------ concurrency
def test_concurrent_punch_in_one_winner(c):
    emp(c)
    def go(_):
        return httpx.post(f"{BASE}/attendance/punch-in", json={"emp_code": "EMP0001", "punched_at": ist(2026, 7, 6, 9, 30)}).status_code
    with ThreadPoolExecutor(16) as ex:
        codes = list(ex.map(go, range(16)))
    assert sorted(codes) == [201] + [409] * 15


def test_concurrent_punch_out_one_winner(c):
    emp(c)
    punch_in(c, "EMP0001", ist(2026, 7, 6, 9, 30))
    def go(_):
        return httpx.post(f"{BASE}/attendance/punch-out", json={"emp_code": "EMP0001", "punched_at": ist(2026, 7, 6, 18, 30)}).status_code
    with ThreadPoolExecutor(16) as ex:
        codes = list(ex.map(go, range(16)))
    assert sorted(codes) == [200] + [409] * 15


def test_concurrent_duplicate_employee(c):
    body = {"emp_code": "EMP0042", "name": "A", "email": "a@b.co", "department": "d", "joined_on": "2026-01-01"}
    with ThreadPoolExecutor(12) as ex:
        codes = list(ex.map(lambda _: httpx.post(f"{BASE}/employees", json=body).status_code, range(12)))
    assert sorted(codes) == [201] + [409] * 11


def test_concurrent_regularize_keeps_all_history(c):
    emp(c)
    punch_in(c, "EMP0001", ist(2026, 7, 6, 9, 30))
    def go(i):
        r = httpx.patch(f"{BASE}/attendance/EMP0001/2026-07-06", json={
            "punch_out": ist(2026, 7, 6, 10, 0) + i * 60000, "reason": f"edit number {i}", "regularized_by": "hr"})
        return r.status_code
    with ThreadPoolExecutor(8) as ex:
        codes = list(ex.map(go, range(8)))
    assert set(codes) <= {200, 409}
    stored = c.mongo.attendance_logs.find_one({"emp_code": "EMP0001"})
    assert len(stored["history"]) == codes.count(200)  # every success left exactly one entry


# ------------------------------------------------------------------ analytics
def test_employee_monthly(c):
    emp(c, joined_on="2026-07-08")  # Wed 8th: working days Jul 8..31 = 18 weekdays
    seed_log(c, "EMP0001", "2026-07-08", late=35, ot=40, wh=9.0, half=False)
    seed_log(c, "EMP0001", "2026-07-09", status="WFH", half=True, wh=3.5)       # 0.5
    seed_log(c, "EMP0001", "2026-07-10", status="ON_DUTY", late=12)
    seed_log(c, "EMP0001", "2026-07-11", late=20, ot=30)                        # Saturday: no present day, late/ot count
    seed_log(c, "EMP0001", "2026-07-13", status="LEAVE")
    seed_log(c, "EMP0001", "2026-07-14", status="ABSENT")
    seed_log(c, "EMP0001", "2026-08-03")                                        # other month
    r = c.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-07"})
    assert r.status_code == 200
    b = r.json()
    assert b == {"emp_code": "EMP0001", "month": "2026-07", "working_days": 18, "present_days": 2.5,
                 "leave_days": 1, "late_count": 3, "total_late_minutes": 67, "total_overtime_minutes": 70,
                 "attendance_pct": 13.89}
    assert c.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-06"}).json()["working_days"] == 0
    assert c.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-06"}).json()["attendance_pct"] is None
    assert c.get("/analytics/employees/EMP0777/monthly", params={"month": "2026-07"}).status_code == 404
    assert c.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-13"}).status_code == 422
    assert c.get("/analytics/employees/EMP0001/monthly").status_code == 422


def test_department_summary(c):
    emp(c, "EMP0001", department="Eng")
    emp(c, "EMP0002", department="Eng", joined_on="2026-07-20")        # joins later in the month, no logs
    emp(c, "EMP0003", department="Eng", joined_on="2026-08-01")        # not yet joined -> excluded
    emp(c, "EMP0004", department="Ops", joined_on="2026-09-01")        # department with zero headcount -> omitted
    emp(c, "EMP0005", department="Sales")                              # no logs at all
    seed_log(c, "EMP0001", "2026-07-06", wh=8.0)
    seed_log(c, "EMP0001", "2026-07-07", wh=9.0, late=15)
    seed_log(c, "EMP0001", "2026-07-08", status="ON_DUTY", wh=7.01)
    seed_log(c, "EMP0001", "2026-07-09", status="LEAVE")
    seed_log(c, "EMP0001", "2026-07-10", status="WFH", half=True)       # present, no work_hours: excluded from avg
    seed_log(c, "EMP0001", "2026-07-11", late=10, wh=5.0)               # Saturday
    r = c.get("/analytics/departments/summary", params={"month": "2026-07"}).json()
    assert [i["department"] for i in r["items"]] == ["Eng", "Sales"]
    eng = r["items"][0]
    assert eng == {"department": "Eng", "headcount": 2, "present_days": 3.5, "avg_work_hours": 7.25,
                   "late_count": 2, "total_late_minutes": 25, "leave_count": 1, "on_duty_count": 1}
    assert r["items"][1] == {"department": "Sales", "headcount": 1, "present_days": 0.0, "avg_work_hours": None,
                             "late_count": 0, "total_late_minutes": 0, "leave_count": 0, "on_duty_count": 0}
    one = c.get("/analytics/departments/summary", params={"month": "2026-07", "department": "Sales"}).json()
    assert len(one["items"]) == 1


def test_leaderboard_ties_and_cutoff(c):
    for i, (code, dep) in enumerate([("EMP0001", "A"), ("EMP0002", "A"), ("EMP0003", "B"), ("EMP0004", "B"),
                                     ("EMP0005", "A"), ("EMP0006", "B")]):
        emp(c, code, department=dep)
    totals = {"EMP0001": 100, "EMP0002": 60, "EMP0003": 60, "EMP0004": 60, "EMP0005": 10, "EMP0006": 0}
    for code, t in totals.items():
        if t:
            seed_log(c, code, "2026-07-06", late=t)
    seed_log(c, "EMP0001", "2026-07-07", late=0)
    seed_log(c, "EMP0099", "2026-07-06", late=500)  # unknown employee ignored
    r = c.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 3}).json()["items"]
    assert [(i["rank"], i["emp_code"]) for i in r] == [(1, "EMP0001"), (2, "EMP0002"), (2, "EMP0003"), (2, "EMP0004")]
    r = c.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 2}).json()["items"]
    assert [(i["rank"], i["emp_code"]) for i in r] == [(1, "EMP0001"), (2, "EMP0002"), (2, "EMP0003"), (2, "EMP0004")]
    r = c.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 50}).json()["items"]
    assert [i["rank"] for i in r] == [1, 2, 2, 2, 5]
    r = c.get("/analytics/leaderboard/late", params={"month": "2026-07", "department": "B"}).json()["items"]
    assert [(i["rank"], i["emp_code"], i["total_late_minutes"], i["late_count"]) for i in r] == [
        (1, "EMP0003", 60, 1), (1, "EMP0004", 60, 1)]
    assert r[0]["name"] == "N EMP0003" and r[0]["department"] == "B"
    assert c.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 0}).status_code == 422
    assert c.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 51}).status_code == 422


def test_trend_gap_filling_and_moving_average(c):
    emp(c, "EMP0001", department="Eng", joined_on="2026-07-01")
    emp(c, "EMP0002", department="Eng", joined_on="2026-07-08")
    seed_log(c, "EMP0001", "2026-07-02")                     # Thu: 1/1
    seed_log(c, "EMP0001", "2026-07-03", late=20)             # Fri: 1/1
    seed_log(c, "EMP0001", "2026-07-04")                     # Sat: present_count counted, rate null
    seed_log(c, "EMP0001", "2026-07-06", half=True)          # Mon: 0.5/1
    seed_log(c, "EMP0001", "2026-07-08", status="LEAVE")     # Wed: headcount 2, rate 0
    seed_log(c, "EMP0002", "2026-07-08", late=11)
    r = c.get("/analytics/departments/Eng/trend", params={"from": "2026-07-01", "to": "2026-07-09"})
    assert r.status_code == 200
    rows = r.json()["items"]
    assert [x["date"] for x in rows] == [f"2026-07-0{d}" for d in range(1, 10)]
    by = {x["date"]: x for x in rows}
    assert by["2026-07-01"] == {"date": "2026-07-01", "is_working_day": True, "headcount": 1, "present_count": 0,
                                "late_count": 0, "attendance_rate": 0.0, "moving_avg_7d": 0.0}
    assert by["2026-07-02"]["attendance_rate"] == 1.0 and by["2026-07-02"]["moving_avg_7d"] == 0.5
    assert by["2026-07-03"]["late_count"] == 1 and by["2026-07-03"]["moving_avg_7d"] == 0.6667
    assert by["2026-07-04"]["is_working_day"] is False and by["2026-07-04"]["present_count"] == 1
    assert by["2026-07-04"]["attendance_rate"] is None and by["2026-07-04"]["moving_avg_7d"] == 0.6667
    assert by["2026-07-05"]["present_count"] == 0 and by["2026-07-05"]["attendance_rate"] is None
    assert by["2026-07-06"]["attendance_rate"] == 0.5 and by["2026-07-06"]["present_count"] == 0.5
    assert by["2026-07-06"]["moving_avg_7d"] == 0.625  # rates 0,1,1,_,_,0.5 -> 2.5 / 4
    assert by["2026-07-08"]["headcount"] == 2 and by["2026-07-08"]["attendance_rate"] == 0.5
    assert c.get("/analytics/departments/Nope/trend", params={"from": "2026-07-01", "to": "2026-07-09"}).status_code == 404
    assert c.get("/analytics/departments/Eng/trend", params={"from": "2026-07-09", "to": "2026-07-01"}).status_code == 422
    assert c.get("/analytics/departments/Eng/trend", params={"from": "2026-01-01", "to": "2026-04-30"}).status_code == 422
    assert c.get("/analytics/departments/Eng/trend", params={"from": "2026-07-01"}).status_code == 422
    one = c.get("/analytics/departments/Eng/trend", params={"from": "2026-07-04", "to": "2026-07-04"}).json()["items"]
    assert len(one) == 1 and one[0]["moving_avg_7d"] is None
    empty = c.get("/analytics/departments/Eng/trend", params={"from": "2027-01-01", "to": "2027-01-03"}).json()["items"]
    assert len(empty) == 3


def test_explain_uses_indexes(c):
    emp(c)
    seed_log(c, "EMP0001", "2026-07-06", late=15)
    cases = {
        "attendance_list": {"date_from": "2026-07-01", "status": "PRESENT"},
        "employee_monthly": {"emp_code": "EMP0001", "month": "2026-07"},
        "department_summary": {"month": "2026-07"},
        "late_leaderboard": {"month": "2026-07", "limit": 5},
        "department_trend": {"department": "Eng", "from": "2026-07-01", "to": "2026-07-31"},
    }
    for name, params in cases.items():
        r = c.get(f"/admin/explain/{name}", params=params)
        assert r.status_code == 200, (name, r.text)
        text = r.text
        assert "IXSCAN" in text and "COLLSCAN" not in text, name
        assert r.json()["endpoint"] == name and r.json()["collection"] in ("employees", "attendance_logs")
    assert c.get("/admin/explain/employee_monthly", params={"emp_code": "EMP0001"}).status_code == 422
    assert c.get("/admin/explain/nonsense").status_code == 422


def test_health(c):
    assert c.get("/health").json() == {"status": "ok"}
