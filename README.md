# Employee Attendance & Analytics API

FastAPI + MongoDB (PyMongo). All application code is in `app/main.py`.

## Run
```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
uvicorn app.main:app --port 8000
```
Indexes are created idempotently at startup. Needs MongoDB 6.0+ (uses `$densify`, `$rank`, `$setWindowFields`).

## Tests
```bash
pip install pytest httpx
pytest -q          # boots the app against a throwaway DB (att_pytest) on MONGO_URI
```

## Notes
- All seven missing endpoints, the explain endpoint and the Part A fixes are implemented; nothing is missing.
- Reported numbers are rounded half-up with exact integer/`Decimal` arithmetic (MongoDB `$round` is banker's rounding).
- `REVIEW.md`: defects found in the starter. `DECISIONS.md`: design answers.
