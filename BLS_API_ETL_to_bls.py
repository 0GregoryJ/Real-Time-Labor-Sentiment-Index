import json
import requests
from datetime import datetime, timezone, date

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.mysql import insert


# -----------------------------
# CONFIG
# -----------------------------
MYSQL_USER = "root"
MYSQL_PASSWORD = "Gj10077436"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "bls"

SERIES_IDS = ["LNS13000000","LNS14000000","CES0000000001","CES0500000002","CES0500000003","CIU2020000000000A"]

STARTYEAR = "2016"
ENDYEAR = str(date.today().year)

BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


# -----------------------------
# SQLALCHEMY SETUP
# -----------------------------
DATABASE_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, future=True)

Base = declarative_base()


class BLSObservation(Base):
    __tablename__ = "bls_timeseries"

    id = Column(Integer, primary_key=True)
    series_id = Column(String(50), nullable=True)
    year = Column(Integer, nullable=True)
    period = Column(String(4), nullable=True)
    value = Column(Float, nullable=True)
    footnotes = Column(String(255))
    retrieved_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("series_id", "year", "period", name="uq_series_year_period"),
    )


# -----------------------------
# API FETCH
# -----------------------------
def fetch_bls(series_ids, startyear, endyear):
    headers = {"Content-type": "application/json"}
    payload = json.dumps({
        "seriesid": series_ids,
        "startyear": startyear,
        "endyear": endyear,
    })

    r = requests.post(BLS_URL, data=payload, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Optional: fail fast on API-level errors
    # BLS typically uses "REQUEST_SUCCEEDED" but be tolerant if missing.
    status = data.get("status")
    if status and status != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API status={status} message={data.get('message')}")

    return data

# -----------------------------
# PARSE JSON -> ROW DICTS
# -----------------------------
def parse_rows(bls_json, monthly_only=True):
    now = datetime.now(timezone.utc)

    rows = []
    for series in bls_json["Results"]["series"]:
        series_id = series["seriesID"]

        for item in series.get("data", []):
            period = item["period"]

            if monthly_only and not ("M01" <= period <= "M12"):
                continue

            # Footnotes list -> single comma-separated string
            footnotes = ""
            for fn in item.get("footnotes", []):
                if fn and fn.get("text"):
                    footnotes += fn["text"] + ","
            footnotes = footnotes.rstrip(",")
            val = (item["value"])
            if val == "-":
                continue
            rows.append({
                "series_id": series_id,
                "year": int(item["year"]),
                "period": period,
                "value": val,
                "footnotes": footnotes,
                "retrieved_at": now,
            })

    return rows


# -----------------------------
# UPSERT INTO MYSQL
# -----------------------------
def upsert_rows(session, rows):
    if not rows:
        return 0

    stmt = insert(BLSObservation).values(rows)

    # On duplicate key (requires UNIQUE constraint), update these columns
    stmt = stmt.on_duplicate_key_update(
        value=stmt.inserted.value,
        footnotes=stmt.inserted.footnotes,
        retrieved_at=stmt.inserted.retrieved_at,
    )

    result = session.execute(stmt)
    return result.rowcount


# -----------------------------
# MAIN
# -----------------------------
def main():
    # Create table if it doesn't exist
    Base.metadata.create_all(engine)

    # Fetch and parse
    bls_json = fetch_bls(SERIES_IDS, STARTYEAR, ENDYEAR)
    rows = parse_rows(bls_json, monthly_only=True)
    print(f"Parsed {len(rows)} rows from API.")

    # Load into MySQL (upsert)
    session = SessionLocal()
    try:
        affected = upsert_rows(session, rows)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    print(f"Upsert complete. Rows affected (inserted/updated): {affected}")

    # Quick sanity query
    with engine.connect() as conn:
        result = conn.execute(
            BLSObservation.__table__
            .select()
            .with_only_columns(
                BLSObservation.series_id,
                BLSObservation.year,
                BLSObservation.period,
                BLSObservation.value,
            )
            .order_by(BLSObservation.series_id, BLSObservation.year.desc(), BLSObservation.period.desc())
            .limit(10)
        )
        print("Sample rows:")
        for row in result:
            print(tuple(row))


if __name__ == "__main__":
    main()
