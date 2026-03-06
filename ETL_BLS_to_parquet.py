import json
import requests
from datetime import datetime, timezone, date
import pandas as pd
from pathlib import Path
import os

#bls API info
SERIES_IDS = ["LNS13000000","LNS14000000","CES0000000001","CES0500000002","CES0500000003","CIU2020000000000A"]

STARTYEAR = "2016"
ENDYEAR = 2026

BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


#API fetch
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

#parse json - row dicts
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

rows = parse_rows(fetch_bls(SERIES_IDS, STARTYEAR, ENDYEAR))

#read data into df, clean, and write to parquet
#into df
BLS_data = pd.DataFrame(rows)
#convert period into month number and create date column
BLS_data["month_num"] = BLS_data["period"].str.replace("M", "", regex=False).astype(int)
BLS_data["date"] = pd.to_datetime(
    dict(year=BLS_data["year"], month=BLS_data["month_num"], day=1)
)
#drop columns
BLS_data = BLS_data.drop(columns=["month_num", "year", "period", "footnotes", "retrieved_at"])

#rename series_ID to query
BLS_data = BLS_data.rename(columns={"series_id": "query"})
#add category column
BLS_data["category"] = "labor_market"
output_path = Path("/Users/gregoryjoshua/Desktop/Projects/SearchSentimentDashboard/data_parquets")
BLS_data.to_parquet(output_path / "bls_data.parquet", index=False)
print(BLS_data)