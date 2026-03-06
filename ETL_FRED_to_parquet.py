# is my fred key
import os
import requests
import pandas as pd
from pathlib import Path

FRED_API_KEY = "59a7ee50b46cb59dbad7aa874b95731c"
SERIES_ID = "PCE"  # Personal Consumption Expenditures (monthly)
START_DATE = "2020-01-01"

BASE_DIR = Path(__file__).resolve().parent
file_path = BASE_DIR / "data_parquets"

def fetch_fred_series_observations(series_id: str, api_key: str, start_date: str) -> pd.DataFrame:
    if not api_key:
        raise ValueError("Missing FRED_API_KEY environment variable.")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        # optional: enforce sorting, though default is fine
        "sort_order": "asc",
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    obs = data.get("observations", [])
    df = pd.DataFrame(obs)

    # Transform: parse date, coerce value to numeric ('.' becomes NaN)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Keep only the essentials
    df = df[["date", "value"]].sort_values("date")
    df["category"] = "consumer_spending"
    df["query"] = "FRED Personal Consumption Expenditures (PCE)"

    return df

def main():
    df = fetch_fred_series_observations(SERIES_ID, FRED_API_KEY, START_DATE)

    df.to_parquet(file_path / "FRED_data.parquet", index=False)

    print(df)

if __name__ == "__main__":
    main()
