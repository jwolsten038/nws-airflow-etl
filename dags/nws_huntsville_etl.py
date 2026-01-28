import os
import datetime as dt
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.types import DateTime, Boolean, Integer, Text
from typing import List, Dict, Any

LAT = 34.73
LON = -86.59

# NWS strongly prefers a descriptive User-Agent with contact info
HEADERS = {
    "User-Agent": "jwols-nws-etl/1.0 (jwolsten03@gmail.com)",
    "Accept": "application/geo+json",
}

def fetch_hourly_periods(lat: float, lon: float) -> List[Dict]:
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    r = requests.get(points_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    forecast_hourly_url = r.json()["properties"]["forecastHourly"]

    r2 = requests.get(forecast_hourly_url, headers=HEADERS, timeout=30)
    r2.raise_for_status()
    return r2.json()["properties"]["periods"]

def get_engine():
    """
    Build a SQLAlchemy engine from environment variables.
    Set these in your shell / Airflow environment:
      PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        database=os.getenv("PGDATABASE", "fraud_analysis"),
    )
    return create_engine(url)

def run():
    periods = fetch_hourly_periods(LAT, LON)
    run_ts_utc = dt.datetime.now(dt.timezone.utc)

    rows = []
    for p in periods:
        rows.append({
            "run_ts_utc": run_ts_utc,
            "location": "Huntsville, AL",
            "start_time": p.get("startTime"),
            "end_time": p.get("endTime"),
            "is_daytime": p.get("isDaytime"),
            "temperature": p.get("temperature"),
            "temperature_unit": p.get("temperatureUnit"),
            "wind_speed": p.get("windSpeed"),
            "wind_direction": p.get("windDirection"),
            "short_forecast": p.get("shortForecast"),
            "detailed_forecast": p.get("detailedForecast"),
            "icon": p.get("icon"),
        })

    df = pd.DataFrame(rows)

    # Parse timestamps (keep as UTC-aware)
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)

    # Enforce numeric temperature (nullable int)
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce").astype("Int64")

    engine = get_engine()

    # Explicit dtype mapping => stable "proper types" in Postgres
    dtype = {
        "run_ts_utc": DateTime(timezone=True),
        "location": Text(),
        "start_time": DateTime(timezone=True),
        "end_time": DateTime(timezone=True),
        "is_daytime": Boolean(),
        "temperature": Integer(),
        "temperature_unit": Text(),
        "wind_speed": Text(),
        "wind_direction": Text(),
        "short_forecast": Text(),
        "detailed_forecast": Text(),
        "icon": Text(),
    }

    # Append each run so you can prove "3 runs in a row"
    df.to_sql(
        "nws_hourly_forecast",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        dtype=dtype,
        method="multi",
        chunksize=2000,
    )

    print(f"Loaded {len(df)} rows into public.nws_hourly_forecast @ {run_ts_utc.isoformat()}")

if __name__ == "__main__":
    run()
