import os
import json
import datetime as dt
import requests
import pandas as pd
from typing import Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.types import DateTime, Boolean, Integer, Text

LAT = 34.73
LON = -86.59

HEADERS = {
    "User-Agent": "jwols-nws-etl/1.0 (jwolsten03@gmail.com)",
    "Accept": "application/geo+json",
}


def fetch_hourly_periods(lat: float, lon: float) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    r = requests.get(points_url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    payload: dict[str, Any] = r.json()
    props = payload.get("properties")
    if not isinstance(props, dict):
        raise ValueError("Malformed API response: missing/invalid 'properties'")

    forecast_hourly_url = props.get("forecastHourly")
    if not isinstance(forecast_hourly_url, str) or not forecast_hourly_url:
        raise ValueError("Malformed API response: missing/invalid 'properties.forecastHourly'")

    r2 = requests.get(forecast_hourly_url, headers=HEADERS, timeout=30)
    r2.raise_for_status()

    payload2: dict[str, Any] = r2.json()
    props2 = payload2.get("properties")
    if not isinstance(props2, dict):
        raise ValueError("Malformed API response: missing/invalid 'properties' (hourly)")

    periods = props2.get("periods")
    if not isinstance(periods, list):
        raise ValueError("Malformed API response: missing/invalid 'properties.periods' (hourly)")

    return periods, payload2, forecast_hourly_url


def get_engine() -> Engine:
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        database=os.getenv("PGDATABASE", "fraud_analysis"),
    )
    return create_engine(url)


def run() -> None:
    periods, payload2, forecast_hourly_url = fetch_hourly_periods(LAT, LON)
    run_ts_utc = dt.datetime.now(dt.timezone.utc)

    engine = get_engine()

    # Extract: store raw API payload as jsonb for replayable transforms
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.nws_hourly_forecast_extract
                  (run_ts_utc, location, lat, lon, source_url, payload)
                VALUES
                  (:run_ts_utc, :location, :lat, :lon, :source_url, CAST(:payload AS jsonb))
                """
            ),
            {
                "run_ts_utc": run_ts_utc,
                "location": "Huntsville, AL",
                "lat": LAT,
                "lon": LON,
                "source_url": forecast_hourly_url,
                "payload": json.dumps(payload2),
            },
        )

    # Transform: flatten periods into curated rows
    rows: list[dict[str, Any]] = []
    for p in periods:
        # Defensive: periods should be dicts, but guard anyway
        if not isinstance(p, dict):
            continue

        rows.append(
            {
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
            }
        )

    df = pd.DataFrame(rows)

    # Guard against missing columns (clear error instead of KeyError)
    required_cols = {"start_time", "end_time", "temperature"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns in transformed df: {sorted(missing)}")

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce").astype("Int64")

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