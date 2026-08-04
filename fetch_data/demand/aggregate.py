"""Build hourly demand-weather rows from complete KPX and ASOS source data."""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from fetch_data.demand.database import (
    get_first_unknown_timestamp,
    get_last_demand_weather_timestamp,
    upsert_demand_weather,
)


def get_recovery_start(
    latest: datetime | None, first_unknown: datetime | None, fallback: datetime
) -> datetime:
    """Use the earliest unrepaired placeholder or the next hourly record."""
    candidates = [
        timestamp
        for timestamp in (first_unknown, latest + timedelta(hours=1) if latest else None)
        if timestamp is not None
    ]
    return min(candidates, default=fallback)


def get_common_end(
    last_complete_demand_hour: datetime | None, latest_weather: datetime | None
) -> datetime | None:
    """Return the exclusive boundary shared by complete demand and weather."""
    if last_complete_demand_hour is None or latest_weather is None:
        return None
    return min(last_complete_demand_hour, latest_weather) + timedelta(hours=1)


def _complete_demand_hours(engine: Engine, start: datetime, end: datetime) -> pd.DataFrame:
    query = text(
        """
        SELECT
            date_trunc('hour', timestamp) AS timestamp,
            AVG(current_demand) AS demand_avg,
            BOOL_OR(is_holiday) AS is_holiday,
            MAX(day_type) AS day_type
        FROM demand_5min
        WHERE timestamp >= :start AND timestamp < :end
        GROUP BY date_trunc('hour', timestamp)
        HAVING COUNT(current_demand) >= 12
        ORDER BY timestamp
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(query, {"start": start, "end": end}).mappings().all()
    return pd.DataFrame(rows, columns=["timestamp", "demand_avg", "is_holiday", "day_type"])


def _load_weather(weather_csv: Path) -> pd.DataFrame:
    weather = pd.read_csv(
        weather_csv,
        usecols=["date", "station_name", "temperature", "humidity"],
        encoding="utf-8-sig",
    )
    weather["timestamp"] = pd.to_datetime(weather["date"], errors="coerce").dt.floor("h")
    weather = weather.dropna(subset=["timestamp", "station_name"])
    weather = weather[weather["station_name"] != "UNKNOWN"]
    weather = weather.dropna(subset=["temperature", "humidity"])
    return weather.drop_duplicates(subset=["timestamp", "station_name"], keep="last")


def remove_repaired_unknowns(engine: Engine, start: datetime, end: datetime) -> int:
    """Delete legacy placeholders only where a real station row now exists."""
    statement = text(
        """
        DELETE FROM demand_weather_1h AS old
        WHERE old.station_name = 'UNKNOWN'
          AND old.timestamp >= :start
          AND old.timestamp < :end
          AND EXISTS (
              SELECT 1 FROM demand_weather_1h AS real
              WHERE real.timestamp = old.timestamp
                AND real.station_name <> 'UNKNOWN'
          )
        """
    )
    with engine.begin() as connection:
        return connection.execute(statement, {"start": start, "end": end}).rowcount


def refresh_demand_views(engine: Engine) -> None:
    """Refresh both dashboard views; database errors deliberately propagate."""
    with engine.begin() as connection:
        connection.execute(text("REFRESH MATERIALIZED VIEW mv_latest_weather"))
        connection.execute(text("REFRESH MATERIALIZED VIEW mv_hourly_national"))


def aggregate_demand_weather(
    engine: Engine,
    weather_csv: Path,
    recover: bool = False,
    now: datetime | None = None,
) -> int:
    """Upsert real ASOS rows for complete hourly demand intervals only."""
    current_hour = (now or datetime.now()).replace(minute=0, second=0, microsecond=0)
    fallback = current_hour - timedelta(hours=48)
    start = (
        get_recovery_start(
            get_last_demand_weather_timestamp(engine),
            get_first_unknown_timestamp(engine),
            fallback,
        )
        if recover
        else fallback
    )
    demand = _complete_demand_hours(engine, start, current_hour)
    if demand.empty:
        return 0

    demand["timestamp"] = pd.to_datetime(demand["timestamp"])
    weather = _load_weather(weather_csv)
    latest_weather = weather["timestamp"].max() if not weather.empty else None
    end = get_common_end(demand["timestamp"].max(), latest_weather)
    if end is None or end <= start:
        return 0

    demand = demand[(demand["timestamp"] >= start) & (demand["timestamp"] < end)]
    weather = weather[(weather["timestamp"] >= start) & (weather["timestamp"] < end)]
    merged = demand.merge(weather, on="timestamp", how="inner")
    if merged.empty:
        return 0

    records = merged[
        [
            "timestamp",
            "station_name",
            "temperature",
            "humidity",
            "demand_avg",
            "is_holiday",
            "day_type",
        ]
    ].where(pd.notnull, None).to_dict(orient="records")
    for record in records:
        record["timestamp"] = record["timestamp"].to_pydatetime()

    saved = upsert_demand_weather(engine, records)
    remove_repaired_unknowns(engine, start, end)
    return saved
