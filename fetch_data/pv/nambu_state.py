"""Nambu PV collection state backed by the core plants/generation schema."""

from datetime import date, datetime, timedelta

from sqlalchemy import text


_TARGETS = text(
    """
    SELECT
      p.plant_id,
      p.plant_code AS gencd,
      p.unit_no AS hogi,
      p.plant_name,
      MAX(g.timestamp) AS last_dt
    FROM plants p
    LEFT JOIN generation g ON g.plant_id = p.plant_id
    WHERE p.operator = 'nambu'
      AND p.fuel_type = 'solar'
      AND p.plant_code IS NOT NULL
    GROUP BY p.plant_id, p.plant_code, p.unit_no, p.plant_name
    ORDER BY p.plant_code, p.unit_no
    """
)

_HOURS_FOR_DAY = text(
    """
    SELECT COUNT(DISTINCT EXTRACT(HOUR FROM timestamp))
    FROM generation
    WHERE plant_id = :plant_id
      AND timestamp >= :day_start
      AND timestamp < :day_end
    """
)

_HOURS_BY_DAY = text(
    """
    SELECT DATE(timestamp) AS day,
           COUNT(DISTINCT EXTRACT(HOUR FROM timestamp)) AS hours
    FROM generation
    WHERE plant_id = :plant_id
      AND timestamp >= :start_dt
      AND timestamp < :end_dt
    GROUP BY DATE(timestamp)
    """
)


def collection_start(
    last_dt: datetime | None, hours: int, today: date
) -> datetime | None:
    if last_dt and last_dt.year < 2025:
        return None
    if not last_dt:
        return datetime.combine(today - timedelta(days=365), datetime.min.time())

    day = last_dt.date()
    if hours >= 24:
        day += timedelta(days=1)
    return datetime.combine(day, datetime.min.time())


def get_nambu_targets(engine, gencd: str | None = None, hogi: int | None = None) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(_TARGETS).mappings().all()

    targets = []
    for row in rows:
        target_code = str(row["gencd"]).strip()
        target_unit = int(str(row["hogi"]).strip())
        if gencd and target_code != gencd:
            continue
        if hogi is not None and target_unit != hogi:
            continue
        targets.append(
            {
                "plant_id": int(row["plant_id"]),
                "gencd": target_code,
                "hogi": target_unit,
                "plant_name": row["plant_name"],
                "last_dt": row["last_dt"],
            }
        )
    return targets


def count_hours_for_day(engine, plant_id: int, day: date) -> int:
    day_start = datetime.combine(day, datetime.min.time())
    with engine.connect() as conn:
        hours = conn.execute(
            _HOURS_FOR_DAY,
            {
                "plant_id": plant_id,
                "day_start": day_start,
                "day_end": day_start + timedelta(days=1),
            },
        ).scalar()
    return int(hours or 0)


def find_incomplete_days(
    engine, plant_id: int, start: date, end: date
) -> list[date]:
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())
    with engine.connect() as conn:
        rows = conn.execute(
            _HOURS_BY_DAY,
            {"plant_id": plant_id, "start_dt": start_dt, "end_dt": end_dt},
        ).mappings()
        complete = {row["day"] for row in rows if int(row["hours"] or 0) >= 24}

    return [
        start + timedelta(days=offset)
        for offset in range((end - start).days + 1)
        if start + timedelta(days=offset) not in complete
    ]
