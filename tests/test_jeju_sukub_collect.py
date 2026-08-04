import asyncio
from datetime import date

import pandas as pd
import pytest

from fetch_data.jeju import jeju_sukub_collect


CSV_HEADER = (
    "\uae30\uc900\uc77c\uc2dc,\uacf5\uae09\ub2a5\ub825(MW),\ud604\uc7ac\uc218\uc694(MW),"
    "\uc2e0\uc7ac\uc0dd\ucd1d\ud569(MW),\uc2e0\uc7ac\uc0dd\ud0dc\uc591\uad11(MW),\uc2e0\uc7ac\uc0dd\ud48d\ub825(MW)"
)


def _source_csv(*rows: str) -> bytes:
    return (CSV_HEADER + "\n" + "\n".join(rows) + "\n").encode()


def _existing_rows() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "timestamp": "2026-07-01 00:00:00",
            "supply_mw": "10",
            "demand_mw": "20",
            "renewable_total_mw": "30",
            "solar_mw": "40",
            "wind_mw": "50",
        },
        {
            "timestamp": "2026-07-03 00:00:00",
            "supply_mw": "30",
            "demand_mw": "40",
            "renewable_total_mw": "50",
            "solar_mw": "60",
            "wind_mw": "70",
        },
    ])


def test_existing_month_fetches_and_merges_backfill_with_realtime_rows(tmp_path, monkeypatch):
    month = date(2026, 7, 1)
    path = tmp_path / "jeju_sukub_202607.csv"
    _existing_rows().to_csv(path, index=False, encoding="utf-8-sig")
    calls = []

    async def fetch_month(session, sem, first, last):
        calls.append((first, last))
        return _source_csv(
            "20260701000000,101,120,130,140,150",
            "20260702000000,202,220,230,240,250",
            "20260702000000,203,320,330,340,350",
        )

    monkeypatch.setattr(jeju_sukub_collect, "OUT_DIR", tmp_path)
    monkeypatch.setattr(jeju_sukub_collect, "_fetch_month", fetch_month)

    assert asyncio.run(jeju_sukub_collect._run_async(month, date(2026, 7, 31))) == [path]
    assert calls == [(date(2026, 7, 1), date(2026, 7, 31))]

    saved = pd.read_csv(path)
    assert pd.to_datetime(saved["timestamp"]).tolist() == [
        pd.Timestamp("2026-07-01 00:00:00"),
        pd.Timestamp("2026-07-02 00:00:00"),
        pd.Timestamp("2026-07-03 00:00:00"),
    ]
    assert saved["supply_mw"].tolist() == [101, 203, 30]
    assert saved["timestamp"].is_unique


@pytest.mark.parametrize("response", [None, _source_csv()])
def test_existing_month_is_retained_when_backfill_fails_or_is_empty(tmp_path, monkeypatch, response):
    month = date(2026, 7, 1)
    path = tmp_path / "jeju_sukub_202607.csv"
    existing = _existing_rows()
    existing.to_csv(path, index=False, encoding="utf-8-sig")
    original_bytes = path.read_bytes()
    calls = []

    async def fetch_month(session, sem, first, last):
        calls.append((first, last))
        return response

    monkeypatch.setattr(jeju_sukub_collect, "OUT_DIR", tmp_path)
    monkeypatch.setattr(jeju_sukub_collect, "_fetch_month", fetch_month)

    assert asyncio.run(jeju_sukub_collect._run_async(month, date(2026, 7, 31))) == []
    assert calls == [(date(2026, 7, 1), date(2026, 7, 31))]
    assert path.read_bytes() == original_bytes
