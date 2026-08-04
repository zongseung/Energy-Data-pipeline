import asyncio
from datetime import date
import logging
import threading

import pandas as pd
import pytest

from fetch_data.jeju import jeju_realtime_collect, jeju_sukub_collect


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
def test_existing_month_is_retained_and_run_fails_when_backfill_is_empty(
    tmp_path, monkeypatch, response
):
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

    with pytest.raises(RuntimeError, match="Jeju monthly collection failed"):
        asyncio.run(jeju_sukub_collect._run_async(month, date(2026, 7, 31)))
    assert calls == [(date(2026, 7, 1), date(2026, 7, 31))]
    assert path.read_bytes() == original_bytes


def test_backfill_blank_cells_do_not_overwrite_existing_values(tmp_path, monkeypatch):
    month = date(2026, 7, 1)
    path = tmp_path / "jeju_sukub_202607.csv"
    _existing_rows().to_csv(path, index=False, encoding="utf-8-sig")
    monkeypatch.setattr(jeju_sukub_collect, "OUT_DIR", tmp_path)

    assert jeju_sukub_collect._save_month(
        _source_csv("20260701000000,,120,130,140,150"), month
    ) == path

    saved = pd.read_csv(path)
    first = saved.loc[
        pd.to_datetime(saved["timestamp"]) == pd.Timestamp("2026-07-01")
    ].iloc[0]
    assert first["supply_mw"] == 10
    assert first["demand_mw"] == 120


@pytest.mark.parametrize("contents", [
    b"",
    b"not_timestamp,supply_mw\nvalue,10\n",
    b"timestamp,supply_mw\nnot-a-timestamp,10\n",
])
def test_malformed_existing_month_is_logged_and_retained(tmp_path, monkeypatch, caplog, contents):
    month = date(2026, 7, 1)
    path = tmp_path / "jeju_sukub_202607.csv"
    path.write_bytes(contents)

    monkeypatch.setattr(jeju_sukub_collect, "OUT_DIR", tmp_path)

    with caplog.at_level(logging.WARNING, logger=jeju_sukub_collect.logger.name):
        assert jeju_sukub_collect._save_month(
            _source_csv("20260701000000,101,120,130,140,150"), month
        ) is None

    assert path.read_bytes() == contents
    assert "\uae30\uc874 \ud30c\uc77c" in caplog.text
    assert "\uc800\uc7a5 \uc0dd\ub7b5" in caplog.text


def test_realtime_row_after_backfill_read_is_not_overwritten(tmp_path, monkeypatch):
    from fetch_data.jeju import jeju_csv_store

    month = date(2026, 7, 1)
    path = tmp_path / "jeju_sukub_202607.csv"
    pd.DataFrame([_existing_rows().iloc[0]]).to_csv(path, index=False, encoding="utf-8-sig")
    backfill_ready = threading.Event()
    realtime_started = threading.Event()
    release_backfill = threading.Event()
    original_atomic_to_csv = jeju_csv_store.atomic_to_csv
    backfill_results = []
    realtime_results = []

    def pause_backfill_before_replace(frame, output_path):
        if threading.current_thread().name == "backfill":
            backfill_ready.set()
            assert realtime_started.wait(timeout=2)
            assert release_backfill.wait(timeout=2)
        original_atomic_to_csv(frame, output_path)

    class FixedDate(date):
        @classmethod
        def today(cls):
            return date(2026, 7, 3)

    async def fetch_today(session, target):
        return _source_csv("20260703000000,303,320,330,340,350")

    monkeypatch.setattr(jeju_sukub_collect, "OUT_DIR", tmp_path)
    monkeypatch.setattr(jeju_realtime_collect, "OUT_DIR", tmp_path)
    monkeypatch.setattr(jeju_realtime_collect, "date", FixedDate)
    monkeypatch.setattr(jeju_realtime_collect, "_fetch_today", fetch_today)
    monkeypatch.setattr(jeju_csv_store, "atomic_to_csv", pause_backfill_before_replace)

    def run_backfill():
        backfill_results.append(
            jeju_sukub_collect._save_month(
                _source_csv(
                    "20260701000000,101,120,130,140,150",
                    "20260702000000,202,220,230,240,250",
                ),
                month,
            )
        )

    def run_realtime():
        realtime_started.set()
        realtime_results.append(asyncio.run(jeju_realtime_collect._poll_once(object())))

    backfill = threading.Thread(target=run_backfill, name="backfill")
    realtime = threading.Thread(target=run_realtime, name="realtime")
    backfill.start()
    assert backfill_ready.wait(timeout=2)
    realtime.start()
    assert realtime_started.wait(timeout=2)
    release_backfill.set()
    backfill.join(timeout=2)
    realtime.join(timeout=2)

    assert not backfill.is_alive()
    assert not realtime.is_alive()
    assert backfill_results == [path]
    assert realtime_results == [1]
    saved = pd.read_csv(path)
    assert pd.to_datetime(saved["timestamp"]).tolist() == [
        pd.Timestamp("2026-07-01 00:00:00"),
        pd.Timestamp("2026-07-02 00:00:00"),
        pd.Timestamp("2026-07-03 00:00:00"),
    ]
    assert saved["supply_mw"].tolist() == [101, 202, 303]
