from datetime import date

import pytest

from fetch_data.smp import smp_realtime
from prefect_flows import smp_flow


def _unconfirmed_grid():
    return [["구분", "구분", "08.04"]] + [
        [
            f"{slot // 4 + 1}h",
            f"{slot % 4 + 1}구간",
            "확정가격은D+1일18시까지공표예정입니다.",
        ]
        for slot in range(96)
    ]


def _numeric_grid():
    return [["구분", "구분", "08.04"]] + [
        [f"{slot // 4 + 1}h", f"{slot % 4 + 1}구간", str(slot + 1)]
        for slot in range(96)
    ]


def test_realtime_smp_unconfirmed_grid_returns_zero_without_upsert(monkeypatch):
    upserts = []
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: _unconfirmed_grid())
    monkeypatch.setattr(
        smp_realtime.C,
        "upsert_realtime_jeju",
        lambda *args, **kwargs: upserts.append(args),
    )

    assert smp_realtime.run_realtime_collection() == 0
    assert upserts == []


def test_realtime_smp_rowspan_unconfirmed_grid_returns_zero(monkeypatch):
    grid = _unconfirmed_grid()
    for row in grid[5:]:
        row[-1] = ""
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    assert smp_realtime.run_realtime_collection() == 0


def test_realtime_smp_all_empty_grid_raises_source_format_error(monkeypatch):
    grid = _unconfirmed_grid()
    for row in grid[1:]:
        row[-1] = ""
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_failed_grid_raises_stale_source_error(monkeypatch):
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="제주 실시간 SMP 원천 데이터가 비어 있습니다"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_truncated_grid_raises_source_format_error(monkeypatch):
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(
        smp_realtime,
        "fetch_grid",
        lambda *args, **kwargs: _unconfirmed_grid()[:-1],
    )

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_duplicate_slot_grid_raises_source_format_error(monkeypatch):
    grid = _unconfirmed_grid()
    grid[-1] = grid[-2]
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_unexpected_price_cell_raises_source_format_error(monkeypatch):
    grid = _numeric_grid()
    grid[-1][-1] = "bad cell"
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


@pytest.mark.parametrize("value", ["NaN", "inf", "-inf"])
def test_realtime_smp_non_finite_price_raises_source_format_error(monkeypatch, value):
    grid = _numeric_grid()
    grid[-1][-1] = value
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_marker_with_error_prefix_raises_source_format_error(monkeypatch):
    grid = _unconfirmed_grid()
    for row in grid[1:]:
        row[-1] = "ERROR: 확정가격은 D+1일 18시 공표 malformed"
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_duplicate_date_headers_raise_source_format_error(monkeypatch):
    grid = [["구분", "구분", "08.03", "08.03"]] + [
        [f"{slot // 4 + 1}h", f"{slot % 4 + 1}구간", "1", "2"]
        for slot in range(96)
    ]
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: grid)
    monkeypatch.setattr(smp_realtime.C, "upsert_realtime_jeju", lambda *args, **kwargs: 0)

    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime.run_realtime_collection()


def test_realtime_smp_backfill_missing_grid_raises_source_error(monkeypatch):
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *args, **kwargs: None)
    monkeypatch.setattr(smp_realtime.C, "get_engine_for", lambda *args, **kwargs: object())

    with pytest.raises(RuntimeError, match="원천 데이터가 비어 있습니다"):
        smp_realtime.run_realtime_backfill(
            start=date(2026, 8, 3),
            end=date(2026, 8, 3),
        )


def test_realtime_smp_task_returns_zero_for_unconfirmed_grid(monkeypatch):
    monkeypatch.setattr(smp_flow, "run_realtime_collection", lambda: 0)

    assert smp_flow.run_smp_realtime_task.fn() == 0


# ---------------------------------------------------------------------------
# 헤더 날짜 해석 — KPX 는 주말/공휴일 열을 통째로 건너뛰고 준다.
# 예전 구현은 '연속된 매일'을 가정해 +1일씩 증가시키다 어긋나면 예외를 던졌고,
# 그 탓에 2026-08-07 부터 매일 수집이 죽었다(실측 헤더: 08.07금 → 08.10월).
# 결번은 견디되 순서/중복 방어는 유지되는지 함께 본다.
# ---------------------------------------------------------------------------

def _hdr(*labels):
    return ["구분", "구분", *labels]


def test_header_dates_tolerates_weekend_gaps():
    """실측 사례: 금요일 다음 열이 월요일이다. 예외 없이 실제 날짜로 읽어야 한다."""
    got = smp_realtime._header_dates(
        _hdr("08.07(금)", "08.10(월)", "08.11(화)"), ref=date(2026, 8, 13)
    )
    assert got[2:] == [date(2026, 8, 7), date(2026, 8, 10), date(2026, 8, 11)]


def test_header_dates_spans_multiple_years():
    """창이 1년을 넘으면 MM.DD 만으로 연도를 못 정한다 — 역순 지점에서 +1년.

    롤오버가 1회이므로 마지막 열(ref 기준 2026)에서 역산해 기준연도는 2025다.
    """
    got = smp_realtime._header_dates(
        _hdr("12.30", "12.31", "01.01", "01.02"), ref=date(2026, 8, 13)
    )
    assert got[2:] == [
        date(2025, 12, 30), date(2025, 12, 31), date(2026, 1, 1), date(2026, 1, 2),
    ]


def test_header_dates_rejects_duplicate_dates():
    """같은 날짜가 두 번 오면 그 날 96슬롯이 중복 적재된다 — 막아야 한다."""
    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime._header_dates(_hdr("08.03", "08.03"), ref=date(2026, 8, 13))


def test_header_dates_rejects_backwards_within_same_year():
    """역순 열도 거부한다(롤오버로 오해하면 연도가 통째로 어긋난다)."""
    with pytest.raises(RuntimeError, match="원천 데이터 형식"):
        smp_realtime._header_dates(_hdr("08.05", "08.04", "08.06"), ref=date(2026, 8, 13))


def test_empty_result_carries_source_diagnosis(monkeypatch):
    """0행일 때 '파서가 깨졌나 / 원천이 멈췄나' 를 로그로 구분할 수 있어야 한다."""
    monkeypatch.setattr(smp_realtime, "make_session", lambda: object())
    monkeypatch.setattr(smp_realtime, "fetch_grid", lambda *a, **k: _unconfirmed_grid())
    df = smp_realtime.parse_realtime_grid(_unconfirmed_grid(), ref=date(2026, 8, 13))
    assert df.empty
    assert df.attrs["confirmed_last"] is None
    assert df.attrs["unconfirmed_days"] == 1
    assert df.attrs["source_last"] == date(2026, 8, 4)
