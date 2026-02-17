"""
impute_missing.py 벤치마크 스크립트

사용법:
    uv run python tests/benchmark/profile_impute.py
"""

import time
import numpy as np
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fetch_data.common.impute_missing import (
    find_consecutive_missing_groups,
    historical_average_impute,
    impute_missing_values,
    spline_impute,
)


def generate_weather_data(n_rows: int, n_stations: int = 3, missing_ratio: float = 0.05, seed: int = 42) -> pd.DataFrame:
    """벤치마크용 기상 데이터 생성"""
    rng = np.random.default_rng(seed)
    hours_per_station = n_rows // n_stations
    stations = [f"station_{i}" for i in range(n_stations)]

    rows = []
    for station in stations:
        dates = pd.date_range("2020-01-01", periods=hours_per_station, freq="h")
        ta = 15.0 + 10.0 * np.sin(np.arange(hours_per_station) * 2 * np.pi / 24) + rng.normal(0, 2, hours_per_station)
        hm = 60.0 + 20.0 * np.sin(np.arange(hours_per_station) * 2 * np.pi / 24) + rng.normal(0, 5, hours_per_station)

        # 결측치 삽입: 연속 그룹으로 삽입 (현실적 패턴)
        n_missing_groups = max(1, int(hours_per_station * missing_ratio / 3))
        for _ in range(n_missing_groups):
            start = rng.integers(0, hours_per_station - 6)
            length = rng.integers(1, 7)  # 1~6개 연속 결측
            ta[start:start + length] = np.nan
            # hm은 다른 위치에 결측
            start2 = rng.integers(0, hours_per_station - 6)
            length2 = rng.integers(1, 7)
            hm[start2:start2 + length2] = np.nan

        df_station = pd.DataFrame({
            "tm": dates.strftime("%Y-%m-%d %H:%M"),
            "stnNm": station,
            "ta": ta,
            "hm": hm,
        })
        rows.append(df_station)

    return pd.concat(rows, ignore_index=True)


def bench_find_groups(series: pd.Series, repeats: int = 5) -> float:
    """find_consecutive_missing_groups 벤치마크"""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        find_consecutive_missing_groups(series)
        times.append(time.perf_counter() - t0)
    return np.median(times)


def bench_spline(series: pd.Series, groups: list, repeats: int = 5) -> float:
    """spline_impute 벤치마크 (<=3 길이 그룹만)"""
    short_groups = [(s, l) for s, l in groups if l <= 3]
    if not short_groups:
        return 0.0
    times = []
    for _ in range(repeats):
        s = series.copy()
        t0 = time.perf_counter()
        for start_idx, length in short_groups:
            spline_impute(s, start_idx, length)
        times.append(time.perf_counter() - t0)
    return np.median(times)


def bench_historical(df: pd.DataFrame, station: str, column: str, groups: list, repeats: int = 3) -> float:
    """historical_average_impute 벤치마크 (>3 길이 그룹만)"""
    long_groups = [(s, l) for s, l in groups if l > 3]
    if not long_groups:
        return 0.0
    times = []
    for _ in range(repeats):
        df_copy = df.copy()
        t0 = time.perf_counter()
        for start_idx, length in long_groups:
            historical_average_impute(df_copy, station, column, start_idx, length)
        times.append(time.perf_counter() - t0)
    return np.median(times)


def bench_impute_full(df: pd.DataFrame, repeats: int = 3) -> float:
    """impute_missing_values 전체 벤치마크"""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        impute_missing_values(df.copy(), debug=False)
        times.append(time.perf_counter() - t0)
    return np.median(times)


def run_benchmark():
    sizes = [300, 1_000, 3_000, 10_000]
    print("=" * 70)
    print("impute_missing.py 벤치마크")
    print("=" * 70)

    results = []

    for n_rows in sizes:
        print(f"\n--- n_rows = {n_rows:,} ---")
        df = generate_weather_data(n_rows, n_stations=3)
        missing_ta = df["ta"].isna().sum()
        missing_hm = df["hm"].isna().sum()
        print(f"  ta 결측: {missing_ta}, hm 결측: {missing_hm}")

        # 1) find_consecutive_missing_groups
        series = df["ta"].copy()
        t_groups = bench_find_groups(series)
        groups = find_consecutive_missing_groups(series)
        print(f"  find_groups:       {t_groups*1000:8.2f} ms  ({len(groups)} groups)")

        # 2) spline_impute
        t_spline = bench_spline(series, groups)
        n_spline = sum(1 for _, l in groups if l <= 3)
        print(f"  spline_impute:     {t_spline*1000:8.2f} ms  ({n_spline} groups)")

        # 3) historical_average_impute
        station = df["stnNm"].iloc[0]
        station_mask = df["stnNm"] == station
        station_indices = df[station_mask].index
        station_series = df.loc[station_indices, "ta"]
        station_groups = find_consecutive_missing_groups(station_series)
        # actual_start_idx 변환
        actual_groups = [(station_indices[s], l) for s, l in station_groups]
        t_hist = bench_historical(df, station, "ta", actual_groups)
        n_hist = sum(1 for _, l in station_groups if l > 3)
        print(f"  historical_avg:    {t_hist*1000:8.2f} ms  ({n_hist} groups)")

        # 4) 전체 impute_missing_values
        t_full = bench_impute_full(df)
        print(f"  impute_full:       {t_full*1000:8.2f} ms")

        results.append({
            "n_rows": n_rows,
            "find_groups_ms": round(t_groups * 1000, 2),
            "spline_ms": round(t_spline * 1000, 2),
            "historical_ms": round(t_hist * 1000, 2),
            "full_ms": round(t_full * 1000, 2),
        })

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"{'n_rows':>8} | {'find_groups':>12} | {'spline':>10} | {'historical':>12} | {'full':>10}")
    print("-" * 70)
    for r in results:
        print(f"{r['n_rows']:>8,} | {r['find_groups_ms']:>10.2f}ms | {r['spline_ms']:>8.2f}ms | {r['historical_ms']:>10.2f}ms | {r['full_ms']:>8.2f}ms")

    return results


if __name__ == "__main__":
    run_benchmark()
