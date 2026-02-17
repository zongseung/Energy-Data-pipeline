import pandas as pd
import numpy as np
import polars as pl
from scipy.interpolate import interp1d
from pathlib import Path
import time
from collections import defaultdict


# ──────────────────────────────────────────────
# 내부 함수 (numpy / polars 기반)
# ──────────────────────────────────────────────

def find_consecutive_missing_groups(series_or_arr):
    """
    연속된 결측치 그룹을 찾아서 (시작 인덱스, 길이) 튜플 리스트로 반환.
    pd.Series 또는 numpy 배열 모두 허용.
    """
    if isinstance(series_or_arr, (pd.Series, pl.Series)):
        is_missing = series_or_arr.is_null().to_numpy() if isinstance(series_or_arr, pl.Series) else series_or_arr.isna().values
    else:
        is_missing = np.isnan(series_or_arr)

    groups = []
    n = len(is_missing)
    i = 0
    while i < n:
        if is_missing[i]:
            start_idx = i
            length = 1
            i += 1
            while i < n and is_missing[i]:
                length += 1
                i += 1
            groups.append((start_idx, length))
        else:
            i += 1
    return groups


def _spline_impute(values, start_idx, length):
    """
    numpy 배열 기반 스플라인 보간. 결측 구간의 보간된 값 배열을 반환.
    values를 in-place로 수정한다.
    """
    gap = slice(start_idx, start_idx + length)
    before = values[:start_idx]
    after = values[start_idx + length:]

    valid_before_mask = ~np.isnan(before)
    valid_after_mask = ~np.isnan(after)

    has_before = valid_before_mask.any()
    has_after = valid_after_mask.any()

    if not has_before or not has_after:
        # 전후 유효값 없으면 선형 보간 (np.interp)
        all_vals = values.copy()
        valid_mask = ~np.isnan(all_vals)
        if valid_mask.sum() >= 2:
            xp = np.where(valid_mask)[0]
            fp = all_vals[valid_mask]
            x_missing = np.arange(start_idx, start_idx + length)
            values[gap] = np.interp(x_missing, xp, fp)
        return

    x_before = np.where(valid_before_mask)[0]
    y_before = before[valid_before_mask]
    x_after = np.where(valid_after_mask)[0] + start_idx + length
    y_after = after[valid_after_mask]

    x_all = np.concatenate([x_before, x_after])
    y_all = np.concatenate([y_before, y_after])

    sort_idx = np.argsort(x_all)
    x_all = x_all[sort_idx]
    y_all = y_all[sort_idx]

    if len(x_all) < 2:
        valid_mask = ~np.isnan(values)
        if valid_mask.sum() >= 2:
            xp = np.where(valid_mask)[0]
            fp = values[valid_mask]
            x_missing = np.arange(start_idx, start_idx + length)
            values[gap] = np.interp(x_missing, xp, fp)
        return

    try:
        f = interp1d(x_all, y_all, kind='cubic', fill_value='extrapolate')
        x_missing = np.arange(start_idx, start_idx + length)
        values[gap] = f(x_missing)
    except Exception:
        valid_mask = ~np.isnan(values)
        if valid_mask.sum() >= 2:
            xp = np.where(valid_mask)[0]
            fp = values[valid_mask]
            x_missing = np.arange(start_idx, start_idx + length)
            values[gap] = np.interp(x_missing, xp, fp)


def _build_historical_lookup(plf_station, column):
    """
    polars DataFrame으로 (month, day, hour) → mean 룩업 dict를 생성.
    """
    if "_parsed_date" not in plf_station.columns:
        return {}, None

    agg = (
        plf_station
        .select([
            pl.col("_parsed_date").dt.month().alias("_m"),
            pl.col("_parsed_date").dt.day().alias("_d"),
            pl.col("_parsed_date").dt.hour().alias("_h"),
            pl.col(column).alias("_val"),
        ])
        .group_by(["_m", "_d", "_h"])
        .agg(pl.col("_val").mean())
    )
    rows = agg.to_numpy()  # shape: (n, 4) — _m, _d, _h, _val
    lookup = {}
    for row in rows:
        m, d, h, v = int(row[0]), int(row[1]), int(row[2]), row[3]
        if v is not None and not np.isnan(v):
            lookup[(m, d, h)] = v

    # station 전체 평균
    station_mean = plf_station[column].mean()

    return lookup, station_mean


# ──────────────────────────────────────────────
# 외부 호환 래퍼 (테스트에서 직접 import)
# ──────────────────────────────────────────────

def spline_impute(series, start_idx, length):
    """
    하위 호환 래퍼 — 테스트에서 pd.Series로 호출.
    내부적으로 _spline_impute(numpy) 사용.
    """
    arr = series.values.copy()
    _spline_impute(arr, start_idx, length)
    series.iloc[start_idx:start_idx + length] = arr[start_idx:start_idx + length]
    return series


def historical_average_impute(df, station_name, column, start_idx, length, date_col='tm', station_col='stnNm'):
    """
    하위 호환 래퍼 — 기존 시그니처 유지.
    내부적으로 polars groupby + numpy 대입 사용.
    """
    plf = pl.from_pandas(df)

    # 날짜 파싱
    if plf[date_col].dtype == pl.Utf8:
        plf = plf.with_columns(pl.col(date_col).str.to_datetime().alias("_parsed_date"))
    else:
        plf = plf.with_columns(pl.col(date_col).alias("_parsed_date"))

    plf_station = plf.filter(pl.col(station_col) == station_name)
    if len(plf_station) == 0:
        return df

    lookup, station_mean = _build_historical_lookup(plf_station, column)
    all_mean = plf[column].mean()

    # 결측 날짜 추출
    missing_dates_raw = df.iloc[start_idx:start_idx + length][date_col]
    if isinstance(missing_dates_raw.iloc[0], str):
        missing_dates_parsed = pd.to_datetime(missing_dates_raw)
    else:
        missing_dates_parsed = missing_dates_raw

    col_loc = df.columns.get_loc(column)
    for idx, dt in zip(range(start_idx, start_idx + length), missing_dates_parsed):
        key = (dt.month, dt.day, dt.hour)
        if key in lookup:
            df.iloc[idx, col_loc] = lookup[key]
        elif station_mean is not None and not np.isnan(station_mean):
            df.iloc[idx, col_loc] = station_mean
        elif all_mean is not None and not np.isnan(all_mean):
            df.iloc[idx, col_loc] = all_mean

    return df


# ──────────────────────────────────────────────
# 메인 함수 (polars 전처리 + numpy 코어)
# ──────────────────────────────────────────────

def impute_missing_values(df, columns=['ta', 'hm'], date_col='tm', station_col='stnNm', debug=True):
    """
    결측치를 처리하는 메인 함수
    - 연속 3개 이하: 스플라인 보간
    - 연속 4개 이상: 같은 지역의 다른 연도 동일 월-일-시 평균값

    Parameters:
    -----------
    debug : bool
        True일 경우 상세한 디버깅 정보를 출력하고 반환합니다.

    Returns:
    --------
    df : DataFrame (pandas)
        결측치가 처리된 데이터프레임
    debug_info : dict (debug=True일 때만)
        디버깅 정보 딕셔너리
    """
    start_time = time.time()

    # 디버깅 정보 초기화
    debug_info = {
        'before': {},
        'after': {},
        'processing_stats': defaultdict(lambda: {'spline': 0, 'historical': 0, 'total_missing': 0}),
        'station_stats': defaultdict(lambda: defaultdict(int)),
        'missing_groups_by_length': defaultdict(int),
        'processing_time': 0
    }

    # ── pandas → polars 변환 ──
    plf = pl.from_pandas(df)

    # 컬럼 타입 확인 및 숫자 변환
    for col in columns:
        if col in plf.columns:
            if plf[col].dtype == pl.Utf8:
                plf = plf.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                if debug:
                    print(f"경고: {col} 컬럼이 문자열이었습니다. 숫자로 변환했습니다.")

    # 처리 전 통계 수집
    if debug:
        print("\n" + "="*80)
        print("결측치 처리 전 통계")
        print("="*80)
        print(f"전체 데이터 shape: {plf.shape}")
        print(f"전체 행 수: {len(plf)}")

        for col in columns:
            if col in plf.columns:
                s = plf[col]
                missing_count = s.null_count()
                missing_pct = (missing_count / len(plf)) * 100
                col_mean = s.mean() if missing_count < len(plf) else None
                col_std = s.std() if missing_count < len(plf) else None
                debug_info['before'][col] = {
                    'missing_count': missing_count,
                    'missing_pct': missing_pct,
                    'mean': col_mean,
                    'std': col_std,
                }
                print(f"\n{col} 컬럼:")
                print(f"  결측치 개수: {missing_count} ({missing_pct:.2f}%)")
                if col_mean is not None:
                    print(f"  평균: {col_mean:.2f}")
                    print(f"  표준편차: {col_std:.2f}")

    # 날짜 컬럼 확인
    if date_col not in plf.columns:
        if 'date' in plf.columns:
            date_col = 'date'
        else:
            raise ValueError(f"날짜 컬럼을 찾을 수 없습니다. {date_col} 또는 'date' 컬럼이 필요합니다.")

    # 날짜 파싱 (polars — 5~10x faster)
    if plf[date_col].dtype == pl.Utf8:
        plf = plf.with_columns(pl.col(date_col).str.to_datetime().alias("_parsed_date"))
    else:
        plf = plf.with_columns(pl.col(date_col).alias("_parsed_date"))

    # 지역 컬럼 확인
    if station_col not in plf.columns:
        if 'station_name' in plf.columns:
            station_col = 'station_name'
        else:
            raise ValueError(f"지역 컬럼을 찾을 수 없습니다. {station_col} 또는 'station_name' 컬럼이 필요합니다.")

    stations = plf[station_col].unique().to_list()

    if debug:
        print(f"\n지역 수: {len(stations)}")
        print(f"지역 목록: {sorted(stations)}")
        print("\n" + "="*80)
        print("결측치 처리 시작")
        print("="*80)

    # ── numpy 배열 추출 (mutable copy) ──
    col_arrays = {}
    for col in columns:
        if col in plf.columns:
            col_arrays[col] = plf[col].to_numpy(allow_copy=True).astype(np.float64)

    station_arr = plf[station_col].to_numpy()
    parsed_dates = plf["_parsed_date"].to_numpy()  # datetime64 array
    date_col_arr = plf[date_col].to_numpy()

    # 전체 평균 사전 계산
    all_means = {col: np.nanmean(col_arrays[col]) if not np.all(np.isnan(col_arrays[col])) else np.nan
                 for col in col_arrays}

    # ── 지역별 처리 루프 (numpy 코어) ──
    total_groups_processed = 0

    for station_idx, station in enumerate(sorted(stations), 1):
        station_mask = (station_arr == station)
        indices = np.where(station_mask)[0]

        if debug:
            print(f"\n[{station_idx}/{len(stations)}] 지역: {station} (데이터 {len(indices)}개)")

        # station polars subset (historical lookup용)
        plf_station = plf.filter(pl.col(station_col) == station)

        for col in columns:
            if col not in col_arrays:
                if debug:
                    print(f"  경고: {col} 컬럼이 없습니다. 건너뜁니다.")
                continue

            arr = col_arrays[col]
            station_vals = arr[indices].copy()
            initial_missing = int(np.isnan(station_vals).sum())

            if initial_missing == 0:
                if debug:
                    print(f"  {col}: 결측치 없음")
                continue

            missing_groups = find_consecutive_missing_groups(station_vals)

            if debug:
                print(f"  {col}: 결측치 {initial_missing}개, 연속 그룹 {len(missing_groups)}개")

            # historical lookup 사전 계산 (이 station+column에 대해 1회)
            lookup, station_mean = _build_historical_lookup(plf_station, col)

            for group_idx, (start_idx, length) in enumerate(missing_groups, 1):
                total_groups_processed += 1
                debug_info['missing_groups_by_length'][length] += 1
                debug_info['station_stats'][station][f'{col}_missing_groups'] += 1
                debug_info['station_stats'][station][f'{col}_missing_values'] += length

                if debug:
                    actual_global_idx = indices[start_idx]
                    missing_date = date_col_arr[actual_global_idx]
                    print(f"    그룹 {group_idx}: 연속 {length}개 결측치 (시작: {missing_date})")

                if length <= 3:
                    # 스플라인 보간 (numpy/scipy)
                    debug_info['processing_stats'][col]['spline'] += 1
                    debug_info['processing_stats'][col]['total_missing'] += length
                    if debug:
                        print(f"      → 스플라인 보간 적용")
                    _spline_impute(station_vals, start_idx, length)
                else:
                    # 역사적 평균값 (polars lookup)
                    debug_info['processing_stats'][col]['historical'] += 1
                    debug_info['processing_stats'][col]['total_missing'] += length
                    if debug:
                        print(f"      → 역사적 평균값 사용")

                    for k in range(start_idx, start_idx + length):
                        global_idx = indices[k]
                        dt = parsed_dates[global_idx]
                        dt_ts = pd.Timestamp(dt)
                        key = (dt_ts.month, dt_ts.day, dt_ts.hour)

                        if key in lookup:
                            station_vals[k] = lookup[key]
                        elif station_mean is not None and not np.isnan(station_mean):
                            station_vals[k] = station_mean
                        elif not np.isnan(all_means.get(col, np.nan)):
                            station_vals[k] = all_means[col]

            # station 결과를 전체 배열에 반영
            arr[indices] = station_vals

            final_missing = int(np.isnan(arr[indices]).sum())
            if debug and final_missing < initial_missing:
                print(f"  {col}: {initial_missing}개 → {final_missing}개 결측치 (처리 완료)")
            elif debug and final_missing > 0:
                print(f"  {col}: 경고 - 여전히 {final_missing}개 결측치 남음")

    # ── numpy → polars → pandas 변환 ──
    for col in col_arrays:
        plf = plf.with_columns(pl.Series(col, col_arrays[col]))

    # 임시 컬럼 제거
    temp_cols = [c for c in plf.columns if c.startswith('_')]
    if temp_cols:
        plf = plf.drop(temp_cols)

    result_df = plf.to_pandas()

    # 처리 후 통계 수집
    processing_time = time.time() - start_time
    debug_info['processing_time'] = processing_time

    if debug:
        print("\n" + "="*80)
        print("결측치 처리 후 통계")
        print("="*80)
        print(f"전체 데이터 shape: {result_df.shape}")

        for col in columns:
            if col in result_df.columns:
                missing_count = int(result_df[col].isna().sum())
                missing_pct = (missing_count / len(result_df)) * 100
                col_mean = result_df[col].mean() if not result_df[col].isna().all() else None
                col_std = result_df[col].std() if not result_df[col].isna().all() else None
                debug_info['after'][col] = {
                    'missing_count': missing_count,
                    'missing_pct': missing_pct,
                    'mean': col_mean,
                    'std': col_std,
                }
                print(f"\n{col} 컬럼:")
                print(f"  결측치 개수: {missing_count} ({missing_pct:.2f}%)")
                if col_mean is not None:
                    print(f"  평균: {col_mean:.2f}")
                    print(f"  표준편차: {col_std:.2f}")

                if col in debug_info['before']:
                    before_count = debug_info['before'][col]['missing_count']
                    after_count = missing_count
                    reduced = before_count - after_count
                    reduction_pct = (reduced / before_count * 100) if before_count > 0 else 0
                    print(f"  처리 전: {before_count}개 → 처리 후: {after_count}개 (감소: {reduced}개, {reduction_pct:.1f}%)")

        print("\n" + "="*80)
        print("처리 방법별 통계")
        print("="*80)
        for col in columns:
            if col in debug_info['processing_stats']:
                stats = debug_info['processing_stats'][col]
                print(f"\n{col} 컬럼:")
                print(f"  스플라인 보간: {stats['spline']}개 그룹")
                print(f"  역사적 평균: {stats['historical']}개 그룹")
                print(f"  총 처리된 결측치: {stats['total_missing']}개")

        print("\n" + "="*80)
        print("연속 결측치 그룹 길이별 분포")
        print("="*80)
        for length in sorted(debug_info['missing_groups_by_length'].keys()):
            count = debug_info['missing_groups_by_length'][length]
            print(f"  길이 {length}: {count}개 그룹")

        print("\n" + "="*80)
        print(f"총 처리 시간: {processing_time:.2f}초")
        print(f"처리된 그룹 수: {total_groups_processed}개")
        print("="*80 + "\n")

    if debug:
        return result_df, debug_info
    else:
        return result_df


def main():
    """
    메인 실행 함수
    """
    csv_path = input("CSV 파일 경로를 입력하세요: ").strip()

    if not Path(csv_path).exists():
        print(f"오류: 파일을 찾을 수 없습니다: {csv_path}")
        return

    print(f"CSV 파일 읽는 중: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    print(f"원본 데이터 shape: {df.shape}")
    print(f"ta 결측치: {df['ta'].isna().sum()}")
    print(f"hm 결측치: {df['hm'].isna().sum()}")

    print("\n결측치 처리 중...")
    df_imputed, debug_info = impute_missing_values(df, columns=['ta', 'hm'], debug=True)

    print(f"\n처리 후 데이터 shape: {df_imputed.shape}")
    print(f"ta 결측치: {df_imputed['ta'].isna().sum()}")
    print(f"hm 결측치: {df_imputed['hm'].isna().sum()}")

    output_path = csv_path.replace('.csv', '_imputed.csv')
    df_imputed.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n결과 저장 완료: {output_path}")


if __name__ == "__main__":
    main()
