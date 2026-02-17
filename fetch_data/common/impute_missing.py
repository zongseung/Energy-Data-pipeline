import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from pathlib import Path
import time
from collections import defaultdict

def find_consecutive_missing_groups(series):
    """
    연속된 결측치 그룹을 찾아서 (시작 인덱스, 길이) 튜플 리스트로 반환
    """
    is_missing = series.isna().values  # numpy 배열로 변환하여 .iloc 오버헤드 제거
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

def spline_impute(series, start_idx, length):
    """
    스플라인 보간을 사용하여 결측치를 채움
    """
    gap = slice(start_idx, start_idx + length)

    # 결측치 전후의 유효한 값들의 인덱스와 값
    valid_before = series.iloc[:start_idx].dropna()
    valid_after = series.iloc[start_idx + length:].dropna()

    if len(valid_before) == 0 or len(valid_after) == 0:
        # 전후 유효값이 없으면 선형 보간 사용
        series.iloc[gap] = series.interpolate(method='linear').iloc[gap]
        return series

    # 전후 유효값들을 합쳐서 스플라인 보간
    x_all = np.concatenate([valid_before.index.values, valid_after.index.values])
    y_all = np.concatenate([valid_before.values, valid_after.values])

    # 인덱스 순서대로 정렬
    sort_idx = np.argsort(x_all)
    x_all = x_all[sort_idx]
    y_all = y_all[sort_idx]

    # 최소 2개의 점이 필요
    if len(x_all) < 2:
        series.iloc[gap] = series.interpolate(method='linear').iloc[gap]
        return series

    try:
        # 스플라인 보간 (cubic)
        f = interp1d(x_all, y_all, kind='cubic', fill_value='extrapolate')
        x_missing = series.iloc[gap].index.values
        series.iloc[gap] = f(x_missing)
    except Exception:
        # 스플라인 실패 시 선형 보간
        series.iloc[gap] = series.interpolate(method='linear').iloc[gap]

    return series

def historical_average_impute(df, station_name, column, start_idx, length, date_col='tm', station_col='stnNm'):
    """
    같은 지역의 다른 연도 동일 월-일-시의 평균값으로 결측치를 채움
    """
    # 결측치가 있는 날짜들 추출
    missing_dates = df.iloc[start_idx:start_idx + length][date_col]

    # 날짜 파싱 (tm 컬럼 형식에 따라 다를 수 있음)
    if isinstance(missing_dates.iloc[0], str):
        missing_dates_parsed = pd.to_datetime(missing_dates)
    else:
        missing_dates_parsed = missing_dates

    # 같은 지역의 다른 데이터 찾기
    station_data = df[df[station_col] == station_name]

    if len(station_data) == 0:
        return df

    # 날짜 컬럼 파싱
    if date_col in station_data.columns:
        src_col = date_col
    elif 'date' in station_data.columns:
        src_col = 'date'
    else:
        src_col = station_data.columns[0]

    parsed_dates = pd.to_datetime(station_data[src_col])

    # (month, day, hour) 룩업 테이블 사전 계산 — 루프 내 반복 필터링 제거
    lookup_df = pd.DataFrame({
        '_month': parsed_dates.dt.month.values,
        '_day': parsed_dates.dt.day.values,
        '_hour': parsed_dates.dt.hour.values,
        '_val': station_data[column].values,
    })
    lookup = lookup_df.groupby(['_month', '_day', '_hour'])['_val'].mean()

    # fallback 값 사전 계산
    col_loc = df.columns.get_loc(column)
    station_all_mean = station_data[column].mean()  # skipna=True by default
    all_mean = df[column].mean()

    # 각 결측치에 대해 룩업 테이블에서 조회
    for idx, missing_date in zip(range(start_idx, start_idx + length), missing_dates_parsed):
        key = (missing_date.month, missing_date.day, missing_date.hour)

        if key in lookup.index:
            value = lookup[key]
            if not pd.isna(value):
                df.iloc[idx, col_loc] = value
                continue

        # fallback: 지역 전체 평균 → 전체 데이터 평균
        if not pd.isna(station_all_mean):
            df.iloc[idx, col_loc] = station_all_mean
        elif not pd.isna(all_mean):
            df.iloc[idx, col_loc] = all_mean

    return df

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
    df : DataFrame
        결측치가 처리된 데이터프레임
    debug_info : dict (debug=True일 때만)
        디버깅 정보 딕셔너리
    """
    start_time = time.time()
    df = df.copy()
    
    # 디버깅 정보 초기화
    debug_info = {
        'before': {},
        'after': {},
        'processing_stats': defaultdict(lambda: {'spline': 0, 'historical': 0, 'total_missing': 0}),
        'station_stats': defaultdict(lambda: defaultdict(int)),
        'missing_groups_by_length': defaultdict(int),
        'processing_time': 0
    }
    
    # 컬럼 타입 확인 및 숫자 변환
    for col in columns:
        if col in df.columns:
            # 문자열 타입이면 숫자로 변환 시도
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if debug:
                    print(f"경고: {col} 컬럼이 문자열이었습니다. 숫자로 변환했습니다.")
    
    # 처리 전 통계 수집
    if debug:
        print("\n" + "="*80)
        print("결측치 처리 전 통계")
        print("="*80)
        print(f"전체 데이터 shape: {df.shape}")
        print(f"전체 행 수: {len(df)}")
        
        for col in columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                missing_pct = (missing_count / len(df)) * 100
                debug_info['before'][col] = {
                    'missing_count': missing_count,
                    'missing_pct': missing_pct,
                    'mean': df[col].mean() if not df[col].isna().all() else None,
                    'std': df[col].std() if not df[col].isna().all() else None
                }
                print(f"\n{col} 컬럼:")
                print(f"  결측치 개수: {missing_count} ({missing_pct:.2f}%)")
                if debug_info['before'][col]['mean'] is not None:
                    print(f"  평균: {debug_info['before'][col]['mean']:.2f}")
                    print(f"  표준편차: {debug_info['before'][col]['std']:.2f}")
    
    # 날짜 컬럼 확인 및 파싱
    if date_col not in df.columns:
        # date 컬럼 시도
        if 'date' in df.columns:
            date_col = 'date'
        else:
            raise ValueError(f"날짜 컬럼을 찾을 수 없습니다. {date_col} 또는 'date' 컬럼이 필요합니다.")
    
    # 날짜 파싱
    if isinstance(df[date_col].iloc[0], str):
        df['_parsed_date'] = pd.to_datetime(df[date_col])
    else:
        df['_parsed_date'] = df[date_col]
    
    # 시간 정보 추출 (hour 컬럼이 있으면 사용, 없으면 날짜에서 추출)
    if 'hour' in df.columns:
        df['_hour'] = df['hour']
    else:
        df['_hour'] = df['_parsed_date'].dt.hour
    
    # 지역 컬럼 확인
    if station_col not in df.columns:
        if 'station_name' in df.columns:
            station_col = 'station_name'
        else:
            raise ValueError(f"지역 컬럼을 찾을 수 없습니다. {station_col} 또는 'station_name' 컬럼이 필요합니다.")
    
    if debug:
        print(f"\n지역 수: {df[station_col].nunique()}")
        print(f"지역 목록: {sorted(df[station_col].unique())}")
        print("\n" + "="*80)
        print("결측치 처리 시작")
        print("="*80)
    
    # 각 지역별로 처리
    total_groups_processed = 0
    for station_idx, station in enumerate(df[station_col].unique(), 1):
        station_mask = df[station_col] == station
        station_indices = df[station_mask].index
        
        if debug:
            print(f"\n[{station_idx}/{df[station_col].nunique()}] 지역: {station} (데이터 {len(station_indices)}개)")
        
        for col in columns:
            if col not in df.columns:
                if debug:
                    print(f"  경고: {col} 컬럼이 없습니다. 건너뜁니다.")
                continue
            
            # 해당 지역의 해당 컬럼만 추출
            station_series = df.loc[station_indices, col].copy()
            initial_missing = station_series.isna().sum()
            
            if initial_missing == 0:
                if debug:
                    print(f"  {col}: 결측치 없음")
                continue
            
            # 연속 결측치 그룹 찾기
            missing_groups = find_consecutive_missing_groups(station_series)
            
            if debug:
                print(f"  {col}: 결측치 {initial_missing}개, 연속 그룹 {len(missing_groups)}개")
            
            for group_idx, (start_idx, length) in enumerate(missing_groups, 1):
                total_groups_processed += 1
                debug_info['missing_groups_by_length'][length] += 1
                debug_info['station_stats'][station][f'{col}_missing_groups'] += 1
                debug_info['station_stats'][station][f'{col}_missing_values'] += length
                
                # 실제 데이터프레임의 인덱스로 변환
                actual_start_idx = station_indices[start_idx]
                
                # 결측치가 있는 날짜 정보
                if date_col in df.columns:
                    missing_date = df.iloc[actual_start_idx][date_col]
                    if debug:
                        print(f"    그룹 {group_idx}: 연속 {length}개 결측치 (시작: {missing_date})")
                
                if length <= 3:
                    # 스플라인 보간
                    debug_info['processing_stats'][col]['spline'] += 1
                    debug_info['processing_stats'][col]['total_missing'] += length
                    if debug:
                        print(f"      → 스플라인 보간 적용")
                    station_series = spline_impute(station_series, start_idx, length)
                    # 원본 데이터프레임에 반영
                    df.loc[station_indices, col] = station_series.values
                else:
                    # 역사적 평균값 사용
                    debug_info['processing_stats'][col]['historical'] += 1
                    debug_info['processing_stats'][col]['total_missing'] += length
                    if debug:
                        print(f"      → 역사적 평균값 사용")
                    df = historical_average_impute(df, station, col, actual_start_idx, length, date_col, station_col)
            
            # 처리 후 검증
            final_missing = df.loc[station_indices, col].isna().sum()
            if debug and final_missing < initial_missing:
                print(f"  {col}: {initial_missing}개 → {final_missing}개 결측치 (처리 완료)")
            elif debug and final_missing > 0:
                print(f"  {col}: 경고 - 여전히 {final_missing}개 결측치 남음")
    
    # 처리 후 통계 수집
    processing_time = time.time() - start_time
    debug_info['processing_time'] = processing_time
    
    if debug:
        print("\n" + "="*80)
        print("결측치 처리 후 통계")
        print("="*80)
        print(f"전체 데이터 shape: {df.shape}")
        
        for col in columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                missing_pct = (missing_count / len(df)) * 100
                debug_info['after'][col] = {
                    'missing_count': missing_count,
                    'missing_pct': missing_pct,
                    'mean': df[col].mean() if not df[col].isna().all() else None,
                    'std': df[col].std() if not df[col].isna().all() else None
                }
                print(f"\n{col} 컬럼:")
                print(f"  결측치 개수: {missing_count} ({missing_pct:.2f}%)")
                if debug_info['after'][col]['mean'] is not None:
                    print(f"  평균: {debug_info['after'][col]['mean']:.2f}")
                    print(f"  표준편차: {debug_info['after'][col]['std']:.2f}")
                
                # 처리 전후 비교
                if col in debug_info['before']:
                    before_count = debug_info['before'][col]['missing_count']
                    after_count = debug_info['after'][col]['missing_count']
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
    
    # 임시 컬럼 제거
    df = df.drop(columns=[col for col in df.columns if col.startswith('_')], errors='ignore')
    
    if debug:
        return df, debug_info
    else:
        return df

def main():
    """
    메인 실행 함수
    """
    # CSV 파일 경로 입력
    csv_path = input("CSV 파일 경로를 입력하세요: ").strip()
    
    if not Path(csv_path).exists():
        print(f"오류: 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    # CSV 읽기
    print(f"CSV 파일 읽는 중: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    print(f"원본 데이터 shape: {df.shape}")
    print(f"ta 결측치: {df['ta'].isna().sum()}")
    print(f"hm 결측치: {df['hm'].isna().sum()}")
    
    # 결측치 처리
    print("\n결측치 처리 중...")
    df_imputed, debug_info = impute_missing_values(df, columns=['ta', 'hm'], debug=True)
    
    print(f"\n처리 후 데이터 shape: {df_imputed.shape}")
    print(f"ta 결측치: {df_imputed['ta'].isna().sum()}")
    print(f"hm 결측치: {df_imputed['hm'].isna().sum()}")
    
    # 결과 저장
    output_path = csv_path.replace('.csv', '_imputed.csv')
    df_imputed.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n결과 저장 완료: {output_path}")

if __name__ == "__main__":
    main()