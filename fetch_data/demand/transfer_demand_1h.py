import pandas as pd
import glob

def make_hourly_demand(input_pattern="Demand_Data_*.csv", output_file="Demand_Data_hourly_demand.csv"):
    files = sorted(glob.glob(input_pattern))
    if not files:
        print("CSV 파일이 없습니다.")
        return

    df_list = []

    for f in files:
        print(f"[로드] {f}")
        df = pd.read_csv(f, encoding="euc-kr")

        # 기준일시 → datetime
        df["기준일시"] = pd.to_datetime(df["기준일시"], format="%Y-%m-%d %H:%M:%S")

        df_list.append(df)

    # 전체 CSV 합치기
    df_all = pd.concat(df_list, ignore_index=True)

    # 기준일시 정렬
    df_all = df_all.sort_values("기준일시")

    # datetime index 설정
    df_all = df_all.set_index("기준일시")

    # "현재수요(MW)" 열만 선택
    if "현재수요(MW)" not in df_all.columns:
        raise ValueError("CSV에 '현재수요(MW)' 열이 없습니다. 실제 컬럼명을 확인하세요.")

    demand_series = df_all["현재수요(MW)"]

    # 1시간 평균
    hourly = demand_series.resample("1H").mean()

    # 저장
    hourly.to_csv(output_file, encoding="euc-kr")

    print(f"[완료] 1시간 평균 현재수요(MW) 생성 → {output_file}")
