import glob
import os
import re
import csv
from datetime import datetime
from workalendar.asia import SouthKorea

PATTERN = re.compile(r"Demand_Data_(\d{8})_(\d{8})\.csv$")

def list_files_sorted(pattern="Demand_Data_*.csv"):
    files = glob.glob(pattern)
    keyed = []
    for f in files:
        base = os.path.basename(f)
        m = PATTERN.search(base)
        if m:
            start, end = m.group(1), m.group(2)
            keyed.append((start, end, f))
    keyed.sort(key=lambda x: (x[0], x[1]))
    return [f for _, _, f in keyed]

def is_holiday_cached(cal: SouthKorea, cache: dict, date_str_yyyy_mm_dd: str) -> int:
    if date_str_yyyy_mm_dd in cache:
        return cache[date_str_yyyy_mm_dd]
    d = datetime.strptime(date_str_yyyy_mm_dd, "%Y-%m-%d").date()
    val = 1 if cal.is_holiday(d) else 0
    cache[date_str_yyyy_mm_dd] = val
    return val

def read_last_datetime(out_file, encoding, datetime_col="기준일시"):
    """
    out_file의 마지막 데이터 행에서 기준일시를 읽어 반환.
    없으면 None.
    """
    if not os.path.exists(out_file) or os.path.getsize(out_file) == 0:
        return None

    last_line = None
    with open(out_file, "r", encoding=encoding, errors="ignore") as f:
        for line in f:
            if line.strip():
                last_line = line

    if not last_line:
        return None

    # 헤더만 있는 경우 방지
    # 마지막 줄이 헤더일 가능성이 낮지만, 안전하게 처리
    # CSV 파싱으로 마지막 줄을 해석
    row = next(csv.reader([last_line]))
    # out_file에는 공휴일 컬럼이 추가되어 있으므로, 기준일시는 앞쪽 동일 위치에 있음
    # 헤더 위치를 정확히 알기 위해 헤더를 읽어 index 확보
    with open(out_file, "r", encoding=encoding, errors="ignore", newline="") as f2:
        reader = csv.reader(f2)
        header = next(reader, None)
        if not header or datetime_col not in header:
            return None
        dt_idx = header.index(datetime_col)

    dt_val = row[dt_idx].strip()
    # 'YYYY-MM-DD HH:MM:SS' 가정
    return datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S")

def concat_csv_backfill_with_holiday(
    files,
    out_file="Demand_Data_all.csv",
    encoding="euc-kr",
    datetime_col="기준일시",
    holiday_col="공휴일",
):
    if not files:
        raise FileNotFoundError("대상 CSV 파일이 없습니다. (Demand_Data_*.csv)")

    cal = SouthKorea()
    holiday_cache = {}

    # 기존 파일의 마지막 기준일시를 읽어서 이후 것만 백필
    last_dt = read_last_datetime(out_file, encoding, datetime_col=datetime_col)
    if last_dt:
        print(f"[INFO] 기존 병합 파일 마지막 기준일시: {last_dt}")
    else:
        print("[INFO] 기존 병합 파일이 없거나 비어있음: 새로 생성합니다.")

    # out_file이 존재하면 append 모드, 없으면 write 모드
    file_exists = os.path.exists(out_file) and os.path.getsize(out_file) > 0
    mode = "a" if file_exists else "w"

    wrote_header = file_exists
    total = len(files)

    with open(out_file, mode, encoding=encoding, newline="") as out_f:
        writer = csv.writer(out_f)

        dt_idx_in = None

        for idx, f in enumerate(files, start=1):
            print(f"[{idx}/{total}] backfill+holiday: {f}")

            with open(f, "r", encoding=encoding, errors="ignore", newline="") as in_f:
                reader = csv.reader(in_f)
                header = next(reader, None)
                if not header:
                    continue

                if datetime_col not in header:
                    raise ValueError(f"'{datetime_col}' 컬럼이 없습니다: {f}")

                dt_idx_in = header.index(datetime_col)

                # 최초 생성일 때만 헤더 쓰기(공휴일 컬럼 추가)
                if not wrote_header:
                    writer.writerow(header + [holiday_col])
                    wrote_header = True

                # 데이터 행 처리
                for row in reader:
                    if not row:
                        continue

                    dt_val = row[dt_idx_in].strip()
                    if len(dt_val) < 19:
                        continue  # 비정상 행 스킵

                    row_dt = datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S")

                    # 백필 핵심: 마지막 dt 이후만 append
                    if last_dt and row_dt <= last_dt:
                        continue

                    date_part = dt_val[:10]  # 'YYYY-MM-DD'
                    holiday = is_holiday_cached(cal, holiday_cache, date_part)

                    writer.writerow(row + [holiday])

                    # last_dt 갱신(append 중인 최신값으로 유지)
                    last_dt = row_dt

    print(f"[DONE] backfilled(+{holiday_col}) → {out_file}")



if __name__ == "__main__":
    files = list_files_sorted("Demand_Data_*.csv")
    print(f"총 {len(files)}개 원본 파일 검사")
    concat_csv_backfill_with_holiday(
        files,
        out_file="Demand_Data_all.csv",
        encoding="euc-kr",
        datetime_col="기준일시",
        holiday_col="공휴일",
    )
