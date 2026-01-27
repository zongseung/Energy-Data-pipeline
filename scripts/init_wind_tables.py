"""
풍력 테이블 초기화 및 CSV 데이터 초기 적재 스크립트

실행:
    uv run python scripts/init_wind_tables.py

동작:
1. wind_namdong, wind_seobu, wind_hangyoung 테이블 생성
2. nandong_wind_power.csv -> wind_namdong
3. seobu_wind.csv -> wind_seobu
4. Hangyoung_wind_power.csv -> wind_hangyoung
"""

import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from fetch_data.wind.database import init_db, get_engine, Base
from fetch_data.wind.namdong_wind_collect import backfill_from_csv as load_namdong
from fetch_data.wind.seobu_wind_load import load_seobu_to_db
from fetch_data.wind.hangyoung_wind_load import load_hangyoung_to_db


def main():
    print("=" * 60)
    print("풍력 테이블 초기화 시작")
    print("=" * 60)

    # 1. 테이블 생성
    print("\n[1/4] 테이블 생성...")
    init_db()

    # 2. 남동발전 풍력 CSV 적재
    print("\n[2/4] 남동발전 풍력 CSV 적재...")
    try:
        namdong_count = load_namdong()
        print(f"  -> 남동발전 풍력: {namdong_count}행 적재")
    except Exception as e:
        print(f"  -> 남동발전 풍력 적재 실패: {e}")
        namdong_count = 0

    # 3. 서부발전 풍력 CSV 적재
    print("\n[3/4] 서부발전 풍력 CSV 적재...")
    try:
        seobu_count = load_seobu_to_db()
        print(f"  -> 서부발전 풍력: {seobu_count}행 적재")
    except Exception as e:
        print(f"  -> 서부발전 풍력 적재 실패: {e}")
        seobu_count = 0

    # 4. 한경풍력 CSV 적재
    print("\n[4/4] 한경풍력 CSV 적재...")
    try:
        hangyoung_count = load_hangyoung_to_db()
        print(f"  -> 한경풍력: {hangyoung_count}행 적재")
    except Exception as e:
        print(f"  -> 한경풍력 적재 실패: {e}")
        hangyoung_count = 0

    # 결과 요약
    print("\n" + "=" * 60)
    print("풍력 테이블 초기화 완료")
    print("=" * 60)
    print(f"  wind_namdong:    {namdong_count:>10,}행")
    print(f"  wind_seobu:      {seobu_count:>10,}행")
    print(f"  wind_hangyoung:  {hangyoung_count:>10,}행")
    print(f"  합계:            {namdong_count + seobu_count + hangyoung_count:>10,}행")
    print()


if __name__ == "__main__":
    main()
