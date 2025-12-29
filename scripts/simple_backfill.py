#!/usr/bin/env python3
"""
간단한 CSV → DB 백필 스크립트

Prefect 없이 직접 실행하여 CSV 데이터를 DB에 로드합니다.
"""

import asyncio
import os
import sys
from pathlib import Path

# 프로젝트 루트 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from fetch_data.database import init_db, get_async_session, Demand5Min
from fetch_data.collect_demand import load_csv_to_db
from fetch_data.aggregate_hourly import aggregate_with_backfill


async def simple_backfill(
    csv_path: str = "/mnt/nvme/weather-pipeline/Demand_Data_all.csv",
    skip_aggregate: bool = False,
):
    """
    간단한 백필 실행
    
    1. CSV → DB 로드 (없는 데이터만)
    2. 1시간 집계 (선택)
    """
    print("=" * 60)
    print("간단 백필 시작")
    print("=" * 60)
    
    # 1. DB 초기화
    print("\n[1/3] DB 테이블 확인 중...")
    await init_db()
    
    # 2. CSV → DB 로드
    print("\n[2/3] CSV → DB 로드 중...")
    if os.path.exists(csv_path):
        count = await load_csv_to_db(csv_path)
        print(f"완료: {count:,}건 저장됨")
    else:
        print(f"CSV 파일 없음: {csv_path}")
        return
    
    # 3. 1시간 집계 (선택)
    if not skip_aggregate:
        print("\n[3/3] 1시간 데이터 집계 중...")
        hourly_count = await aggregate_with_backfill()
        print(f"완료: {hourly_count:,}건 통합됨")
    else:
        print("\n[3/3] 1시간 집계 건너뜀 (skip_aggregate=True)")
    
    print("\n" + "=" * 60)
    print("백필 완료!")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="간단한 CSV → DB 백필")
    parser.add_argument(
        "--csv",
        default="/mnt/nvme/weather-pipeline/Demand_Data_all.csv",
        help="CSV 파일 경로"
    )
    parser.add_argument(
        "--skip-aggregate",
        action="store_true",
        help="1시간 집계 건너뛰기"
    )
    
    args = parser.parse_args()
    
    asyncio.run(simple_backfill(
        csv_path=args.csv,
        skip_aggregate=args.skip_aggregate,
    ))

