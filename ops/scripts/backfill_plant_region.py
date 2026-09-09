"""발전소 좌표 → 시도·시군구 역지오코딩 (1회성).

좌표가 본부·부지 단위라 89기가 29개 지점으로 뭉친다. 지점 단위로만 조회하면
호출이 89번에서 29번으로 줄고, 같은 부지의 호기들이 같은 값을 갖게 된다.

키가 필요 없는 Nominatim 을 쓴다. 사용 정책상 초당 1회를 넘기지 않는다.

    uv run python scripts/backfill_plant_region.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import create_engine, text  # noqa: E402

from pipeline.fetch_data.common.coordinates import admin_region  # noqa: E402
from pipeline.fetch_data.common.db_utils import resolve_db_url  # noqa: E402

UA = "energy-data-pipeline/1.0 (research; contact via repo admin)"
SLEEP_S = 1.2  # Nominatim 사용 정책: 초당 1회 이하


def reverse(lat: float, lon: float) -> dict:
    q = urllib.parse.urlencode(
        {"format": "jsonv2", "lat": lat, "lon": lon, "accept-language": "ko"}
    )
    req = urllib.request.Request(
        f"https://nominatim.openstreetmap.org/reverse?{q}", headers={"User-Agent": UA}
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r).get("address", {})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="조회만 하고 쓰지 않는다")
    args = ap.parse_args()

    engine = create_engine(resolve_db_url())
    with engine.begin() as conn:
        points = conn.execute(text(
            "SELECT DISTINCT lat, lon FROM plants WHERE lat IS NOT NULL AND sido IS NULL"
        )).fetchall()
        print(f"조회할 지점 {len(points)}개 (예상 {len(points) * SLEEP_S:.0f}초)")

        filled = 0
        for i, (lat, lon) in enumerate(points, 1):
            try:
                sido, sigungu = admin_region(reverse(lat, lon))
            except Exception as e:  # 한 지점 실패가 전체를 멈추지 않게
                print(f"  [{i}/{len(points)}] {lat},{lon} 실패: {e}")
                time.sleep(SLEEP_S)
                continue
            print(f"  [{i}/{len(points)}] {lat:.4f},{lon:.4f} → {sido} {sigungu or ''}")
            if sido and not args.dry_run:
                filled += conn.execute(text(
                    "UPDATE plants SET sido = :sido, sigungu = :sigungu "
                    "WHERE lat = :lat AND lon = :lon"
                ), {"sido": sido, "sigungu": sigungu, "lat": lat, "lon": lon}).rowcount
            time.sleep(SLEEP_S)

        print(f"\n{'(dry-run) ' if args.dry_run else ''}갱신된 발전소 {filled}기")
    return 0


if __name__ == "__main__":
    sys.exit(main())
