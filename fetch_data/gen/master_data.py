"""남동발전(KOEN) 비태양광 발전소 마스터데이터(위치·용량) CSV 빌더.

원본 수집 CSV(gen_data_raw/)를 스캔해 gen_data/ 아래
namdong_gen_plant_locations.csv / namdong_gen_plant_capacities.csv 를 재생성한다.
수집 본체(namdong_collect.run)가 매 수집 끝에 호출한다.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from fetch_data.common.koen import read_csv_flexible
from fetch_data.common.logger import get_logger
from fetch_data.constants import NamdongGenAPI
from fetch_data.gen.capacities import resolve_capacity
from fetch_data.gen.locations import resolve_location

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = PROJECT_ROOT / "gen_data_raw"
DEFAULT_LOC_PATH = PROJECT_ROOT / "gen_data" / "namdong_gen_plant_locations.csv"
DEFAULT_CAP_PATH = PROJECT_ROOT / "gen_data" / "namdong_gen_plant_capacities.csv"


# -------------------------
# 발전소 위치 CSV 생성
# -------------------------
def build_plant_locations(
    results: Dict[str, List[Path]],
    out_path: Path,
) -> pd.DataFrame:
    """수집 CSV들의 '발전구분'을 위경도/주소에 매핑해 위치 CSV로 저장.

    Returns:
        DataFrame[gen_type, plant, lat, lon, address, site, matched]
    """
    rows: List[dict] = []
    seen: set = set()

    for gen_key, files in results.items():
        label = NamdongGenAPI.GEN_TYPES[gen_key]["label"]
        for fp in files:
            try:
                df = read_csv_flexible(fp)
            except Exception as e:
                logger.warning(f"[위치] 읽기 실패 {fp.name}: {e}")
                continue
            if "발전구분" not in df.columns:
                continue
            for plant in df["발전구분"].astype(str).str.strip().unique():
                key = (gen_key, plant)
                if not plant or key in seen:
                    continue
                seen.add(key)
                loc = resolve_location(plant)
                rows.append(
                    {
                        "gen_type": label,
                        "plant": plant,
                        "lat": loc["lat"] if loc else None,
                        "lon": loc["lon"] if loc else None,
                        "address": loc["address"] if loc else None,
                        "site": loc["site"] if loc else None,
                        "matched": bool(loc),
                    }
                )

    loc_df = pd.DataFrame(
        rows, columns=["gen_type", "plant", "lat", "lon", "address", "site", "matched"]
    ).sort_values(["gen_type", "plant"]).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    loc_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    unmatched = loc_df.loc[~loc_df["matched"], "plant"].tolist()
    logger.info(f"[위치] 발전소 {len(loc_df)}개 -> {out_path}")
    if unmatched:
        logger.warning(f"[위치] 미매핑(좌표 없음) {len(unmatched)}개: {unmatched}")
    return loc_df


def rebuild_locations_from_raw(
    raw_dir: Path = DEFAULT_RAW_DIR,
    loc_path: Path = DEFAULT_LOC_PATH,
    gen_keys: Optional[List[str]] = None,
) -> pd.DataFrame:
    """디스크에 저장된 '모든' 원본 CSV에서 위치 CSV를 재생성한다.

    build_plant_locations 는 단일 수집 run 의 메모리 결과(results)만 반영하므로,
    일부 발전원만 수집한 run 이 위치 CSV를 덮어쓰면 나머지 발전원이 사라진다.
    이 함수는 raw_dir 의 발전원별 원본을 전부 스캔해 항상 완전한 위치 CSV를 만든다.
    """
    gen_keys = gen_keys or list(NamdongGenAPI.GEN_TYPES.keys())
    results: Dict[str, List[Path]] = {}
    for key in gen_keys:
        cat_dir = Path(raw_dir) / key
        files = sorted(cat_dir.glob("koen_*.csv")) if cat_dir.exists() else []
        results[key] = files
        logger.info(f"[위치재생성] {key}: 원본 {len(files)}개")
    return build_plant_locations(results, loc_path)


# -------------------------
# 호기별 위치 + 정격용량 CSV 생성
# -------------------------
def build_plant_capacities(
    raw_dir: Path = DEFAULT_RAW_DIR,
    out_path: Path = DEFAULT_CAP_PATH,
    gen_keys: Optional[List[str]] = None,
) -> pd.DataFrame:
    """디스크의 원본 CSV에서 (발전구분, 호기) 조합을 모아, 호기 단위로
    위치(locations) + 정격용량(capacities)을 결합한 CSV를 생성한다.

    plant_name(= 발전구분_호기)은 transform_gen 의 규칙과 동일하므로
    시간별 long CSV(gen_data/{category}_long.csv)와 plant_name 으로 바로 조인된다.

    Returns:
        DataFrame[gen_type, plant_name, plant, hogi, lat, lon, address, site,
                  capacity_mw, capacity_confidence, capacity_source]
    """
    gen_keys = gen_keys or list(NamdongGenAPI.GEN_TYPES.keys())
    rows: List[dict] = []
    seen: set = set()

    for gen_key in gen_keys:
        label = NamdongGenAPI.GEN_TYPES[gen_key]["label"]
        cat_dir = Path(raw_dir) / gen_key
        files = sorted(cat_dir.glob("koen_*.csv")) if cat_dir.exists() else []
        for fp in files:
            try:
                df = read_csv_flexible(fp)
            except Exception as e:
                logger.warning(f"[용량] 읽기 실패 {fp.name}: {e}")
                continue
            if "발전구분" not in df.columns or "호기" not in df.columns:
                continue
            combos = (
                df[["발전구분", "호기"]]
                .astype(str)
                .apply(lambda s: s.str.strip())
                .drop_duplicates()
            )
            for plant, hogi in combos.itertuples(index=False):
                plant_name = f"{plant}_{hogi}"
                key = (gen_key, plant_name)
                if not plant or not hogi or key in seen:
                    continue
                seen.add(key)
                loc = resolve_location(plant)
                cap = resolve_capacity(plant_name)
                rows.append(
                    {
                        "gen_type": label,
                        "plant_name": plant_name,
                        "plant": plant,
                        "hogi": hogi,
                        "lat": loc["lat"] if loc else None,
                        "lon": loc["lon"] if loc else None,
                        "address": loc["address"] if loc else None,
                        "site": loc["site"] if loc else None,
                        "capacity_mw": cap["capacity_mw"] if cap else None,
                        "capacity_confidence": cap["confidence"] if cap else "미확인",
                        "capacity_source": cap["source"] if cap else None,
                    }
                )

    cap_df = pd.DataFrame(
        rows,
        columns=[
            "gen_type", "plant_name", "plant", "hogi",
            "lat", "lon", "address", "site",
            "capacity_mw", "capacity_confidence", "capacity_source",
        ],
    ).sort_values(["gen_type", "plant_name"]).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    cap_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    no_cap = cap_df.loc[cap_df["capacity_mw"].isna(), "plant_name"].tolist()
    logger.info(f"[용량] 호기 {len(cap_df)}개 -> {out_path}")
    if no_cap:
        logger.warning(f"[용량] 용량 미확정 {len(no_cap)}개: {no_cap}")
    return cap_df
