"""
DB 스키마 리팩터링 마이그레이션 (docs/schema-refactor-plan.md P1~P3).

대상 DB: LOCAL_DB_URL (pv-data-postgres :5436 — 실제 메인 데이터 DB)

원칙:
  - 삭제·변경 없음: CREATE IF NOT EXISTS + INSERT ON CONFLICT DO NOTHING 만 사용
  - 멱등: 재실행 안전
  - 기존 테이블(nambu_generation 등)은 그대로 둔다 (P6 DROP은 별도 결정 후)

Phase:
  p1  : plants / generation 테이블 + 뷰 생성
  p2  : plants 마스터 적재 (5개 시계열 테이블 + 코드 dict + KOEN CSV 통합)
  p3  : generation 백필 (DB 5테이블 + gen_data CSV 3종)
  verify: 행수 대사 리포트

실행:
    uv run python scripts/migrations/schema_migration.py            # 전체 (p1→p2→p3→verify)
    uv run python scripts/migrations/schema_migration.py --phase p1
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

import os  # noqa: E402

DB_URL = os.environ["LOCAL_DB_URL"]
GEN_DATA_DIR = PROJECT_ROOT / "gen_data"

engine = create_engine(DB_URL, echo=False)


# ═══════════════════════════════════════════════════════════
# P1. DDL
# ═══════════════════════════════════════════════════════════

DDL = """
CREATE TABLE IF NOT EXISTS plants (
    plant_id      serial PRIMARY KEY,
    plant_code    varchar(50),
    plant_name    varchar(200) NOT NULL,
    unit_no       varchar(10)  NOT NULL DEFAULT '1',
    fuel_type     varchar(20)  NOT NULL,
    operator      varchar(50)  NOT NULL,
    region        varchar(20)  DEFAULT 'mainland',
    capacity_mw   double precision,
    capacity_confidence varchar(10) DEFAULT '확실',
    lat double precision,
    lon double precision,
    address       text,
    site_name     varchar(100),
    install_angle double precision,
    module_spec   text,
    inverter_spec text,
    created_at    timestamp DEFAULT now(),
    CONSTRAINT uq_plants_name_unit UNIQUE (plant_name, unit_no)
);
CREATE INDEX IF NOT EXISTS ix_plants_fuel ON plants (fuel_type);
CREATE INDEX IF NOT EXISTS ix_plants_operator ON plants (operator);
COMMENT ON TABLE plants IS '발전소 마스터. 좌표/용량/스펙의 단일 진실 원천';
COMMENT ON COLUMN plants.fuel_type IS 'solar|wind|hydro|thermal|fuel_cell';
COMMENT ON COLUMN plants.capacity_confidence IS '확실|근사|불확실 (KOEN 공개자료 조사 신뢰도)';

CREATE TABLE IF NOT EXISTS generation (
    timestamp  timestamp NOT NULL,
    plant_id   int NOT NULL REFERENCES plants(plant_id),
    gen_kwh    double precision,
    source     varchar(30) NOT NULL DEFAULT 'api',
    PRIMARY KEY (timestamp, plant_id)
);
CREATE INDEX IF NOT EXISTS ix_generation_plant_ts ON generation (plant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_generation_ts_brin ON generation USING brin (timestamp);
COMMENT ON TABLE generation IS '시간별 발전량 통합 (PV/풍력/수력/화력/연료전지). 단위 kWh';
COMMENT ON COLUMN generation.timestamp IS '구간 시각 KST. 원천 테이블의 시간 규약을 변환 없이 보존 (남부=구간시작, KOEN계열=hour-ending→N시 표기)';
COMMENT ON COLUMN generation.source IS 'api|backfill|manual';

CREATE OR REPLACE VIEW v_generation_hourly AS
SELECT g.timestamp, p.plant_name, p.unit_no, p.fuel_type, p.operator,
       p.region, p.lat, p.lon, g.gen_kwh
FROM generation g JOIN plants p USING (plant_id);
COMMENT ON VIEW v_generation_hourly IS 'energy_hub FDW 노출용 안정 인터페이스';
"""


def p1():
    print("=" * 60)
    print("P1. DDL 생성 (plants / generation / v_generation_hourly)")
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("  완료 (IF NOT EXISTS — 기존 객체 무변경)")


# ═══════════════════════════════════════════════════════════
# P2. plants 마스터 적재
# ═══════════════════════════════════════════════════════════

_CAP_RE = re.compile(r"^\s*([\d,]+(?:\.\d+)?)\s*kW", re.IGNORECASE)
_ANGLE_RE = re.compile(r"설치각\s*:\s*(\d+(?:\.\d+)?)")


def _parse_capacity_kw(cap_text):
    if not cap_text:
        return None
    m = _CAP_RE.match(str(cap_text))
    if m:
        return float(m.group(1).replace(",", ""))
    return None


def _parse_angle(angle_text):
    if not angle_text:
        return None
    m = _ANGLE_RE.search(str(angle_text))
    return float(m.group(1)) if m else None


UPSERT_PLANT = text("""
    INSERT INTO plants (plant_code, plant_name, unit_no, fuel_type, operator, region,
                        capacity_mw, capacity_confidence, lat, lon, address, site_name,
                        install_angle, module_spec, inverter_spec)
    VALUES (:code, :name, :unit, :fuel, :op, :region,
            :cap_mw, :conf, :lat, :lon, :addr, :site,
            :angle, :module, :inverter)
    ON CONFLICT (plant_name, unit_no) DO NOTHING
""")


def _row(name, unit="1", fuel="solar", op="nambu", code=None, region="mainland",
         cap_mw=None, conf="확실", lat=None, lon=None, addr=None, site=None,
         angle=None, module=None, inverter=None):
    return dict(code=code, name=name, unit=str(unit), fuel=fuel, op=op, region=region,
                cap_mw=cap_mw, conf=conf, lat=lat, lon=lon, addr=addr, site=site,
                angle=angle, module=module, inverter=inverter)


def p2():
    print("=" * 60)
    print("P2. plants 마스터 적재")
    rows = []
    report = {}

    with engine.connect() as conn:
        # ── 남부발전: (gencd, hogi) 단위 + nambu_plants 메타 조인 ──
        nambu = conn.execute(text("""
            SELECT DISTINCT ON (g.gencd, g.hogi)
                   g.gencd, g.hogi, g.plant_name,
                   p.address, p.capacity, p.install_angle, p.latitude, p.longitude
            FROM nambu_generation g
            LEFT JOIN nambu_plants p ON p.plant_name = g.plant_name
            ORDER BY g.gencd, g.hogi, g.datetime DESC
        """)).fetchall()
        matched = 0
        for r in nambu:
            cap_kw = _parse_capacity_kw(r.capacity)
            if r.latitude is not None:
                matched += 1
            rows.append(_row(
                name=r.plant_name, unit=r.hogi, fuel="solar", op="nambu", code=r.gencd,
                cap_mw=cap_kw / 1000.0 if cap_kw else None,
                conf="확실" if r.latitude is not None else "불확실",
                lat=r.latitude, lon=r.longitude, addr=r.address,
                angle=_parse_angle(r.install_angle),
            ))
        report["nambu(units)"] = f"{len(nambu)} (메타 매칭 {matched})"

        # ── 남동 태양광: namdong_plants 조인 + dict 폴백 ──
        from fetch_data.pv.database import NAMDONG_PLANT_LOCATIONS, get_namdong_location

        namdong = conn.execute(text("""
            SELECT DISTINCT g.plant_name, p.latitude, p.longitude
            FROM namdong_generation g
            LEFT JOIN namdong_plants p ON p.plant_name = g.plant_name
        """)).fetchall()
        for r in namdong:
            lat, lon = r.latitude, r.longitude
            conf = "확실"
            if lat is None:
                loc = get_namdong_location(r.plant_name)
                lat, lon = loc["lat"], loc["lon"]
                conf = "근사"
            rows.append(_row(name=r.plant_name, fuel="solar", op="namdong",
                             lat=lat, lon=lon, conf=conf))
        report["namdong_solar"] = len(namdong)

        # ── 풍력 3사 ──
        from fetch_data.gen.locations import resolve_location

        wn = conn.execute(text("SELECT DISTINCT plant_name FROM wind_namdong")).fetchall()
        for r in wn:
            loc = resolve_location(r.plant_name)
            rows.append(_row(name=r.plant_name, fuel="wind", op="namdong",
                             lat=loc["lat"] if loc else None,
                             lon=loc["lon"] if loc else None,
                             addr=loc["address"] if loc else None,
                             site=loc["site"] if loc else None,
                             conf="근사" if loc else "불확실"))
        report["wind_namdong"] = len(wn)

        ws = conn.execute(text("""
            SELECT plant_name, max(capacity_mw) AS cap
            FROM wind_seobu GROUP BY plant_name
        """)).fetchall()
        for r in ws:
            rows.append(_row(name=r.plant_name, fuel="wind", op="seobu",
                             cap_mw=r.cap, conf="확실" if r.cap else "불확실"))
        report["wind_seobu"] = len(ws)

        wh = conn.execute(text("SELECT DISTINCT plant_name FROM wind_hangyoung")).fetchall()
        for r in wh:
            rows.append(_row(name=r.plant_name, fuel="wind", op="hangyoung",
                             region="jeju", lat=33.350, lon=126.180,
                             addr="제주특별자치도 제주시 한경면", conf="근사"))
        report["wind_hangyoung"] = len(wh)

        # ── KOEN 비태양광 (CSV) + 용량/좌표 dict ──
        from fetch_data.gen.capacities import KOEN_PLANT_CAPACITIES

        fuel_map = {"ocean_hydro": "hydro", "thermal": "thermal", "fuel_cell": "fuel_cell"}
        for cat, fuel in fuel_map.items():
            csv_path = GEN_DATA_DIR / f"{cat}_long.csv"
            if not csv_path.exists():
                report[f"koen_{cat}"] = "CSV 없음 — 건너뜀"
                continue
            names = pd.read_csv(csv_path, usecols=["plant_name"])["plant_name"].unique()
            for name in names:
                cap_info = KOEN_PLANT_CAPACITIES.get(name)
                loc = resolve_location(name)
                rows.append(_row(
                    name=name, fuel=fuel, op="namdong",  # KOEN=한국남동발전 → namdong 통일
                    cap_mw=cap_info["capacity_mw"] if cap_info else None,
                    conf=cap_info["confidence"] if cap_info else "불확실",
                    lat=loc["lat"] if loc else None,
                    lon=loc["lon"] if loc else None,
                    addr=loc["address"] if loc else None,
                    site=loc["site"] if loc else None,
                ))
            report[f"koen_{cat}"] = len(names)

    inserted = 0
    with engine.begin() as conn:
        before = conn.execute(text("SELECT count(*) FROM plants")).scalar()
        for r in rows:
            conn.execute(UPSERT_PLANT, r)
        after = conn.execute(text("SELECT count(*) FROM plants")).scalar()
        inserted = after - before

    print(f"  수집 소스별: {report}")
    print(f"  plants 적재: 시도 {len(rows)}건 → 신규 {inserted}건 (총 {after}행)")


# ═══════════════════════════════════════════════════════════
# P3. generation 백필
# ═══════════════════════════════════════════════════════════

BACKFILLS = [
    ("nambu_generation", """
        INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
        SELECT g.datetime, p.plant_id, g.generation, 'backfill'
        FROM nambu_generation g
        JOIN plants p ON p.plant_code = g.gencd
                     AND p.unit_no = g.hogi::varchar
                     AND p.operator = 'nambu'
        ON CONFLICT (timestamp, plant_id) DO NOTHING
    """),
    ("namdong_generation", """
        INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
        SELECT g.datetime, p.plant_id, g.generation, 'backfill'
        FROM namdong_generation g
        JOIN plants p ON p.plant_name = g.plant_name
                     AND p.operator = 'namdong' AND p.fuel_type = 'solar'
        ON CONFLICT (timestamp, plant_id) DO NOTHING
    """),
    ("wind_namdong", """
        INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
        SELECT g.timestamp, p.plant_id, g.generation, 'backfill'
        FROM wind_namdong g
        JOIN plants p ON p.plant_name = g.plant_name
                     AND p.operator = 'namdong' AND p.fuel_type = 'wind'
        ON CONFLICT (timestamp, plant_id) DO NOTHING
    """),
    ("wind_seobu", """
        INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
        SELECT g.timestamp, p.plant_id, g.generation, 'backfill'
        FROM wind_seobu g
        JOIN plants p ON p.plant_name = g.plant_name
                     AND p.operator = 'seobu' AND p.fuel_type = 'wind'
        ON CONFLICT (timestamp, plant_id) DO NOTHING
    """),
    ("wind_hangyoung", """
        INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
        SELECT g.timestamp, p.plant_id, g.generation, 'backfill'
        FROM wind_hangyoung g
        JOIN plants p ON p.plant_name = g.plant_name
                     AND p.operator = 'hangyoung'
        ON CONFLICT (timestamp, plant_id) DO NOTHING
    """),
]

CSV_BACKFILLS = {"ocean_hydro": "hydro", "thermal": "thermal", "fuel_cell": "fuel_cell"}


def p3():
    print("=" * 60)
    print("P3. generation 백필 (삽입만, ON CONFLICT 무시)")

    for name, sql in BACKFILLS:
        with engine.begin() as conn:
            src = conn.execute(text(f"SELECT count(*) FROM {name}")).scalar()
            before = conn.execute(text("SELECT count(*) FROM generation")).scalar()
            conn.execute(text(sql))
            after = conn.execute(text("SELECT count(*) FROM generation")).scalar()
        skipped = src - (after - before)
        print(f"  {name}: 원본 {src:,} → 신규 {after - before:,} (중복/기적재 {skipped:,})")

    # CSV 3종
    for cat, fuel in CSV_BACKFILLS.items():
        csv_path = GEN_DATA_DIR / f"{cat}_long.csv"
        if not csv_path.exists():
            print(f"  {cat}: CSV 없음 — 건너뜀")
            continue
        df = pd.read_csv(csv_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df[df["timestamp"].notna()]

        with engine.begin() as conn:
            id_map = dict(conn.execute(text(
                "SELECT plant_name, plant_id FROM plants WHERE operator='namdong' AND fuel_type=:f"
            ), {"f": fuel}).fetchall())
            df["plant_id"] = df["plant_name"].map(id_map)
            unmapped = df["plant_id"].isna().sum()
            df = df[df["plant_id"].notna()].copy()
            df["plant_id"] = df["plant_id"].astype(int)

            before = conn.execute(text("SELECT count(*) FROM generation")).scalar()
            payload = [
                {"ts": t, "pid": pid, "kwh": g}
                for t, pid, g in zip(df["timestamp"], df["plant_id"], df["generation"])
            ]
            ins = text("""
                INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
                VALUES (:ts, :pid, :kwh, 'backfill')
                ON CONFLICT (timestamp, plant_id) DO NOTHING
            """)
            for i in range(0, len(payload), 10000):
                conn.execute(ins, payload[i:i + 10000])
            after = conn.execute(text("SELECT count(*) FROM generation")).scalar()
        print(f"  csv_{cat}: 원본 {len(df):,} → 신규 {after - before:,} (미매핑 {unmapped})")


# ═══════════════════════════════════════════════════════════
# 검증
# ═══════════════════════════════════════════════════════════

def verify():
    print("=" * 60)
    print("검증 리포트")
    with engine.connect() as conn:
        plants_n = conn.execute(text("SELECT count(*) FROM plants")).scalar()
        gen_n = conn.execute(text("SELECT count(*) FROM generation")).scalar()
        print(f"  plants: {plants_n}행 / generation: {gen_n:,}행")

        print("\n  [fuel_type × operator 분포]")
        for r in conn.execute(text("""
            SELECT p.fuel_type, p.operator, count(*) AS plants,
                   coalesce(sum(c.cnt), 0) AS gen_rows
            FROM plants p
            LEFT JOIN (SELECT plant_id, count(*) AS cnt FROM generation GROUP BY plant_id) c
                   USING (plant_id)
            GROUP BY 1, 2 ORDER BY 1, 2
        """)):
            print(f"    {r.fuel_type:<10} {r.operator:<12} plants={r.plants:<4} rows={int(r.gen_rows):,}")

        print("\n  [원본 대사]")
        for tbl in ["nambu_generation", "namdong_generation",
                    "wind_namdong", "wind_seobu", "wind_hangyoung"]:
            n = conn.execute(text(f"SELECT count(*) FROM {tbl}")).scalar()
            print(f"    {tbl}: {n:,}")


# ═══════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["p1", "p2", "p3", "verify"], default=None)
    args = ap.parse_args()

    print(f"대상 DB: {DB_URL.split('@')[-1]}")
    if args.phase:
        {"p1": p1, "p2": p2, "p3": p3, "verify": verify}[args.phase]()
    else:
        p1(); p2(); p3(); verify()


if __name__ == "__main__":
    main()
