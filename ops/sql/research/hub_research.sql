-- =============================================================================
-- energy-hub-db 에 실행: 연구용으로 내보낼 뷰 + FDW 전용 읽기 계정
--
-- pv-db 에는 PostGIS 가 없어 geometry 컬럼을 FDW 로 직접 읽을 수 없다.
-- 그래서 hub 쪽에서 좌표(lon/lat)·길이(length_km)로 평탄화한 뷰를 만들고,
-- pv-db(hub_fdw.sql)는 이 뷰만 IMPORT 한다. 평탄한 원본은 그대로 내보낸다.
--
-- 시각 컬럼 이름은 research 스키마 관례에 맞춰 timestamp 로 통일한다
-- (원본은 ts). demand_fdw 의 jeju_supply_demand 와 같은 처리다.
--
-- 적용: docker exec -i energy-hub-db psql -U energy_user -d energy_hub \
--         < sql/research/hub_research.sql
-- =============================================================================

-- ── 송배전망 ────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW research.substations AS
SELECT id, name, name_en, voltage, sub_type, operator, sido,
       ST_X(geom) AS lon, ST_Y(geom) AS lat
FROM public.substation;

CREATE OR REPLACE VIEW research.power_lines AS
SELECT id, name, power_type, voltage, sido,
       round((ST_Length(geom::geography) / 1000.0)::numeric, 3) AS length_km
FROM public.power_line;

-- ── 제주 ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW research.jeju_demand_hourly AS
SELECT ts AS "timestamp", demand_mw, source
FROM public.jeju_demand_hourly;

CREATE OR REPLACE VIEW research.jeju_generation_mix AS
SELECT ts AS "timestamp", fuel_type, gen_mwh
FROM public.jeju_generation_mix;

-- ── 방재기상관측(AWS) 일자료 ────────────────────────────────────────────────
-- 일 단위라 timestamp 가 아니라 date 로 내보낸다. 원본 obs_ts 는 00:00 고정이라
-- 시간 정보가 없다 — 시간별이 필요하면 research.weather_asos 를 써야 한다.
CREATE OR REPLACE VIEW research.aws_obs_daily AS
SELECT obs_date AS "date", stn AS station_id, name AS station_name, type AS station_type,
       ta, wd, ws, rn_day, rn_hr1, hm, pa, ps, imputed
FROM pf.aws_obs_daily;

-- FDW 전용 읽기 계정 — 실제 소유 계정(energy_user)의 비밀번호를 pv-db 쪽
-- SQL 파일에 박지 않기 위한 것. 폐쇄 도커망 전용 개발 자격증명.
DO $$ BEGIN
    CREATE ROLE pv_fdw LOGIN PASSWORD 'pv_fdw';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

GRANT USAGE ON SCHEMA research TO pv_fdw;
GRANT SELECT ON research.substations, research.power_lines, research.kepco_grid,
               research.jeju_demand_hourly, research.jeju_generation_mix,
               research.aws_obs_daily TO pv_fdw;
