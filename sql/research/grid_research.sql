-- =============================================================================
-- energy-hub-db 에 실행: 송배전망 데모/연구용 평탄화 뷰 + FDW 전용 읽기 계정
--
-- pv-db 에는 PostGIS 가 없어 geometry 컬럼을 FDW 로 직접 읽을 수 없다.
-- 그래서 hub 쪽에서 좌표(lon/lat)·길이(length_km)로 평탄화한 뷰를 만들고,
-- pv-db(grid_fdw.sql)는 이 뷰만 IMPORT 한다. kepco_grid 는 원래 평탄해서 그대로.
--
-- 적용: docker exec -i energy-hub-db psql -U energy_user -d energy_hub \
--         < sql/research/grid_research.sql
-- =============================================================================

CREATE OR REPLACE VIEW research.substations AS
SELECT id, name, name_en, voltage, sub_type, operator, sido,
       ST_X(geom) AS lon, ST_Y(geom) AS lat
FROM public.substation;

CREATE OR REPLACE VIEW research.power_lines AS
SELECT id, name, power_type, voltage, sido,
       round((ST_Length(geom::geography) / 1000.0)::numeric, 3) AS length_km
FROM public.power_line;

-- FDW 전용 읽기 계정 — 실제 소유 계정(energy_user)의 비밀번호를 pv-db 쪽
-- SQL 파일에 박지 않기 위한 것. 폐쇄 도커망 전용 개발 자격증명.
DO $$ BEGIN
    CREATE ROLE pv_fdw LOGIN PASSWORD 'pv_fdw';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

GRANT USAGE ON SCHEMA research TO pv_fdw;
GRANT SELECT ON research.substations, research.power_lines,
               research.kepco_grid TO pv_fdw;
