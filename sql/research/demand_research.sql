-- =============================================================================
-- research 스키마 — 연구원 배포용 읽기전용 뷰 (demand DB)
-- =============================================================================
-- pv DB 의 sql/research/pv_research.sql 과 같은 구조다. 두 DB 는 별개 클러스터라
-- role 도 각각 만들어야 한다.
--
-- 실행법
--   뷰만 생성/갱신 : psql -U demand -d demand -f sql/research/demand_research.sql
--   role 발급      : psql -U demand -d demand -v role_name=hong -v password="$PW" \
--                         -f sql/research/demand_research.sql
--   role 회수      : psql -U demand -d demand -v drop_role=hong \
--                         -f sql/research/demand_research.sql
--
-- 시간 규약 — docs/time-convention-audit.md 는 generation 코어만 다뤘고 이 DB 는
-- 다루지 않았다. 아래는 이 파일 작성 시점에 코드/데이터로 확인한 범위이며,
-- **근거가 없는 항목은 시프트하지 않았다.** 근거 없는 시간 보정으로 이미 큰 문제를
-- 겪은 프로젝트다 — 확정 근거가 생기기 전까지 손대지 말 것.
--
--   demand_5min        : KPX sukub CSV 의 `기준일시` 를 그대로 적재
--                        (fetch_data/demand/collect.py:COLUMN_MAPPING). 구간 라벨이
--                        아니라 시각 표기로 보이나 순시값/5분평균 여부는 확인 못 함. 무보정.
--   jeju_supply_demand : 제주 실시간 CSV 의 timestamp 를 그대로 적재
--                        (fetch_data/jeju/load_jeju_demand_db.py:58). 무보정.
--   demand_weather_1h  : demand_avg 는 demand_5min 을 date_trunc('hour') 로 묶어
--                        평균한 값이므로 **구간시작이 확정적**이다
--                        (fetch_data/demand/aggregate.py:42-48). 기상 컬럼은 ASOS
--                        관측값을 floor('h') 한 것이라 ASOS 규약을 그대로 따른다.
--   heat_demand        : 이 저장소에 적재 코드가 없다(외부 유입). year/month/day/hour
--                        컬럼이 timestamp 와 100% 일치함(불일치 0행)만 확인했다.
--                        원천 라벨 의미는 불명 — 무보정.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA IF NOT EXISTS research;
COMMENT ON SCHEMA research IS
    '연구원 배포용 읽기전용 뷰. 운영 테이블은 public 스키마에 있으며 접근 권한이 없다.';

-- 컬럼 구성이 바뀌면 CREATE OR REPLACE VIEW 가 실패하므로 매번 새로 만든다.
DROP VIEW IF EXISTS
    research.demand_5min,
    research.jeju_supply_demand,
    research.heat_demand,
    research.heat_demand_location,
    research.demand_weather_1h
CASCADE;


-- -----------------------------------------------------------------------------
-- 전국 계통 수급 (5분)
-- -----------------------------------------------------------------------------
CREATE VIEW research.demand_5min AS
SELECT
    d.timestamp,
    d.current_demand,
    d.current_supply,
    d.supply_capacity,
    d.supply_reserve,
    d.reserve_rate,
    d.operation_reserve,
    d.is_holiday,
    d.day_type
FROM demand_5min d;

COMMENT ON VIEW research.demand_5min IS
    'KPX 전국 계통 수급 5분 데이터. 단위는 MW(예비율만 %).';
COMMENT ON COLUMN research.demand_5min."timestamp" IS
    'KPX 원천의 기준일시를 그대로 쓴다(KST). 구간 라벨인지 순시 시각인지 확정 근거가 없어 보정하지 않았다.';
COMMENT ON COLUMN research.demand_5min.day_type IS
    '요일/휴일 구분 코드. 원천 정의 문서를 확인하지 못했다.';


-- -----------------------------------------------------------------------------
-- 제주 수급 (5분)
--   원본 컬럼명은 ts 지만 research 스키마 전체에서 timestamp 로 통일한다.
-- -----------------------------------------------------------------------------
CREATE VIEW research.jeju_supply_demand AS
SELECT
    j.ts AS "timestamp",
    j.supply_mw,
    j.demand_mw,
    j.renewable_total_mw,
    j.solar_mw,
    j.wind_mw
FROM jeju_supply_demand j;

COMMENT ON VIEW research.jeju_supply_demand IS
    '제주 계통 수급 5분 데이터(MW). 원본 컬럼 ts 를 timestamp 로 이름만 바꿨다.';
COMMENT ON COLUMN research.jeju_supply_demand."timestamp" IS
    '제주 실시간 원천의 시각을 그대로 쓴다(KST). 무보정.';


-- -----------------------------------------------------------------------------
-- 지역난방 열수요 (시간별)
--   year/month/day/hour 는 timestamp 와 완전히 중복이라(불일치 0행) 제외했다.
-- -----------------------------------------------------------------------------
CREATE VIEW research.heat_demand AS
SELECT
    h.timestamp,
    h.branch,
    h.heat_demand,
    h.temperature,
    h.temperature_chill,
    h.humidity,
    h.wind_direction,
    h.wind_speed,
    h.rain_1h,
    h.rain_daily
FROM heat_demand h;

COMMENT ON VIEW research.heat_demand IS
    '지사별 시간별 열수요 + 동반 기상. branch 는 research.heat_demand_location.name 과 조인한다.';
COMMENT ON COLUMN research.heat_demand."timestamp" IS
    'KST. 이 저장소에 적재 코드가 없어 원천 라벨 규약(구간시작/구간종료)을 확인하지 못했다 — 보정하지 않았다.';


CREATE VIEW research.heat_demand_location AS
SELECT
    l.name,
    l.address,
    l.latitude,
    l.longitude
FROM heat_demand_location l;

COMMENT ON VIEW research.heat_demand_location IS
    '열수요 지사 위치. name 이 research.heat_demand.branch 와 대응한다.';


-- -----------------------------------------------------------------------------
-- 시간별 수요 × 기상 (파생 테이블)
-- -----------------------------------------------------------------------------
CREATE VIEW research.demand_weather_1h AS
SELECT
    d.timestamp,
    d.station_name,
    d.temperature,
    d.humidity,
    d.demand_avg,
    d.is_holiday,
    d.day_type
FROM demand_weather_1h d;

COMMENT ON VIEW research.demand_weather_1h IS
    'demand_5min 시간평균 × ASOS 관측을 시각으로 조인한 파생 테이블. 지점 수만큼 수요값이 반복되므로 수요만 필요하면 research.demand_5min 을 쓸 것.';
COMMENT ON COLUMN research.demand_weather_1h."timestamp" IS
    '구간시작 KST. demand_avg 는 [H, H+1) 구간의 5분값 평균이라 구간시작이 확정적이다. 기상 컬럼은 ASOS 관측값을 같은 시각에 붙인 것이라 ASOS 라벨 규약을 따른다.';
COMMENT ON COLUMN research.demand_weather_1h.demand_avg IS
    '해당 1시간 구간 전국 수요(MW)의 5분값 평균.';


-- -----------------------------------------------------------------------------
-- 권한 — pv DB 와 같은 그룹 role 패턴 (뷰 재생성 시 권한이 복구된다)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_ro') THEN
        CREATE ROLE research_ro NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA research TO research_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA research TO research_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA research GRANT SELECT ON TABLES TO research_ro;

COMMIT;


-- =============================================================================
-- 개인 role 발급 —  -v role_name=<이름> -v password=<비밀번호> 를 준 경우에만 실행
-- =============================================================================
\if :{?role_name}
  \if :{?password}
BEGIN;
CREATE ROLE :role_name LOGIN PASSWORD :'password';
REVOKE ALL ON SCHEMA public FROM :role_name;   -- public 은 뷰 뒤에 숨긴다
GRANT research_ro TO :role_name;
ALTER ROLE :role_name SET search_path = research;
ALTER ROLE :role_name SET statement_timeout = '60s';
ALTER ROLE :role_name SET log_statement = 'all';   -- 개인별 감사 추적
ALTER ROLE :role_name CONNECTION LIMIT 5;
COMMIT;
\echo '[demand] role 발급 완료:' :role_name
  \else
-- 비밀번호를 안 주면 :'password' 가 리터럴 ":password" 로 새어 들어간다. 확실히 실패시킨다.
DO $$ BEGIN RAISE EXCEPTION 'role_name 을 줬으면 password 도 줘야 한다 (-v password="...")'; END $$;
  \endif
\endif


-- =============================================================================
-- 개인 role 회수 —  -v drop_role=<이름> 을 준 경우에만 실행
-- =============================================================================
\if :{?drop_role}
DROP ROLE :drop_role;
\echo '[demand] role 회수 완료:' :drop_role
\endif


-- =============================================================================
-- 검증 — 한 번 돌리고 버리는 쿼리.
-- =============================================================================
--
-- (1) 뷰 5개가 있는가
--     SELECT table_name FROM information_schema.views
--      WHERE table_schema='research' ORDER BY 1;
--
-- (2) id / created_at 이 새어나가지 않았는가
--     SELECT table_name, column_name FROM information_schema.columns
--      WHERE table_schema='research' AND column_name IN ('id','created_at','updated_at');
--     -- 기대: 0 rows
--
-- (3) 행수가 원본과 맞는가
--     SELECT (SELECT count(*) FROM demand_5min)        = (SELECT count(*) FROM research.demand_5min)        AS d5_ok,
--            (SELECT count(*) FROM jeju_supply_demand) = (SELECT count(*) FROM research.jeju_supply_demand) AS jeju_ok,
--            (SELECT count(*) FROM heat_demand)        = (SELECT count(*) FROM research.heat_demand)        AS heat_ok,
--            (SELECT count(*) FROM demand_weather_1h)  = (SELECT count(*) FROM research.demand_weather_1h)  AS dw_ok;
--     -- 기대: 전부 t
--
-- (4) 읽기전용 role 권한 (검증용 role 을 만들어 확인하고 반드시 DROP 한다)
--     psql -U demand -d demand -v role_name=research_probe -v password=probe -f 이 파일
--     PGPASSWORD=probe psql -h localhost -p 5433 -U research_probe -d demand \
--       -c "SELECT count(*) FROM public.demand_5min;"     -- 기대: permission denied
--     PGPASSWORD=probe psql -h localhost -p 5433 -U research_probe -d demand \
--       -c "SELECT count(*) FROM research.demand_5min;"   -- 기대: 1322047 (증가 중)
--     psql -U demand -d demand -v drop_role=research_probe -f 이 파일
-- =============================================================================
