-- =============================================================================
-- research 스키마 — 연구원 배포용 읽기전용 뷰 (pv DB)
-- =============================================================================
-- 운영 테이블(public.*)은 감추고 이 스키마의 뷰만 노출한다. 뷰는 소유자(pv)
-- 권한으로 실행되므로 읽기전용 role 은 public 스키마 테이블을 직접 SELECT 할 수 없다.
--
-- 실행법
--   뷰만 생성/갱신 : psql -U pv -d pv -f sql/research/pv_research.sql
--   role 발급      : psql -U pv -d pv -v role_name=hong -v password="$PW" \
--                         -f sql/research/pv_research.sql
--   role 회수      : psql -U pv -d pv -v drop_role=hong -f sql/research/pv_research.sql
--   (비밀번호는 파일에 쓰지 말고 변수로 넘긴다. 셸 히스토리에 남지 않게 할 것.)
--
-- 근거 문서 — 아래 SQL 조각은 여기서 그대로 가져왔다. 임의로 고치지 말 것.
--   docs/time-convention-audit.md          §7 — generation 시간 보정 규칙
--   docs/broken-plants-audit.md            §6 — data_quality / *_valid_* 등급
--   docs/asos-solar-radiation-stations.md     — 일사 관측 63지점 / 미관측 32지점
--   docs/plant-coordinates-audit.md           — 좌표 신뢰도(전부 근사)
--
-- 불변 제약: generation.source 는 어떤 뷰에도 노출하지 않는다(수집 방법 은폐).
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA IF NOT EXISTS research;
COMMENT ON SCHEMA research IS
    '연구원 배포용 읽기전용 뷰. 운영 테이블은 public 스키마에 있으며 접근 권한이 없다.';

-- 컬럼 구성이 바뀌면 CREATE OR REPLACE VIEW 가 실패하므로 매번 새로 만든다.
-- 권한은 아래 research_ro 그룹 GRANT 로 항상 복구된다.
DROP VIEW IF EXISTS
    research.generation,
    research.plants,
    research.smp_hourly,
    research.smp_realtime_jeju,
    research.smp_weighted_avg,
    research.weather_asos
CASCADE;


-- -----------------------------------------------------------------------------
-- research.plants — 발전소 마스터 + 데이터 품질 등급
--   등급 CASE 4개는 docs/broken-plants-audit.md §6 에서 그대로 복사했다.
--
--   data_quality
--     정상       : 시간별·일별 모두 사용 가능 (태양광 일주곡선 검사 통과)
--     시간별무효 : 시간별 분포를 쓰면 안 됨. 일별 합계는 daily_valid_* 참조
--     전면무효   : 일별 합계조차 신뢰 불가. 사용 권장하지 않음
--     미검증     : 이 감사의 대상이 아니었음(비태양광 46기). '정상' 이 아니다.
--                  특히 풍력 3계열은 시간 규약 자체가 미확정이다.
--
--   ※ '시간별무효' 라도 hourly_valid_from 이 있으면 그 날짜 이후는 시간별이 정상이다.
--     영흥태양광 #3 (20/29/32) 이 여기 해당하며, data_quality 만 보고 걸러내면
--     2025-07-01 이후 약 13개월 정상 구간이 통째로 버려진다.
-- -----------------------------------------------------------------------------
CREATE VIEW research.plants AS
SELECT
    p.plant_id,
    p.plant_name,
    p.unit_no,            -- plant_name 은 유일하지 않다(삼척소내 3기·부산본부 2기).
                          -- 발전소 단위 집계는 plant_id 또는 (plant_name, unit_no) 로 할 것.
    p.operator,
    p.fuel_type,
    p.region,
    p.capacity_mw,
    p.lat,
    p.lon,

    -- 실제 발전소가 아니라 다른 행들의 합계 계열.
    -- 140(영암태양광_합계)은 141·142 와 같은 사업을 합산한 계열이다.
    -- 현재 적재분은 140=2019~2021, 141·142=2022~2025 로 시간대가 겹치지 않지만,
    -- 향후 백필로 겹치면 즉시 이중계상된다. 전체 합산 시 is_aggregate 를 제외할 것.
    (p.plant_id = 140) AS is_aggregate,

    CASE
        WHEN p.plant_id IN (30, 35)                        THEN '전면무효'
        WHEN p.plant_id IN (8, 20, 21, 23, 25, 27, 28, 29, 32, 37)
                                                           THEN '시간별무효'
        WHEN p.fuel_type <> 'solar'                        THEN '미검증'  -- 이 감사는 태양광만 봤다
        ELSE '정상'
    END AS data_quality,

    -- 시간별 값을 믿어도 되는 시작일. NULL = 시간별을 믿을 근거가 없음(무효 또는 미검증)
    CASE
        WHEN p.plant_id IN (20, 29, 32)                    THEN DATE '2025-07-01'
        WHEN p.plant_id IN (8, 21, 23, 25, 27, 28, 30, 35, 37)
                                                           THEN NULL
        WHEN p.fuel_type <> 'solar'                        THEN NULL      -- 미검증. 풍력은 시간규약도 미확정
        ELSE DATE '1900-01-01'
    END AS hourly_valid_from,

    -- 일별 합계를 믿어도 되는 구간. from 이 NULL 이면 일별도 사용 불가 또는 미검증
    CASE
        WHEN p.plant_id IN (30, 35)                        THEN NULL               -- 월 상수
        WHEN p.plant_id IN (20, 29, 32)                    THEN DATE '2024-01-01'  -- 2022~23 일경계 이월
        WHEN p.plant_id  = 25                              THEN DATE '2023-01-01'  -- 2022 일경계 이월
        WHEN p.fuel_type <> 'solar'                        THEN NULL               -- 미검증
        ELSE DATE '1900-01-01'
    END AS daily_valid_from,
    CASE
        WHEN p.plant_id  = 37                              THEN DATE '2025-09-30'  -- 이후 전면 0
        ELSE DATE '2999-12-31'
    END AS daily_valid_to,

    COALESCE(
        CASE p.plant_id
            WHEN 23  THEN '원천이 하루 총량을 24시 한 칸에 넣어 보냄. 일별합계만 사용. 반올림값·결측 149일 포함'
            WHEN 30  THEN '원천이 월 단위 값을 일수로 나눠 매일 같은 값을 보냄. 일별·시간별 모두 사용 불가'
            WHEN 35  THEN '원천이 월 단위 값을 일수로 나눠 매일 같은 값을 보냄. 일별·시간별 모두 사용 불가'
            WHEN 8   THEN '정오 억제 + 저녁 정격출력 고정. ESS 연계 계량 추정. 일별합계는 유효'
            WHEN 25  THEN '정오 억제 + 저녁 정격출력 고정. 2022년은 일 경계 이월로 일별합계도 무효'
            WHEN 20  THEN 'ESS 연계 계량 추정. 2025-07-01부터 시간별 정상. 2022~2023 일별합계는 이월로 무효'
            WHEN 29  THEN 'ESS 연계 계량 추정. 2025-07-01부터 시간별 정상. 2022~2023 일별합계는 이월로 무효'
            WHEN 32  THEN 'ESS 연계 계량 추정. 2025-07-01부터 시간별 정상. 2022~2023 일별합계는 이월로 무효'
            WHEN 21  THEN 'ESS 연계 계량 추정. 일별합계는 유효'
            WHEN 27  THEN 'ESS 연계 계량 추정. 일별합계는 유효'
            WHEN 28  THEN 'ESS 연계 계량 추정. 일별합계는 유효'
            WHEN 37  THEN 'ESS 연계 계량 추정. 2025-10-01 이후 전 구간 0 — 발전 없음이 아니라 원인 불명'
            -- 서부 풍력은 원천 CSV 발전기명이 2022-08-01자로 바뀌어 같은 사이트가 두 행으로 갈렸다.
            WHEN 49  THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-08-01부터는 plant_id 47(장흥풍력)로 이어진다 — 시계열 연결 시 병합 필요'
            WHEN 47  THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-07-31까지는 plant_id 49(장흥 풍력 발전소)에 있다'
            WHEN 50  THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-08-01부터는 plant_id 48(화순풍력)로 이어진다 — 시계열 연결 시 병합 필요'
            WHEN 48  THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-07-31까지는 plant_id 50(화순 풍력 발전소)에 있다'
            WHEN 140 THEN '141·142 의 합계 계열(2019~2021 구간만 적재). 개별 발전소가 아니다 — 합산 시 제외할 것'
            ELSE NULL
        END,
        CASE
            WHEN p.fuel_type = 'wind'  THEN '시각 ±1시간 불확실 — 원천 라벨이 구간시작인지 구간종료인지 확정 근거가 없어 보정하지 않았다'
            WHEN p.fuel_type <> 'solar' THEN '태양광 일주곡선 검사 대상이 아니었음(미검증). 시간 보정은 적용돼 있다'
        END
    ) AS data_quality_note
FROM plants p;

COMMENT ON VIEW research.plants IS
    '발전소 마스터. 품질 등급 근거: docs/broken-plants-audit.md';
COMMENT ON COLUMN research.plants.lat IS
    '위도. 검증된 것은 태양광 45기뿐이며(44기 보유, 율치태양광만 NULL) 그 44기는 전부 부지 POI·행정구역 중심 근사값(대략 ±2km)이라 정밀 지번 좌표가 아니다 — ASOS 최근접 지점 매칭에는 충분하나 그 이상은 재검증 필요. 비태양광 40기(화력 24·연료전지 8·수력 4·풍력 4) 좌표는 검증 대상이 아니었다 — 신뢰도 불명. 근거: docs/plant-coordinates-audit.md';
COMMENT ON COLUMN research.plants.lon IS
    '경도. lat 과 동일 — 태양광만 검증됐고 비태양광은 신뢰도 불명.';
COMMENT ON COLUMN research.plants.capacity_mw IS
    '정격용량 MW. 91기 중 41기만 값이 있고 공시값과 추정값이 섞여 있다. 정밀도를 보장하지 않는다.';
COMMENT ON COLUMN research.plants.is_aggregate IS
    'true 면 실제 발전소가 아니라 합계 계열(현재 plant_id 140 하나). 전체 합산 시 제외할 것.';
COMMENT ON COLUMN research.plants.data_quality IS
    '정상 / 시간별무효 / 전면무효 / 미검증. ''미검증''은 비태양광 46기 — 검사 수단이 없었을 뿐 정상이라는 뜻이 아니다.';
COMMENT ON COLUMN research.plants.hourly_valid_from IS
    '이 날짜 이후의 시간별 값은 신뢰 가능. NULL 이면 시간별을 믿을 근거가 없다. data_quality 가 ''시간별무효''여도 이 값이 있으면 그 이후 구간은 쓸 수 있다(영흥태양광 #3 3기).';


-- -----------------------------------------------------------------------------
-- research.generation — 시간 보정된 발전량
--   보정 규칙은 docs/time-convention-audit.md §7 에서 그대로 복사했다.
--   원천 hour-ending(라벨 N = 구간 [N-1, N)) 을 구간시작 KST 로 통일한다.
--
--   ※ source 컬럼은 노출하지 않는다. 아래 SELECT 목록에 추가하지 말 것.
--   ※ 연구원이 조인 없이 쓰도록 plants 속성과 품질 등급을 펼쳐 둔다.
-- -----------------------------------------------------------------------------
CREATE VIEW research.generation AS
SELECT
    g.plant_id,
    p.plant_name,
    p.unit_no,
    p.operator,
    p.fuel_type,
    g.timestamp - CASE
        -- 남부 태양광: 2025-12-31 이전 레거시 적재분만 1시간 늦다.
        --             2026-01-01 00:00 이후는 이미 구간시작이므로 건드리지 않는다.
        WHEN p.operator = 'nambu'  AND p.fuel_type = 'solar'
             AND g.timestamp < TIMESTAMP '2026-01-01 00:00'          THEN INTERVAL '1 hour'

        -- 남동 비태양광(KOEN CSV 경로): hour-ending 라벨을 무보정 적재.
        WHEN p.operator = 'namdong'
             AND p.fuel_type IN ('thermal', 'fuel_cell', 'hydro')    THEN INTERVAL '1 hour'

        -- 풍력 3계열(namdong/seobu/hangyoung)은 원천 라벨 의미 미확정 → 보정하지 않는다.
        -- 확정되면 아래를 살린다 (docs/time-convention-audit.md §5):
        -- WHEN p.operator IN ('namdong', 'hangyoung') AND p.fuel_type = 'wind'
        --                                                          THEN INTERVAL '1 hour'

        ELSE INTERVAL '0'
    END AS "timestamp",
    g.gen_kwh,
    p.is_aggregate,
    p.data_quality,
    p.hourly_valid_from
FROM generation g
JOIN research.plants p USING (plant_id);

COMMENT ON VIEW research.generation IS
    '시간별 발전량. 시간 보정 근거: docs/time-convention-audit.md §7';
COMMENT ON COLUMN research.generation."timestamp" IS
    '구간시작 KST. 값 09:00 은 [09:00, 10:00) 구간을 뜻한다. 원천의 hour-ending 라벨은 여기서 이미 보정됐다. 단 풍력(fuel_type=''wind'')은 원천 라벨 의미가 미확정이라 무보정 — ±1시간 불확실.';
COMMENT ON COLUMN research.generation.gen_kwh IS
    '해당 1시간 구간의 발전량(kWh). 원천 CSV 헤더가 MWh 로 적힌 계열도 실제 값은 kWh 다.';


-- -----------------------------------------------------------------------------
-- SMP — 시간 라벨은 수집 단계(fetch_data/smp/smp_collect.py:84)에서 이미
--       hour-ending → 구간시작으로 변환됐다. 여기서 추가 보정하지 않는다.
--       id(대리키)만 제외한다.
-- -----------------------------------------------------------------------------
CREATE VIEW research.smp_hourly AS
SELECT s.timestamp, s.region, s.price
FROM smp_hourly s;

COMMENT ON VIEW research.smp_hourly IS
    '하루전시장 시간별 SMP(원/kWh). region: 2010-01-01 이전은 단일시장이라 unified, 이후 land/jeju 분리.';
COMMENT ON COLUMN research.smp_hourly."timestamp" IS
    '구간시작 KST (수집 단계에서 hour-ending → 구간시작 변환 완료).';

CREATE VIEW research.smp_realtime_jeju AS
SELECT s.timestamp, s.region, s.price, s.is_confirmed
FROM smp_realtime_jeju s;

COMMENT ON VIEW research.smp_realtime_jeju IS
    '제주 실시간시장 15분 SMP(원/kWh). 음수 가격이 정상적으로 발생한다.';
COMMENT ON COLUMN research.smp_realtime_jeju.is_confirmed IS
    'false 면 잠정치 — 이후 확정치로 갱신될 수 있다.';

CREATE VIEW research.smp_weighted_avg AS
SELECT s.period_type, s.period, s.region, s.price_type, s.weighted_avg
FROM smp_weighted_avg s;

COMMENT ON VIEW research.smp_weighted_avg IS
    '가중평균 가격(원/kWh). period_type: daily/monthly/yearly, price_type: smp/blmp(BLMP 는 육지 2001~2006 만 존재).';


-- -----------------------------------------------------------------------------
-- research.weather_asos — ASOS 시간별 기상
--   일사량은 95개 지점 중 63개만 관측한다(docs/asos-solar-radiation-stations.md).
--   미관측 32개 지점은 solar_radiation 이 사실상 전부 NULL 이다(성산·제천 각 1행,
--   값 0인 이상치 2건 제외). 이는 결측이 아니라 정상이다.
--   두 경우를 구분할 수 있도록 has_solar_sensor 를 함께 내보낸다.
--
--   ※ 일사량의 시간 라벨 규약은 아직 "추정"이다(강수·일조 항목의 구간종료 서술에서
--     유추). KMA 공식 문구를 찾지 못했으므로 여기서 시프트하지 않는다.
-- -----------------------------------------------------------------------------
CREATE VIEW research.weather_asos AS
SELECT
    w.timestamp,
    w.station_name,
    w.temperature,
    w.humidity,
    w.solar_radiation,
    CASE
        WHEN w.station_name IN (
            '속초','북춘천','철원','동두천','파주','대관령','춘천','백령도','북강릉','강릉',
            '동해','서울','인천','원주','울릉도','수원','영월','충주','서산','청주',
            '대전','추풍령','안동','상주','포항','대구','전주','울산','창원','광주',
            '부산','통영','목포','여수','흑산도','완도','고창','홍성','제주','고산',
            '서귀포','진주','태백','보령','세종','정읍','고창군','영광군','김해시','순창군',
            '북창원','양산시','보성군','강진군','고흥','의령군','함양군','광양시','청송군','구미',
            '경주시','거창','밀양'
        ) THEN TRUE
        WHEN w.station_name IN (
            '울진','군산','순천','성산','강화','양평','이천','인제','홍천','정선군',
            '제천','보은','천안','부여','금산','부안','임실','남원','장수','장흥',
            '해남','진도군','봉화','영주','문경','영덕','의성','영천','합천','산청',
            '거제','남해'
        ) THEN FALSE
        ELSE NULL   -- 2025-07-01 전수조사(95지점) 이후 새로 생긴 지점 — 확인 안 됨
    END AS has_solar_sensor
FROM weather_asos w;

COMMENT ON VIEW research.weather_asos IS
    'ASOS 시간별 기상 관측. 근거: docs/asos-solar-radiation-stations.md';
COMMENT ON COLUMN research.weather_asos.solar_radiation IS
    '일사량 MJ/m². 시간 라벨이 구간시작인지 구간종료인지는 아직 추정 단계라 보정하지 않았다 — 시각 정밀도가 중요한 분석에는 주의. 야간과 미관측 지점은 NULL.';
COMMENT ON COLUMN research.weather_asos.has_solar_sensor IS
    'true 면 일사 관측 지점(63개) — solar_radiation 이 NULL 이면 야간이거나 결측. false 면 미관측 지점(32개) — 사실상 전부 NULL 이며(성산·제천 각 1행, 값 0인 이상치 2건 제외) 결측이 아니다. NULL 이면 2025-07-01 전수조사에 없던 신규 지점.';


-- -----------------------------------------------------------------------------
-- 권한 — 그룹 role 에 모아 둔다.
--   뷰를 DROP/CREATE 하면 개별 GRANT 가 사라지지만, 아래 ALTER DEFAULT PRIVILEGES
--   가 이미 그걸 복구한다(pg_default_acl 에 research_ro=r 로 등록됨).
--   그럼에도 그룹으로 모으는 이유는, 기본권한은 "그 권한을 설정한 실행 주체가
--   만든 객체"에만 붙기 때문이다. 누가 뷰를 다시 만들든 아래 GRANT 두 줄이
--   매번 권한을 되돌려 놓는다.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_ro') THEN
        CREATE ROLE research_ro NOLOGIN;
    END IF;
END
$$;

-- PG14 의 public 스키마는 PUBLIC 의사롤에 CREATE 가 붙어 있다(=UC/pv).
-- 개인 role 에서 REVOKE 해도 이 경로가 남아 연구원이 운영 DB 에 테이블을 만들 수 있다.
-- 소유자(pv)는 UC/pv 를 따로 갖고 있어 영향 없다.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

GRANT USAGE ON SCHEMA research TO research_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA research TO research_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA research GRANT SELECT ON TABLES TO research_ro;

COMMIT;

-- 감사 추적: log_connections=off + log_line_prefix 에 %u(user)/%d(database) 가 없으면
-- ALTER ROLE ... SET log_statement='all' 로 남긴 문장이 누구 것인지 복원할 수 없다
-- (PID(%p) 는 재사용되고 접속 인가 로그 자체가 안 남는다). ALTER SYSTEM 은 트랜잭션 블록
-- 안에서 실행할 수 없으므로 COMMIT 뒤, 클러스터 전체에 적용한다.
ALTER SYSTEM SET log_line_prefix = '%m [%p] %u@%d ';
ALTER SYSTEM SET log_connections = 'on';
SELECT pg_reload_conf();


-- =============================================================================
-- 개인 role 발급 —  -v role_name=<이름> -v password=<비밀번호> 를 준 경우에만 실행
-- =============================================================================
-- 같은 role_name 으로 다시 돌리면 비밀번호만 갱신한다(멱등).
\if :{?role_name}
  \if :{?password}
SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role_name') AS role_exists \gset
BEGIN;
  \if :role_exists
ALTER ROLE :role_name LOGIN PASSWORD :'password';
  \else
CREATE ROLE :role_name LOGIN PASSWORD :'password';
  \endif
REVOKE ALL ON SCHEMA public FROM :role_name;   -- PUBLIC 경유 CREATE 는 위 상시 블록에서 막았다
GRANT research_ro TO :role_name;
ALTER ROLE :role_name SET search_path = research;
ALTER ROLE :role_name SET statement_timeout = '60s';
ALTER ROLE :role_name SET log_statement = 'all';   -- 개인별 감사 추적
ALTER ROLE :role_name CONNECTION LIMIT 5;
COMMIT;
\echo '[pv] role 발급 완료:' :role_name
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
\echo '[pv] role 회수 완료:' :drop_role
\endif


-- =============================================================================
-- 검증 — 한 번 돌리고 버리는 쿼리. 위 DDL 적용 뒤 확인용.
-- =============================================================================
--
-- (1) 뷰 6개가 있는가
--     SELECT table_name FROM information_schema.views
--      WHERE table_schema='research' ORDER BY 1;
--     -- 기대: generation, plants, smp_hourly, smp_realtime_jeju,
--     --       smp_weighted_avg, weather_asos
--
-- (2) source 컬럼이 어떤 뷰에도 없는가  (가장 중요)
--     SELECT table_name, column_name FROM information_schema.columns
--      WHERE table_schema='research' AND column_name='source';
--     -- 기대: 0 rows
--
-- (3) 행수가 원본과 맞는가 (뷰는 필터가 없으므로 정확히 같아야 한다)
--     SELECT (SELECT count(*) FROM generation)   = (SELECT count(*) FROM research.generation)   AS gen_ok,
--            (SELECT count(*) FROM plants)       = (SELECT count(*) FROM research.plants)       AS plants_ok,
--            (SELECT count(*) FROM smp_hourly)   = (SELECT count(*) FROM research.smp_hourly)   AS smp_ok,
--            (SELECT count(*) FROM weather_asos) = (SELECT count(*) FROM research.weather_asos) AS asos_ok;
--     -- 기대: 전부 t
--
-- (4) 시간 보정이 실제로 걸렸는가 (남부 태양광 레거시 첫 시각이 00:00 이 되어야 한다)
--     SELECT min("timestamp") FROM research.generation
--      WHERE operator='nambu' AND fuel_type='solar';
--     -- 기대: 2015-10-23 00:00  (원본은 01:00)
--
-- (5) 트랩 3개가 컬럼으로 드러나는가
--     SELECT plant_id, plant_name, data_quality, hourly_valid_from, is_aggregate
--       FROM research.plants
--      WHERE plant_id IN (20, 29, 32, 140) OR fuel_type='wind' ORDER BY plant_id;
--     -- 기대: 20/29/32 = 시간별무효 + hourly_valid_from 2025-07-01
--     --       140 = is_aggregate t,  풍력 = 미검증
--
-- (6) 읽기전용 role 권한 (검증용 role 을 만들어 확인하고 반드시 DROP 한다)
--     psql -U pv -d pv -v role_name=research_probe -v password=probe -f 이 파일
--     PGPASSWORD=probe psql -h localhost -p 5436 -U research_probe -d pv \
--       -c "SELECT count(*) FROM public.plants;"        -- 기대: permission denied
--     PGPASSWORD=probe psql -h localhost -p 5436 -U research_probe -d pv \
--       -c "SELECT count(*) FROM research.plants;"      -- 기대: 91
--     PGPASSWORD=probe psql -h localhost -p 5436 -U research_probe -d pv \
--       -c "CREATE TABLE research.x(i int);"            -- 기대: permission denied
--     psql -U pv -d pv -v drop_role=research_probe -f 이 파일
-- =============================================================================
