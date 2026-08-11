-- ════════════════════════════════════════════════════════════════════════
-- P8. 시간규약 보정을 v_generation_hourly 로 이동 (데이터 무변경)
--
-- 문제: generation 테이블에는 소스별 시간규약(구간시작/hour-ending)이 혼재 적재
--       되는데, 보정이 research.generation(연구원용) 한 곳에만 있어서
--       v_generation_hourly 소비자(Grafana·energy_hub FDW)는 남부 태양광
--       2026-01-01 이전 행과 남동 화력/연료전지/수력 행을 1시간 늦은 라벨로
--       보고 있었다 (태양광이 22시에 발전 중인 것으로 표시).
--
-- 해법: 보정 CASE 를 v_generation_hourly 한 곳에 두고, 다른 모든 뷰가 이 위에
--       계층화한다. 수집기·원본 테이블·기존 행은 건드리지 않는다.
--         generation(원본, 규약 혼재)
--           └ v_generation_hourly(보정, 단일 규약: KST 구간시작) ← 유일한 CASE
--               ├ v_generation_daily / v_generation_monthly (P7 롤업)
--               └ research.generation (sql/research/pv_research.sql)
--
-- 보정 규칙 근거: docs/time-convention-audit.md §7 (임의로 고치지 말 것)
-- 멱등: CREATE OR REPLACE. plant_id 는 기존 컬럼 순서 보존을 위해 끝에 추가.
-- 적용: docker exec -i pv-data-postgres psql -U pv -d pv \
--         < scripts/migrations/p8_time_convention_views.sql
-- ════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_generation_hourly AS
SELECT
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
    p.plant_name,
    p.unit_no,
    p.fuel_type,
    p.operator,
    p.region,
    p.lat,
    p.lon,
    g.gen_kwh,
    g.plant_id
FROM generation g
JOIN plants p USING (plant_id);

COMMENT ON VIEW v_generation_hourly IS
  '시간별 발전량 — KST 구간시작으로 보정된 단일 규약 (09:00 = [09:00,10:00) 구간). '
  '보정 CASE 는 여기 한 곳에만 있다. 근거: docs/time-convention-audit.md §7. '
  '풍력은 규약 미확정으로 무보정(±1h 불확실).';

-- ── P7 롤업을 보정된 v_generation_hourly 위로 계층화 ─────────────────────
CREATE OR REPLACE VIEW v_generation_daily AS
SELECT
    (v.timestamp)::date          AS gen_date,      -- KST 달력일
    v.plant_id,
    v.plant_name,
    v.operator,
    v.fuel_type,
    v.region,
    SUM(v.gen_kwh)               AS gen_kwh,        -- 일 합계(누적 에너지)
    COUNT(v.gen_kwh)             AS hours_count     -- 적재된 시간수(24=완전, 결측 탐지)
FROM v_generation_hourly v
GROUP BY 1, 2, 3, 4, 5, 6;

COMMENT ON VIEW v_generation_daily IS
  '발전량 일별 롤업(KST, kWh 합). 보정된 v_generation_hourly의 표준 일집계. energy_hub FDW/대시보드 공용 인터페이스';

CREATE OR REPLACE VIEW v_generation_monthly AS
SELECT
    date_trunc('month', v.timestamp)::date   AS gen_month,   -- 해당 월 1일(KST)
    v.plant_id,
    v.plant_name,
    v.operator,
    v.fuel_type,
    v.region,
    SUM(v.gen_kwh)                           AS gen_kwh,      -- 월 합계
    COUNT(DISTINCT (v.timestamp)::date)      AS days_count    -- 데이터 있는 일수(결측 탐지)
FROM v_generation_hourly v
GROUP BY 1, 2, 3, 4, 5, 6;

COMMENT ON VIEW v_generation_monthly IS
  '발전량 월별 롤업(KST, kWh 합). 보정된 v_generation_hourly의 표준 월집계. energy_hub FDW/대시보드 공용 인터페이스';
