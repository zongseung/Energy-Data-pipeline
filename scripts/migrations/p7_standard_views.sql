-- ════════════════════════════════════════════════════════════════════════
-- P7. 표준 집계 뷰 (발전 일/월 롤업)
--
-- 목적: 소비자(Grafana·energy_hub FDW)가 시간별 raw(v_generation_hourly)에서
--       매번 제각각 즉석 집계하던 것을 표준 정의 하나로 통일한다.
--       - 시간 기준: KST. generation.timestamp 는 naive KST 저장(태양광 피크 12~13시로 검증).
--         별도 타임존 변환(AT TIME ZONE) 없이 timestamp 를 그대로 KST로 취급한다.
--         (대시보드의 'AT TIME ZONE Asia/Seoul' 혼용은 이 뷰로 대체해 제거)
--       - 집계 규칙: 발전량 gen_kwh 는 누적 에너지(kWh) → SUM
--       - 시간 규약: 전 소스가 0~23시로 균일 저장(hour-ending 흘러넘침 없음) →
--         timestamp::date 로 묶으면 KST 달력일 합계가 일관. (소스별 ≤1h 의미 오프셋은
--         태양광은 야간 0이라 무영향, 24h 발전은 경계 1시간 미만 영향 — 필요 시 P8에서
--         plants.time_convention 도입해 정규화)
--
-- 멱등: CREATE OR REPLACE. 데이터 무변경(뷰만 생성).
-- 대상 DB: pv-data-postgres (:5436) — generation/plants 보유
-- ════════════════════════════════════════════════════════════════════════

-- ── 일별 발전량 (한 행 = 발전소 × 1일, KST) ──────────────────────────────
CREATE OR REPLACE VIEW v_generation_daily AS
SELECT
    (g.timestamp)::date          AS gen_date,      -- KST 달력일
    g.plant_id,
    p.plant_name,
    p.operator,
    p.fuel_type,
    p.region,
    SUM(g.gen_kwh)               AS gen_kwh,        -- 일 합계(누적 에너지)
    COUNT(g.gen_kwh)             AS hours_count     -- 적재된 시간수(24=완전, 결측 탐지)
FROM generation g
JOIN plants p USING (plant_id)
GROUP BY 1, 2, 3, 4, 5, 6;

COMMENT ON VIEW v_generation_daily IS
  '발전량 일별 롤업(KST, kWh 합). v_generation_hourly의 표준 일집계. energy_hub FDW/대시보드 공용 인터페이스';

-- ── 월별 발전량 (한 행 = 발전소 × 1개월, KST) ────────────────────────────
CREATE OR REPLACE VIEW v_generation_monthly AS
SELECT
    date_trunc('month', g.timestamp)::date   AS gen_month,   -- 해당 월 1일(KST)
    g.plant_id,
    p.plant_name,
    p.operator,
    p.fuel_type,
    p.region,
    SUM(g.gen_kwh)                           AS gen_kwh,      -- 월 합계
    COUNT(DISTINCT (g.timestamp)::date)      AS days_count    -- 데이터 있는 일수(결측 탐지)
FROM generation g
JOIN plants p USING (plant_id)
GROUP BY 1, 2, 3, 4, 5, 6;

COMMENT ON VIEW v_generation_monthly IS
  '발전량 월별 롤업(KST, kWh 합). v_generation_hourly의 표준 월집계. energy_hub FDW/대시보드 공용 인터페이스';
