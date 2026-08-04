-- ============================================================
-- namdong_solar 재마이그레이션: "24h→1행 붕괴" 복구
--
-- 문제: 구 namdong_generation 은 시각을 datetime + 별도 hour(1~24) 로 저장하는데
--       datetime 포맷이 혼재한다.
--         · 과거(2022-01 ~ 2026-05): datetime = 날짜(자정), 시각은 hour 컬럼에만 있음
--         · 라이브(2025-12 ~)        : datetime = 구간시작(= 날짜 + (hour-1)시)
--       기존 백필/트리거가 NEW.datetime 만 써서 자정포맷의 하루 24시간이
--       (자정, plant) 1행으로 붕괴 → 코어 namdong solar 가 일당 1행(132,596)으로
--       ~85% 손실 + timestamp 자정 오염.
--
-- 올바른 시각 = datetime 의 '날짜' + (hour-1)시  (= 구간시작).
--   근거: 비자정 행 96,278개 전부 extract(hour from datetime) = hour-1 로 100% 일치.
--   두 포맷 모두 이 식으로 동일하게 복원됨(검증: 재구성 고유행 = 구 고유(datetime,plant,hour) = 871,632, 충돌 0).
--
-- 절차(트랜잭션):
--   1) 기존 namdong solar 코어 행 전체 삭제 (자정 오염 포함)
--   2) date+(hour-1) 로 재적재. 동일 (ts,plant) 충돌 시 최신 id(나중 수집) 보존.
-- 멱등: 재실행해도 같은 결과.
-- ============================================================

BEGIN;

DELETE FROM generation g USING plants p
WHERE g.plant_id = p.plant_id
  AND p.operator = 'namdong' AND p.fuel_type = 'solar';

INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
SELECT DISTINCT ON (ts, plant_id) ts, plant_id, gen_kwh, 'backfill'
FROM (
    SELECT date_trunc('day', g.datetime) + ((g.hour - 1) * interval '1 hour') AS ts,
           p.plant_id                                                          AS plant_id,
           g.generation::double precision                                      AS gen_kwh,
           g.id                                                                AS id
    FROM namdong_generation g
    JOIN plants p ON p.plant_name = g.plant_name
                 AND p.operator = 'namdong' AND p.fuel_type = 'solar'
) s
ORDER BY ts, plant_id, id DESC;

-- 검증
SELECT count(*) AS namdong_solar_core_rows,
       min(g.timestamp) AS mn, max(g.timestamp) AS mx
FROM generation g JOIN plants p ON g.plant_id = p.plant_id
WHERE p.operator = 'namdong' AND p.fuel_type = 'solar';

COMMIT;
