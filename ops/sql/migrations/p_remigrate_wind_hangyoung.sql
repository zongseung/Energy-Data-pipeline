-- ============================================================
-- wind_hangyoung 재마이그레이션: "여러 터빈 합산" 복구
--
-- 문제: 원본 Hangyoung_wind_power.csv 는 (timestamp, generation)만 있고
--       plant_name 이 없다. **한 timestamp 에 터빈/단지별 여러 행**(보통 2, 일부 3)이
--       들어있다(한경1·2단계 등). 로더(hangyoung_backfill.py)가 전부 plant_name='Hangyoung'
--       으로 적재 → 같은 (timestamp, 'Hangyoung') 에 값이 다른 여러 행.
--       기존 백필(DO NOTHING)/트리거(DO UPDATE)는 그중 1행만 보존 → **나머지 터빈 누락**
--       (코어 최대 15,848 = 합산값 23,780의 ~66%, 단지 1개만 반영).
--
-- 올바른 처리: 같은 (timestamp, plant) 의 터빈들을 **합산(SUM)** = 단지 총 출력.
--   검증: 합산행수 106,608, 최대 23,780kWh(≈24MW) — 한경풍력 총용량 ~27MW에 부합.
--
-- ※ 주의(잠재 footgun): hangyoung 은 일회성 CSV load(스케줄 없음)라 트리거는 휴면이지만,
--   hangyoung_backfill 을 재실행하면 per-row 트리거가 again 1터빈만 남겨 코어를 재오염시킨다.
--   재로드 시에는 반드시 이 스크립트로 다시 합산해야 한다(또는 터빈을 별 plant_name 으로 분리).
--
-- 멱등: 재실행해도 같은 결과.
-- ============================================================

BEGIN;

DELETE FROM generation g USING plants p
WHERE g.plant_id = p.plant_id AND p.operator = 'hangyoung';

INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
SELECT w.timestamp, p.plant_id, sum(w.generation), 'backfill'
FROM wind_hangyoung w
JOIN plants p ON p.plant_name = w.plant_name AND p.operator = 'hangyoung'
GROUP BY w.timestamp, p.plant_id;

-- 검증
SELECT count(*) AS hangyoung_core_rows,
       min(g.timestamp) AS mn, max(g.timestamp) AS mx,
       round(max(g.gen_kwh)::numeric, 1) AS max_kwh
FROM generation g JOIN plants p ON g.plant_id = p.plant_id
WHERE p.operator = 'hangyoung';

COMMIT;
