-- P9-11: v_generation_hourly 의 남동 시간보정 조건을 재분류에 맞춘다 (2026-10-21)
--
-- P9-10 에서 남동 thermal 24기를 coal·combined·biomass 로 쪼갰더니
-- 보정 CASE 의 `fuel_type IN ('thermal','fuel_cell','hydro')` 에서 빠져나갔다.
-- 실측 증거 — 보정이 빠진 biomass 의 0시 평균이 2,633,429 로 다른 시각(~121,000)의 22배다.
-- 원천이 hour-ending 이라 '24시' 값이 다음날 0시로 흘러넘친 것이다.
--
-- 연료 목록으로 거는 대신 **"남동의 비태양광·비풍력 전체"** 로 조건을 바꾼다.
-- 연료를 더 쪼개도(oil 추가 등) 다시 깨지지 않는다.
--   · 남동 태양광 — 원본이 이미 구간시작(p_remigrate_namdong_solar.sql 에서 date+(hour-1)로 재적재)
--   · 남동 풍력  — 시각 규약 미확정이라 보정하지 않는다(기존과 동일)
BEGIN;

CREATE OR REPLACE VIEW v_generation_hourly AS
SELECT g."timestamp" -
       CASE
           WHEN p.operator::text = 'namdong'::text
            AND p.fuel_type::text NOT IN ('solar', 'wind')
               THEN '01:00:00'::interval
           ELSE '00:00:00'::interval
       END AS "timestamp",
   p.plant_name, p.unit_no, p.fuel_type, p.operator, p.region,
   p.lat, p.lon, g.gen_kwh, g.plant_id
  FROM generation g
  JOIN plants p USING (plant_id);

COMMIT;
