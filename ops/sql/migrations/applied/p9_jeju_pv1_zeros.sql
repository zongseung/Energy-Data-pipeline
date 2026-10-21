-- P9-15: 중부 jeju-pv-1 의 2021년 이후 0 행 삭제 (2026-10-21)
--
-- 설비 실태: 40kW 급 소형 태양광(제주발전본부 '태양광 1호기').
--   2018-11 ~ 2020-12 는 정상 가동 — 피크 12시 일주곡선, 합계 84.8 MWh, 최대 40kWh/h.
--   **2021-01 부터 2026-09 까지 48,732행이 전부 0** — 철거된 것으로 보인다.
--
-- 0 만 지우고 **2018~2020 실적은 남긴다**. 유효한 발전 기록이라 버릴 이유가 없고,
-- 6년간 깔린 0 이 research.generation 에서 "발전 없는 설비"로 보이는 게 문제였다.
-- 값이 전부 0 이므로 되돌릴 정보가 없다(백업 불필요).
BEGIN;

DELETE FROM generation g
 USING plants p
 WHERE g.plant_id = p.plant_id
   AND p.operator = 'jungbu' AND p.source_key = 'jeju-pv-1'
   AND g.timestamp >= '2021-01-01';

COMMIT;
