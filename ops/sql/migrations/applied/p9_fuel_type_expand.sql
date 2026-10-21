-- P9-10: fuel_type 세분화 — thermal 을 실제 연료로 재분류 (2026-10-21)
--
-- 왜: `thermal` 24기가 LNG 복합·유연탄·바이오매스를 한 값에 뭉쳐 담고 있었다.
--   발전원별 분석(재생 비중, 배출, 연료비)에서 이 셋을 구분하지 못하면 답이 틀린다.
--   남부 PwrGenTran 을 적재하면 석탄 8 · 중유 2 · 복합 48 이 추가로 들어오므로
--   그때 쓸 값을 지금 맞춘다.
--
-- 근거 (전부 `pipeline/fetch_data/gen/capacities.py` 주석과 설비 구성):
--   · 분당 10기 — "분당복합화력", 1BLOCK = GT 77.75MW×5 + ST 185MW, 2BLOCK = GT×3 + ST 115MW.
--     GT/ST 묶음 구조 자체가 복합사이클(LNG)의 증거다 → combined
--   · 영흥 6기(800·870MW) · 삼천포 4기(560·500MW) — 대형 기저부하 유연탄 → coal
--   · 영동 2기(125·200MW) — capacities.py 주석 "영동에코발전 (100% 바이오매스)" → biomass
--   · 여수 2기(340·328.6MW) — **중유로 알려져 있으나 코드에 근거가 없다.**
--     추측으로 바꾸지 않고 thermal 로 남긴다. 남동 공시자료 확인 후 oil 로 옮길 것.
--
-- fuel_type 값: solar · wind · hydro · fuel_cell · coal · combined · biomass · thermal(미분류)
--   그리고 PwrGenTran 적재 시 oil 이 추가된다.
BEGIN;

UPDATE plants SET fuel_type = 'combined'
 WHERE operator='namdong' AND fuel_type='thermal' AND site_name='분당발전본부';

UPDATE plants SET fuel_type = 'coal'
 WHERE operator='namdong' AND fuel_type='thermal' AND site_name IN ('영흥발전본부','삼천포발전본부');

UPDATE plants SET fuel_type = 'biomass'
 WHERE operator='namdong' AND fuel_type='thermal' AND site_name='영동에코발전본부';

COMMIT;
