-- ============================================================
-- operator 라벨 통일: koen -> namdong
--
-- KOEN = 한국남동발전 = 남동발전. 비태양광(thermal/fuel_cell/hydro)이
-- operator='koen'으로, 같은 회사 solar/wind는 'namdong'으로 갈려 있던
-- 불일치를 해소한다. operator 컬럼만 갱신하므로 plant_id 불변 →
-- generation 시계열(FK)에는 영향 없음.
--
-- 멱등: 이미 통일됐으면 0행 갱신.
-- ============================================================

UPDATE plants SET operator = 'namdong' WHERE operator = 'koen';

-- 확인
SELECT operator, fuel_type, count(*)
FROM plants
WHERE operator = 'namdong'
GROUP BY operator, fuel_type
ORDER BY fuel_type;
