-- P9-3: 키 전환 (2026-10-21)
--   (operator, source_key, unit_no) 가 "같은 설비인가"의 유일한 판단 기준이 된다.
--   plant_name UNIQUE 는 해제 — 원천이 표기를 바꾸면 같은 설비가 새 행으로 들어가
--   시계열이 쪼개졌다(서부 '화순풍력'/'화순 풍력 발전소' 가 실제 사례).
--   plant_name 은 이제 표시용이다.
BEGIN;

ALTER TABLE plants ALTER COLUMN source_key SET NOT NULL;
ALTER TABLE plants ADD CONSTRAINT uq_plants_source UNIQUE (operator, source_key, unit_no);
ALTER TABLE plants DROP CONSTRAINT uq_plants_name_unit;

COMMIT;
