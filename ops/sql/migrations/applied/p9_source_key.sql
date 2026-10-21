-- P9-1: plants 키 재설계 1단계 (2026-10-21)
--   · source_key 신설 — 외부 데이터를 우리 행에 잇는 끈. 값 채우기·제약은 2·3단계.
--   · plant_code → official_code 강등 — 96행 중 18행만 있고 B997/S997 은 가짜라 키가 될 수 없다.
--   · capacity_confidence 제거 — 96/96 이 default 라 정보량이 0.
-- research 스키마 뷰 7개는 위 세 컬럼을 참조하지 않는다(확인 완료) — 깨지지 않는다.
BEGIN;

ALTER TABLE plants ADD COLUMN source_key text;
COMMENT ON COLUMN plants.source_key IS
  '우리가 정한 설비 식별자. 외부 데이터를 이 행에 잇는 끈. 한 번 정하면 바꾸지 않는다';

ALTER TABLE plants RENAME COLUMN plant_code TO official_code;
COMMENT ON COLUMN plants.official_code IS
  '발전사 공식 코드(남부 strOrgCd 등). 참고용이며 키가 아니다 — 18/96 행만 있고 B997/S997 은 가짜';

ALTER TABLE plants DROP COLUMN capacity_confidence;

COMMIT;
