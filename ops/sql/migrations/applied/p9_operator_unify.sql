-- P9-6: operator 값 체계 통일 (2026-10-21)
--   체계가 셋이 섞여 있었다 — 한글 로마자(nambu/namdong/seobu) + 영문 약칭(ewp/ekr).
--   한글 로마자로 통일한다. (hangyoung 은 P9-4 에서 nambu 로 흡수됨 — 발전소명이 operator 자리에 있었다)
--   DB 뷰에 operator 값 하드코딩은 v_generation_hourly 의 'namdong' 뿐이고 그건 안 바뀐다.
BEGIN;
UPDATE plants SET operator = 'dongseo'    WHERE operator = 'ewp';   -- 한국동서발전
UPDATE plants SET operator = 'nongeochon' WHERE operator = 'ekr';   -- 한국농어촌공사
COMMIT;
