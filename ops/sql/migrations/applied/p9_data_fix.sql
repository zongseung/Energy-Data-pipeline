-- P9-4: 데이터 교정 (2026-10-21)
--   ① 한경: operator 자리에 발전소명이 들어가 있었다. 한경풍력은 남부발전 소유다
--     (남부 파일데이터 15043410_한경풍력.csv 가 증거). operator='hangyoung' 으로 조회하는 코드는 없다.
--   ② 서부 분절 병합: 같은 설비가 이름 표기만 바뀌어 두 행으로 갈렸다. 기간이 안 겹쳐
--     PK 충돌 0건(확인 완료). 행이 많은 쪽(49·50)으로 모으고 이름을 짧은 쪽으로 교정.
--     ※ 화순 용량이 48=16MW / 50=18MW 로 다른데 판정 자료가 없어 50의 18MW 를 유지한다.
--   ③ 결측 표기 통일: NaN(서부 30) · -1(남부 24, 남동 1) → NULL.
-- 되돌리기: data/backup/{seobu_merge,nan_negative,plants}_*_20261021.csv
BEGIN;

UPDATE plants SET operator='nambu', plant_name='한경풍력', source_key='hangyeong-wind'
 WHERE plant_id=51;

UPDATE generation SET plant_id=49 WHERE plant_id=47;
UPDATE generation SET plant_id=50 WHERE plant_id=48;
DELETE FROM plants WHERE plant_id IN (47,48);
UPDATE plants SET plant_name='장흥풍력', source_key='장흥풍력' WHERE plant_id=49;
UPDATE plants SET plant_name='화순풍력', source_key='화순풍력' WHERE plant_id=50;

UPDATE generation SET gen_kwh=NULL WHERE gen_kwh='NaN'::float8 OR gen_kwh<0;

COMMIT;

-- P9-4b (2026-10-21 추가): region 오류 1건.
--   plant_id 3 남제주소내는 좌표(33.2371, 126.3417)가 제주인데 region='mainland' 였다.
--   좌표로 전수 검사(lat < 33.6 AND region <> 'jeju')한 결과 이 1건뿐이다.
UPDATE plants SET region = 'jeju' WHERE plant_id = 3;
