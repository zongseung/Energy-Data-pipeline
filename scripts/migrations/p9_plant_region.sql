-- P9: 발전소 시도·시군구 (2026-08-25, DB 적용 완료)
--
-- research.plants 의 region 은 mainland/jeju 2값뿐이라 지역이 아니다. 지역별
-- 집계도, "이 발전소 어디 있나"도 답할 수 없었다. 좌표는 있으니 역지오코딩으로
-- 채운다 — 좌표가 부지 단위라 89기가 28개 지점이고, 지점 단위로만 조회한다.
--
-- 값 채우기: uv run python scripts/backfill_plant_region.py
-- 주의: 좌표는 ±2km 근사라 시·군·구까지만 신뢰할 수 있다. 번지·도로명을 좌표에서
--       뽑으면 2km 밖 엉뚱한 건물이 찍힌다 — address 는 원천 자료로만 채울 것.

ALTER TABLE public.plants ADD COLUMN IF NOT EXISTS sido    varchar(30);
ALTER TABLE public.plants ADD COLUMN IF NOT EXISTS sigungu varchar(30);

COMMENT ON COLUMN public.plants.sido IS
  '시도. 좌표 역지오코딩(OSM Nominatim) 값이라 2026-07 행정구역 개편이 반영돼 있다 — address 컬럼보다 최신이다.';
COMMENT ON COLUMN public.plants.sigungu IS
  '시군구. 좌표가 부지 단위라 같은 부지의 호기는 같은 값을 갖는다. 광역시 일부는 구가 안 잡혀 NULL 이다.';

CREATE OR REPLACE VIEW research.plants AS
SELECT p.plant_id,
    p.plant_name,
    p.unit_no,
    p.operator,
    p.fuel_type,
    p.region,
    p.capacity_mw,
    p.lat,
    p.lon,
    p.plant_id = 140 AS is_aggregate,
        CASE
            WHEN p.plant_id = ANY (ARRAY[30, 35]) THEN '전면무효'::text
            WHEN p.plant_id = ANY (ARRAY[8, 20, 21, 23, 25, 27, 28, 29, 32, 37]) THEN '시간별무효'::text
            WHEN p.fuel_type::text <> 'solar'::text THEN '미검증'::text
            ELSE '정상'::text
        END AS data_quality,
        CASE
            WHEN p.plant_id = ANY (ARRAY[20, 29, 32]) THEN '2025-07-01'::date
            WHEN p.plant_id = ANY (ARRAY[8, 21, 23, 25, 27, 28, 30, 35, 37]) THEN NULL::date
            WHEN p.fuel_type::text <> 'solar'::text THEN NULL::date
            ELSE '1900-01-01'::date
        END AS hourly_valid_from,
        CASE
            WHEN p.plant_id = ANY (ARRAY[30, 35]) THEN NULL::date
            WHEN p.plant_id = ANY (ARRAY[20, 29, 32]) THEN '2024-01-01'::date
            WHEN p.plant_id = 25 THEN '2023-01-01'::date
            WHEN p.fuel_type::text <> 'solar'::text THEN NULL::date
            ELSE '1900-01-01'::date
        END AS daily_valid_from,
        CASE
            WHEN p.plant_id = 37 THEN '2025-09-30'::date
            ELSE '2999-12-31'::date
        END AS daily_valid_to,
    COALESCE(
        CASE p.plant_id
            WHEN 23 THEN '원천이 하루 총량을 24시 한 칸에 넣어 보냄. 일별합계만 사용. 반올림값·결측 149일 포함'::text
            WHEN 30 THEN '원천이 월 단위 값을 일수로 나눠 매일 같은 값을 보냄. 일별·시간별 모두 사용 불가'::text
            WHEN 35 THEN '원천이 월 단위 값을 일수로 나눠 매일 같은 값을 보냄. 일별·시간별 모두 사용 불가'::text
            WHEN 8 THEN '정오 억제 + 저녁 정격출력 고정. ESS 연계 계량 추정. 일별합계는 유효'::text
            WHEN 25 THEN '정오 억제 + 저녁 정격출력 고정. 2022년은 일 경계 이월로 일별합계도 무효'::text
            WHEN 20 THEN 'ESS 연계 계량 추정. 2025-07-01부터 시간별 정상. 2022~2023 일별합계는 이월로 무효'::text
            WHEN 29 THEN 'ESS 연계 계량 추정. 2025-07-01부터 시간별 정상. 2022~2023 일별합계는 이월로 무효'::text
            WHEN 32 THEN 'ESS 연계 계량 추정. 2025-07-01부터 시간별 정상. 2022~2023 일별합계는 이월로 무효'::text
            WHEN 21 THEN 'ESS 연계 계량 추정. 일별합계는 유효'::text
            WHEN 27 THEN 'ESS 연계 계량 추정. 일별합계는 유효'::text
            WHEN 28 THEN 'ESS 연계 계량 추정. 일별합계는 유효'::text
            WHEN 37 THEN 'ESS 연계 계량 추정. 2025-10-01 이후 전 구간 0 — 발전 없음이 아니라 원인 불명'::text
            WHEN 49 THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-08-01부터는 plant_id 47(장흥풍력)로 이어진다 — 시계열 연결 시 병합 필요'::text
            WHEN 47 THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-07-31까지는 plant_id 49(장흥 풍력 발전소)에 있다'::text
            WHEN 50 THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-08-01부터는 plant_id 48(화순풍력)로 이어진다 — 시계열 연결 시 병합 필요'::text
            WHEN 48 THEN '시각 ±1시간 불확실(시간 규약 미확정). 2022-07-31까지는 plant_id 50(화순 풍력 발전소)에 있다'::text
            WHEN 140 THEN '영암 사업 전체 계열(2019~2021). 원천이 이 기간에 1차/2차를 구분해 주지 않아 통합값으로만 존재한다. 141·142(2022~)와 기간이 겹치지 않으므로 그대로 합산해도 이중계상되지 않는다 — 오히려 제외하면 2019~2021 영암 발전량이 사라진다'::text
            ELSE NULL::text
        END,
        CASE
            WHEN p.fuel_type::text = 'wind'::text THEN '시각 ±1시간 불확실 — 원천 라벨이 구간시작인지 구간종료인지 확정 근거가 없어 보정하지 않았다'::text
            WHEN p.fuel_type::text <> 'solar'::text THEN '태양광 일주곡선 검사 대상이 아니었음(미검증). 시간 보정은 적용돼 있다'::text
            ELSE NULL::text
        END) AS data_quality_note,
    p.sido,
    p.sigungu,
    p.address
   FROM plants p;
