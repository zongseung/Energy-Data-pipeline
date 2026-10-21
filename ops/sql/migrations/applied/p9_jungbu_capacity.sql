-- P9-8: 중부 설비용량 채우기 (2026-10-21)
--   출처: 공공데이터 15139102(일자별 소수력 — **호기 단위**) · 15140015(지역별 태양광 — 발전소 단위)
--   **1:1 로 대응되는 것만 넣는다.** 공식 태양광은 발전소 단위라 호기가 여럿이면 쪼갤 근거가 없다:
--     제주성읍리태양광 1,994kW ↔ 성읍 1·2호기 / 신보령태양광 4,038kW ↔ 1호기(+2호기는 전구간0)
--     인천아시아드 1,948·2,743kW ↔ 1·2호기 (증설 이력인지 호기별인지 불명)
--   → 위 세 건은 비워 둔다. 신보령 제2소수력도 공식 자료에 없다.
BEGIN;
UPDATE plants SET capacity_mw = v.mw FROM (VALUES
  ('boryeong-hydro-1',    5.0),     -- 보령소수력 1호기 5,000kW
  ('boryeong-hydro-2',    2.5),     -- 보령소수력 2호기 2,500kW
  ('sinboryeong-hydro-1', 5.0),     -- 신보령소수력 1호기 5,000kW
  ('jejudae-pv',          1.087),   -- 제주대태양광 1,087kW (1:1)
  ('boryeong-floating-pv',1.172),   -- 보령수상태양광 1,172kW (1:1, 분절 합친 계열)
  ('sejong-pv-1',         0.327)    -- 세종태양광 327kW (1:1)
) AS v(sk, mw) WHERE plants.operator='jungbu' AND plants.source_key = v.sk;
COMMIT;
