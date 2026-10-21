-- P9-5: v_generation_hourly 의 남부 1시간 보정 CASE 제거 (2026-10-21)
--
-- 왜 있었나: 옛 nambu_transform.py 가 qhorgen01 을 1시로 읽어(1-based) source='backfill'
--   767,662행이 1시간 뒤로 밀려 적재됐다. 뷰에서 -1시간으로 땜질하고 있었다
--   (`timestamp < '2026-01-01'` 조건이 그 흔적이다).
--
-- 왜 없애나: 2026-10-21 파일데이터 재적재로 **원본이 올바른 시각**이 됐다(34계열 3,218,582행).
--   CASE 를 두면 뷰가 1시간을 또 빼서 거꾸로 틀어진다 — 실측:
--     원본 2024년 태양광 피크 12시  →  뷰 통과 후 11시
--
-- 남동 화력·연료전지·수력 보정은 그대로 둔다 (원본이 아직 hour-ending 이다).
-- 남은 남부 backfill 5계열(135,408행 — 파일에 없는 9988·9989·9985·9987·9979)은
--   여전히 1시간 밀려 있다. 이들은 API 재수집으로 교정해야 한다. [[nambu-filedata-ingest]]
BEGIN;

CREATE OR REPLACE VIEW v_generation_hourly AS
SELECT g."timestamp" -
       CASE
           WHEN p.operator::text = 'namdong'::text
            AND (p.fuel_type::text = ANY (ARRAY['thermal','fuel_cell','hydro']::text[]))
               THEN '01:00:00'::interval
           ELSE '00:00:00'::interval
       END AS "timestamp",
   p.plant_name,
   p.unit_no,
   p.fuel_type,
   p.operator,
   p.region,
   p.lat,
   p.lon,
   g.gen_kwh,
   g.plant_id
  FROM generation g
  JOIN plants p USING (plant_id);

COMMIT;
