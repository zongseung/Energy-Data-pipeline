-- P9-2: source_key 값 채우기 (2026-10-21)
--   남부 18행은 매핑표(값대조+용량대조+공식코드표)로 확정한 슬러그.
--   나머지 78행은 현재 plant_name 이 이미 코드처럼 안정적이라 그대로 쓴다(공백만 정규화).
--   source_key 는 **설비** 단위다 — 호기는 unit_no 가 담당한다 (busan-complex 1·2호기).
BEGIN;

-- ── 남부 18행 ──
UPDATE plants SET source_key = v.sk FROM (VALUES
  (1,  'sinincheon-onsite'),        -- 8760 신인천소내태양광
  (2,  'icheon-d'),                 -- 9979 이천D(백사면B) · 2023-04 종료
  (3,  'namjeju-onsite'),           -- 997D 남제주화력소내태양광
  (4,  'busan-newport'),            -- 997G 부산신항태양광
  (5,  'busan-warehouse'),          -- 997N 부산복합자재창고태양광
  (6,  'busan-watertreat'),         -- 997Q 부산수처리태양광
  (7,  'busan-1400kwp'),            -- 997R 부산1400kWp (DB 이름이 '부산운동장'으로 잘못됨)
  (8,  'sinincheon-view'),          -- 997S 신인천전망대법사면태양광
  (9,  'sinincheon-intake'),        -- 997Y 신인천 해수취수로 상부
  (10, 'sinincheon-parking'),       -- 9985 신인천 주차장태양광
  (11, 'busanstation-parking'),     -- 9987 부산역 선상주차장 · 2023-10 종료
  (12, 'sinincheon-north'),         -- 9988 신인천 북측부지
  (13, 'sinincheon-parking-st12'),  -- 9989 신인천 1,2단계 주차장
  (14, 'busan-complex'),            -- 9976 부산복합태양광       (unit 1)
  (15, 'busan-complex'),            -- 997Z 부산복합태양광 2단계 (unit 2)
  (16, 'samcheok-onsite'),          -- 997T/997O/997X 삼척소내태양광 (unit 1·2·3)
  (17, 'samcheok-onsite'),
  (18, 'samcheok-onsite')
) AS v(pid, sk) WHERE plants.plant_id = v.pid;

-- ── 나머지 78행: 현재 이름을 그대로 (공백 → '-', 소문자화는 하지 않는다 — 한글 보존) ──
UPDATE plants
   SET source_key = regexp_replace(trim(plant_name), '\s+', '-', 'g')
 WHERE source_key IS NULL;

COMMIT;
