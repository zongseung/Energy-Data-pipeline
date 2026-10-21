-- ⚠️⚠️ 적용 완료 (2026-10-21). **재실행하면 데이터가 깨진다** — 절대 다시 돌리지 말 것.
--     이 파일은 멱등이 아니다. 자세한 건 ../README.md 의 '재실행하면 데이터가 깨지는 것' 참고.
-- P9-7: 남부 backfill 5계열의 1시간 시프트 교정 (2026-10-21)
--
-- 대상: 파일데이터에 없어 재적재로 교정하지 못한 5계열 135,408행.
--   9988 신인천 북측부지 · 9989 신인천 1·2단계 주차장 · 9985 신인천 주차장 ·
--   9987 부산역 선상주차장(2023-10 종료) · 9979 이천D(2023-04 종료)
--
-- 근거: 같은 발전소인데 source 에 따라 태양광 피크가 갈린다 —
--   source='api'(현재 코드, 0-based) 피크 12시 / source='backfill'(옛 1-based) 피크 13시.
--   값은 맞고 시각만 +1시간 밀렸으므로 다시 받지 않고 집합째로 당긴다.
--
-- 행별 UPDATE 는 불가능하다: 연속 시간대라 각 행을 -1h 하면 바로 앞 행 자리와 겹쳐
--   PK 충돌이 135,336건 난다. 임시 테이블로 떠서 지우고 다시 넣는다.
--
-- 되돌리기: data/backup/nambu_backfill_before_shiftfix_20261021.csv (135,408행)
BEGIN;

CREATE TEMP TABLE _shift AS
SELECT g.timestamp - interval '1 hour' AS ts, g.plant_id, g.gen_kwh
  FROM generation g JOIN plants p USING (plant_id)
 WHERE p.operator = 'nambu' AND g.source = 'backfill';

DELETE FROM generation g
 USING plants p
 WHERE g.plant_id = p.plant_id AND p.operator = 'nambu' AND g.source = 'backfill';

INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
SELECT ts, plant_id, gen_kwh, 'backfill-fixed' FROM _shift
    ON CONFLICT (timestamp, plant_id)
    DO UPDATE SET gen_kwh = EXCLUDED.gen_kwh, source = EXCLUDED.source;

COMMIT;
