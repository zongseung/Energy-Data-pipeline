-- ============================================================
-- P6: 구(舊) 테이블 DROP
--
-- 전제(완료): ① 수집기 5종 코어 직접쓰기 전환(이미지 재빌드 반영, 라이브 검증),
--             ② energy_hub FDW/백엔드 코어 전환(P5), ③ export_q1_dataset.py 코어 전환,
--             ④ Grafana 14패널 코어 전환, ⑤ pg_dump 백업(backups/old_tables_20260613.sql.gz).
-- 데이터는 plants/generation 코어에 전량 이전됨(중복/시각붕괴/멀티터빈 교정 포함).
--
-- 롤백: 백업 gz를 psql로 복원.
-- ============================================================

BEGIN;
DROP TABLE IF EXISTS nambu_generation;
DROP TABLE IF EXISTS nambu_plants;
DROP TABLE IF EXISTS namdong_generation;
DROP TABLE IF EXISTS namdong_plants;
DROP TABLE IF EXISTS wind_namdong;
DROP TABLE IF EXISTS wind_seobu;
DROP TABLE IF EXISTS wind_hangyoung;
COMMIT;

-- 고아 트리거 함수 정리 (트리거는 테이블과 함께 제거됨; 함수 본체만 잔존)
DROP FUNCTION IF EXISTS trg_nambu_to_generation();
DROP FUNCTION IF EXISTS trg_namdong_to_generation();
DROP FUNCTION IF EXISTS trg_wind_namdong_to_generation();
DROP FUNCTION IF EXISTS trg_wind_seobu_to_generation();
DROP FUNCTION IF EXISTS trg_wind_hangyoung_to_generation();
DROP FUNCTION IF EXISTS _ensure_plant(varchar, varchar, varchar, varchar, varchar);

-- 확인: 구테이블 잔존 0, 코어 유지
SELECT 'remaining_old_tables' AS k, count(*)::text AS v FROM pg_tables
WHERE schemaname='public'
  AND tablename IN ('nambu_generation','nambu_plants','namdong_generation','namdong_plants','wind_namdong','wind_seobu','wind_hangyoung')
UNION ALL SELECT 'plants', count(*)::text FROM plants
UNION ALL SELECT 'generation', count(*)::text FROM generation;
