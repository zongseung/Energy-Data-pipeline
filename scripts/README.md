# scripts/

운영·마이그레이션 보조 스크립트.

## 루트 (상시 사용 / 외부 참조)
- `backup_pv_db.sh` / `restore_pv_db.sh` — pv-data-postgres(5436) → NAS 백업/복원
- `init_wind_tables.py` — 풍력 테이블 초기화

## migrations/ (일회성·기록용 — 상시 실행 안 함, 재현/증적용 보존)
- `schema_migration.py` — plants/generation 코어 마이그레이션 (P1~P3, 멱등 재실행 안전)
- `p4_dual_write_triggers.sql` — 구 테이블 → generation dual-write 트리거 (DB에 적용됨, LIVE)
- `p_unify_koen_operator.sql` — KOEN operator 통일 SQL

> migrations/ 는 직접 실행할 일이 거의 없다. 삭제 금지(마이그레이션 증적·라이브 트리거 정의).
