# 마이그레이션

## 폴더 규칙

```
ops/sql/migrations/
├── README.md          ← 이 파일. 적용 이력
├── <새 파일>.sql       ← 아직 안 돌린 것만 여기 둔다
└── applied/           ← 적용 끝난 것. 다시 돌리지 않는다
```

**루트가 비어 있으면 적용 대기 중인 마이그레이션이 없다는 뜻이다.**
새로 만들면 루트에 두고, DB 에 적용한 뒤 `applied/` 로 옮긴다.

```bash
docker exec -i pv-data-postgres psql -U pv -d pv -v ON_ERROR_STOP=1 < ops/sql/migrations/<파일>.sql
git mv ops/sql/migrations/<파일>.sql ops/sql/migrations/applied/
```

`-v ON_ERROR_STOP=1` 을 꼭 붙인다. 없으면 중간 문장이 실패해도 나머지가 계속 돈다.
그리고 **`docker exec` 에 `-i` 를 빠뜨리면 stdin 이 전달되지 않아 아무 일도 없이 끝난다**
(2026-10-21 에 실제로 겪었다 — 출력이 비어 있으면 이걸 의심할 것).

## ⚠️ 재실행하면 데이터가 깨지는 것

아래 둘은 **멱등이 아니다.** `for f in applied/*.sql; do psql < $f; done` 같은 일괄 실행은 절대 금지.

| 파일 | 재실행하면 |
|---|---|
| `applied/p9_fix_backfill_shift.sql` | 시각을 **또 1시간 당긴다** — 2시간 밀림 |
| `applied/p9_jangheung_unit_fix.sql` | **또 1000 으로 나눈다** — 값이 1/1,000,000 |

나머지는 멱등이다(`UPDATE`·`CREATE OR REPLACE VIEW`·조건부 `DELETE`).

## 적용 이력

번호가 `p9` 로 겹친다 — `p9_plant_region.sql`(2026-09, region 백필)은 **예전 P9** 이고,
나머지 `p9_*`(2026-10-21)은 **키 재설계 P9** 다. 혼동 주의.

### 2026-06 ~ 09 — 코어 스키마 수립

| 파일 | 한 일 |
|---|---|
| `schema_migration.py` | `plants`+`generation` 코어 DDL·마스터·백필 (P1~P3) |
| `p4_dual_write_triggers.sql` | 구테이블 INSERT → 코어 UPSERT 트리거 (P4, 테이블과 함께 제거됨) |
| `p_unify_koen_operator.sql` | `koen` → `namdong` 통일 |
| `p_remigrate_namdong_solar.sql` | 남동 태양광 자정붕괴 복구 (132,596 → 871,632행) |
| `p_remigrate_wind_hangyoung.sql` | 한경풍력 멀티터빈 SUM 재적재 |
| `p_drop_old_tables.sql` | 구테이블 7개 + 트리거함수 6개 DROP (P6) |
| `p7_standard_views.sql` | `v_generation_daily`/`v_generation_monthly` (P7) |
| `p8_time_convention_views.sql` | 시간 규약 보정을 `v_generation_hourly` 한 곳으로 (P8) |
| `p9_plant_region.sql` | `plants.region` 백필 |

### 2026-10-21 — 키 재설계와 데이터 교정

적용 순서대로다. 의존이 있어 번호를 섞어 돌리면 안 된다.

| 파일 | 한 일 |
|---|---|
| `p9_source_key.sql` | `source_key` 신설 · `plant_code`→`official_code` · `capacity_confidence` 제거 |
| `p9_source_key_values.sql` | 96행 `source_key` 채움 (남부 18 = 매핑표 슬러그, 나머지 78 = 이름 정규화) |
| `p9_source_key_constraint.sql` | `NOT NULL` + `UNIQUE (operator, source_key, unit_no)`, `plant_name` UNIQUE 해제 |
| `p9_data_fix.sql` | 한경 `operator`→`nambu` · 서부 장흥·화순 분절 병합 · NaN/-1→NULL · `plant_id 3` region→jeju |
| `p9_drop_nambu_shift.sql` | `v_generation_hourly` 의 남부 1시간 보정 CASE 제거 (파일 재적재로 원본이 올바라짐) |
| `p9_operator_unify.sql` | `ewp`→`dongseo` · `ekr`→`nongeochon` |
| `p9_jungbu_capacity.sql` | 중부 설비용량 6건 (소수력 3 + 1:1 태양광 3) |
| `p9_fix_backfill_shift.sql` | ⚠️ 남부 backfill 5계열 135,408행 시각 -1h (비멱등) |
| `p9_fuel_type_expand.sql` | 남동 `thermal` → `coal`·`combined`·`biomass` 재분류 |
| `p9_hourly_view_fuel_fix.sql` | 위 재분류로 깨진 남동 시간보정 조건을 "비태양광·비풍력" 으로 |
| `p9_yeosu_bundang.sql` | 여수 2기→`coal` 확정 · 분당 복합 10기 `capacity_mw` 비움(정격 15배 초과) |
| `p9_namdong_cumulative_outliers.sql` | 남동 누적값 혼입 12행 삭제 (영흥_6 9 · 영동_2 2 · 영동_1 1) |
| `p9_jangheung_unit_fix.sql` | ⚠️ 서부 장흥풍력 16,776행 Wh→kWh (비멱등) |
| `p9_research_plants_sourcekey.sql` | `research.plants` 판정을 `plant_id` 하드코딩 → `source_key` 기준으로. 신규 설비 `미검증` 처리, 동해바이오·영흥 1·2 기록 |
| `p9_jeju_pv1_zeros.sql` | 중부 `jeju-pv-1` 2021년 이후 0 행 48,732개 삭제 (2018~2020 실적은 보존) |

배경·근거는 각 파일 상단 주석에 있다. 관련 메모리 — `pv-schema-refactor-state`,
`nambu-filedata-ingest`, `jungbu-ingest`.
