# 데이터 카탈로그 · 스키마 사전

뷰마다 "무엇이 얼마나 있는가"(행수·기간·지점 수·갱신주기·알려진 결측)와
"컬럼이 무슨 뜻인가"(의미·단위·시간 규약)를 함께 둔다. 두 문서를 오갈 필요가
없게 하기 위해서다.

> **수치 조회 시점: 2026-08-11.** 아래 행수·기간은 이 시점에 DB에서 직접
> 조회한 값이다. 데이터는 계속 쌓이므로 최신 값이 필요하면 각 절의 쿼리를
> 그대로 다시 실행하라. 컬럼 의미는 DB의 `COMMENT`로도 확인 가능하다:
>
> ```sql
> SELECT column_name, col_description('research.generation'::regclass, ordinal_position)
> FROM information_schema.columns
> WHERE table_schema='research' AND table_name='generation';
> ```

---

## pv DB

### research.plants — 발전소 마스터 + 데이터 품질 등급

발전소당 1행(시계열 아님). 다른 발전 수집기가 신규 발전소를 발견할 때 함께
갱신되며 별도 배치 스케줄은 없다.

| 항목 | 값 |
|---|---|
| 행수 | 91 (태양광 45 · 화력 24 · 연료전지 8 · 풍력 10 · 해양소수력 4) |
| region | `mainland` / `jeju` — **SMP 뷰의 region(`land`/`jeju`/`unified`)과 코드값이 다르다.** SMP와 지역으로 조인하려면 `mainland → land`로 직접 매핑해야 한다 |

**컬럼**

| 컬럼 | 의미 | 비고 |
|---|---|---|
| `plant_id` | 발전소 고유 ID | **조인 키는 이것을 써라.** `plant_name`은 유일하지 않다(삼척소내 3기, 부산본부 2기 등 동명이 존재) |
| `plant_name` | 발전소명 | |
| `unit_no` | 호기 번호 | `plant_name`과 함께 써야 유일 식별 가능 |
| `operator` | 사업자: `nambu`/`namdong`/`seobu`/`hangyoung`/`ekr` | |
| `fuel_type` | 발전원: `solar`/`thermal`/`fuel_cell`/`hydro`/`wind` | |
| `region` | `mainland`/`jeju` | 위 표 참고 |
| `capacity_mw` | 정격용량(MW) | 아래 "capacity_mw 보유율" 참고. 공시값·추정값이 섞여 있어 정밀도 보장 안 함 |
| `lat`, `lon` | 위도·경도 | 아래 "좌표" 항목 참고 |
| `is_aggregate` | 합계 계열 여부 | 아래 "함정 — is_aggregate" 참고 |
| `data_quality` | `정상`/`시간별무효`/`전면무효`/`미검증` | 아래 "함정 — data_quality" 참고 |
| `hourly_valid_from` | 시간별 값을 신뢰할 수 있는 시작일 (NULL이면 근거 없음) | 아래 "함정 — hourly_valid_from" 참고 |
| `daily_valid_from` / `daily_valid_to` | 일별 합계를 신뢰할 수 있는 구간 | `시간별무효`라도 일별 합계는 이 구간에서 유효할 수 있다 |
| `data_quality_note` | 등급 판정 사유 한 줄 | 예: ESS 연계 계량 추정, 정오 억제 등 |

**data_quality 분포**

| 등급 | 기수 | 의미 |
|---|---|---|
| 정상 | 33 | 시간별·일별 모두 사용 가능 (태양광 일주곡선 검사 통과) |
| 시간별무효 | 10 | 시간별 분포를 쓰면 안 됨. 일별 합계는 `daily_valid_from/to` 확인 후 사용 |
| 전면무효 | 2 | 일별 합계조차 신뢰 불가 (`탑선태양광_1`, `탑선태양광_3` — 원천이 월 단위 값을 일수로 나눠 매일 같은 값을 보냄) |
| 미검증 | 46 | 비태양광 전체. **"깨졌다"가 아니라 "이 감사가 태양광만 봤다"는 뜻** — 정상이라는 보증이 아니다 |

**capacity_mw 보유율** (fuel_type별, 91기 중 41기만 값 있음)

| fuel_type | 전체 | capacity_mw 있음 | 보유율 |
|---|---|---|---|
| solar | 45 | 3 | 7% |
| thermal | 24 | 24 | 100% |
| fuel_cell | 8 | 6 | 75% |
| hydro | 4 | 4 | 100% |
| wind | 10 | 4 | 40% |

태양광 45기 중 42기는 `capacity_mw`가 NULL이다. 발전량을 설비용량으로 나눠
이용률을 구하는 분석은 태양광에서는 거의 불가능하다고 보면 된다.

**좌표(lat/lon)**

| 구분 | 전체 | 좌표 보유 | 검증 여부 |
|---|---|---|---|
| 태양광 | 45 | 44 (`율치태양광`만 NULL) | 44기 전부 검증됨 — 단 부지 POI·행정구역 중심의 **근사값(±2km 수준)**이지 정밀 지번 좌표가 아니다. ASOS 최근접 지점 매칭에는 충분하나 그 이상 정밀도가 필요한 분석에는 재검증 필요 |
| 비태양광(화력·연료전지·수력·풍력) | 46 | 40 | **검증 대상이 아니었다 — 신뢰도 불명.** 좌표가 있다고 해서 정확하다는 뜻이 아니다 |

`capacity_confidence` 같은 신뢰도 컬럼은 이 뷰에 없다(의미가 오염된
컬럼이라 배포에서 제외했다). 좌표를 쓸 때는 위 표의 정성적 설명이 전부다.

---

### research.generation — 시간별 발전량 (시간 보정 완료)

| 항목 | 값 |
|---|---|
| 행수 | 3,699,768 |
| 기간 | 2013-01-01 ~ 2026-08-10 |
| 발전소 수 | 91 (plants 전체) |
| 단위 | `gen_kwh` = **kWh** (아래 "함정 — 단위" 참고) |

**갱신 주기** (발전원마다 다르다 — 하나의 스케줄이 아니다)

| 계열 | 스케줄 |
|---|---|
| 남부 태양광 (`operator='nambu'`) | 매일 09:30 |
| 남동 태양광 (`operator='namdong', fuel_type='solar'`) | 매월 10일 10:00 |
| KOEN 비태양광 (`operator='namdong'`, 화력·연료전지·해양소수력) | 매월 10일 10:00 |
| 남동 풍력 (`operator='namdong', fuel_type='wind'`) | 매월 10일 11:00 |
| 서부·한경 풍력 (`operator IN ('seobu','hangyoung')`) | **정기 수집 없음.** 서부는 2023-06-30, 한경은 2025-03-01 이후 갱신되지 않은 1회성 적재분이다 |
| 영암·율치 태양광 (`operator='ekr'`) | 매년 1월 8일 04:00 |

**컬럼**

| 컬럼 | 의미 | 비고 |
|---|---|---|
| `plant_id`, `plant_name`, `unit_no`, `operator`, `fuel_type` | `research.plants`와 동일 | 조인 없이 바로 쓰라고 펼쳐 둔 것 |
| `timestamp` | 발전 시각 | **KST 구간시작** (아래 "함정 — 시간 규약" 참고) |
| `gen_kwh` | 해당 1시간 구간 발전량 | **단위는 kWh** (아래 "함정 — 단위" 참고) |
| `is_aggregate`, `data_quality`, `hourly_valid_from` | `research.plants`에서 그대로 가져옴 | 아래 함정 참고 |

#### 함정 — 시간 규약 (KST 구간시작)

모든 `timestamp`는 **KST 구간시작**으로 통일되어 있다. `09:00` 값은
`[09:00, 10:00)` 구간의 발전량이라는 뜻이다. 원천(KPX/발전사)은 대부분
1~24시로 "구간이 끝난 시각"을 라벨로 쓰는 hour-ending 표기인데, 뷰가
이미 구간시작으로 시프트해서 내보낸다 — **직접 시프트하지 마라.** 단
**풍력(`fuel_type='wind'`)은 예외다.** 원천 라벨이 구간시작인지 구간종료인지
확정할 근거가 없어 보정하지 않았다 — **±1시간 불확실**하다고 보고 다뤄라.

#### 함정 — gen_kwh 단위

KOEN 비태양광(화력·연료전지·해양소수력) 계열은 **원천 CSV 헤더가
"발전량(MWh)"로 적혀 있지만 실제 저장된 값은 kWh다.** 헤더 표기만 보고
`×1000`을 하면 발전량이 1000배로 부풀려진다. 모든 `fuel_type`에서
`gen_kwh`는 예외 없이 kWh 단위다.

#### 함정 — data_quality 4단계

`research.plants`의 등급을 그대로 물려받는다. 특히 **`미검증`(비태양광
46기)은 "품질이 나쁘다"가 아니라 "이 등급을 매긴 감사가 태양광만
검사했다"는 뜻**이다. 비태양광 데이터를 함부로 걸러내지 마라 — 걸러낼
근거가 없다는 것과 정상이라는 것은 다르다. 반대로 태양광의 `시간별무효`·
`전면무효` 12기는 명확한 근거로 등급이 매겨졌으니 시간별 분석에서
제외하는 것이 맞다.

#### 함정 — hourly_valid_from

`data_quality='시간별무효'`인 발전소 중 **영흥태양광 #3 계열 3기
(`plant_id` 20/29/32)는 2025-07-01부터 시간별 값도 정상**이다.
`data_quality`만 보고 `시간별무효`를 통째로 걸러내면 **13개월치(2025-07 ~
현재)의 정상 데이터를 버리게 된다.** 반드시 `hourly_valid_from`을 함께
확인하라:

```sql
-- 시간별 분석에 쓸 수 있는 행만 남기는 올바른 필터
WHERE (data_quality = '정상')
   OR (data_quality = '시간별무효' AND "timestamp" >= hourly_valid_from)
```

#### 함정 — is_aggregate (이중계상)

`plant_id 140`(`영암태양광_합계`)은 `plant_id 141`(영암1차)·`142`(영암2차)의
**합계 계열**이다. 현재는 140이 2019~2021년, 141·142가 2022~2025년만
적재돼 있어 기간이 겹치지 않지만, 향후 백필로 겹치면 즉시 이중계상된다.
전체 발전량을 합산할 때는 항상 `WHERE is_aggregate = false`를 넣어라.

---

### research.smp_hourly — 하루전시장 시간별 SMP

| 항목 | 값 |
|---|---|
| 행수 | 367,176 |
| 기간 | 2001-05-01 ~ 2026-08-10 |
| region | `unified`(2001-05~2009-12, 76,008행) / `land`·`jeju`(2010-01~현재, 각 145,584행) |
| 갱신 주기 | 매일 09:00 (전날 하루전시장 시간별 SMP) |
| 단위 | 원/kWh |

| 컬럼 | 의미 |
|---|---|
| `timestamp` | KST 구간시작 (수집 단계에서 이미 hour-ending → 구간시작 변환 완료) |
| `region` | `unified`(2010년 이전 단일시장) / `land`(육지) / `jeju`(제주) |
| `price` | SMP (원/kWh) |

2010-01-01 이전은 육지·제주가 분리되지 않은 단일 시장이었다
(`region='unified'`). 그 이전 시기를 다루는 분석에서 `land`/`jeju`로
필터링하면 데이터가 통째로 빠진다.

---

### research.smp_realtime_jeju — 제주 실시간시장 15분 SMP

| 항목 | 값 |
|---|---|
| 행수 | 78,912 |
| 기간 | 2024-03-01 ~ 2026-05-31 |
| 갱신 주기 | 매일 19:00 (전일 확정치) |

**알려진 결측**: 스케줄은 매일 갱신이지만, 조회 시점(2026-08-11) 기준
실제 최신 데이터는 **2026-05-31**로 약 2개월 이상 갭이 있다. 이 뷰를 쓰기
전에 `SELECT max("timestamp") FROM research.smp_realtime_jeju;`로 최신
시점을 먼저 확인하라.

| 컬럼 | 의미 |
|---|---|
| `timestamp` | KST 구간시작. 15분 단위(1시간=4구간) |
| `region` | 항상 `jeju` |
| `price` | SMP (원/kWh). **음수 가격이 정상적으로 발생한다** — 이상치로 걸러내지 말 것 |
| `is_confirmed` | `false`면 잠정치(이후 확정치로 갱신될 수 있음) |

---

### research.smp_weighted_avg — 가중평균 SMP (일/월/연)

| 항목 | 값 |
|---|---|
| 갱신 주기 | daily 행: 매일 09:00 / monthly·yearly 행: 매월 2일 07:00 |

| period_type | price_type | 행수 | 기간 |
|---|---|---|---|
| daily | smp | 15,299 | 2001-05-01 ~ 2026-08-10 |
| monthly | smp | 693 | 2001-04-01 ~ 2026-04-01 |
| monthly | blmp | 69 | 2001-04-01 ~ 2006-12-01 |
| yearly | smp | 66 | 2001-01-01 ~ 2025-01-01 |
| yearly | blmp | 6 | 2001-01-01 ~ 2006-01-01 |

| 컬럼 | 의미 |
|---|---|
| `period_type` | `daily`/`monthly`/`yearly` |
| `period` | 집계 기준일 (월별이면 해당 월 1일, 연별이면 해당 연도 1월 1일) |
| `region` | `land`/`jeju` 등 |
| `price_type` | `smp`(계통한계가격) / `blmp`(기저한계가격) |
| `weighted_avg` | 가중평균 가격 (원/kWh) |

**함정**: `price_type='blmp'`는 **2001~2006년, 육지(region 관련값)만
존재**한다. 최근 데이터나 제주 BLMP를 찾으면 결과가 빈 것이 정상이다
(결측이 아니라 애초에 그 시기 이후 BLMP 자체가 발행되지 않는다).

---

### research.weather_asos — ASOS 시간별 기상

| 항목 | 값 |
|---|---|
| 행수 | 4,739,376 |
| 기간 | 2019-01-01 ~ 2026-08-10 |
| 지점 수 | 95 (일사 관측 63 / 미관측 32) |
| 갱신 주기 | 매일 09:00 (전날 기상 데이터) |

| 컬럼 | 의미 | 단위 |
|---|---|---|
| `timestamp` | 관측 시각 | 기온/습도는 KST, 시간 라벨 규약 확인됨. **일사량만 시간 라벨이 아직 "추정" 단계**(아래 참고) |
| `station_name` | 관측소명 | |
| `temperature` | 기온 | ℃ |
| `humidity` | 습도 | % |
| `solar_radiation` | 일사량 | MJ/m² |
| `has_solar_sensor` | 일사 관측 지점 여부 | 아래 "함정" 참고 |

#### 함정 — has_solar_sensor로 판별하라

일사 관측 지점은 **63개**다(`has_solar_sensor=true`). **`solar_radiation IS
NOT NULL`로 지점을 세면 안 된다** — 실제로 세보면 65개가 나온다. 미관측
지점 중 `성산`·`제천` 2곳에 각각 **1건씩, 값 0인 이상치**가 섞여 있기
때문이다(정상 관측 지점은 평균 23,382행의 값을 갖는 것과 비교하면 명백한
이상치). 관측 지점을 판별할 때는 반드시 `has_solar_sensor`를 써라:

```sql
-- 틀린 방법 (65개로 잘못 나옴)
SELECT DISTINCT station_name FROM research.weather_asos WHERE solar_radiation IS NOT NULL;

-- 맞는 방법 (63개)
SELECT DISTINCT station_name FROM research.weather_asos WHERE has_solar_sensor;
```

`has_solar_sensor=false`인 32개 지점은 `solar_radiation`이 항상 NULL이며
이것은 결측이 아니라 애초에 관측하지 않는 것이 정상이다.
`has_solar_sensor=true`인 지점도 야간이거나 관측 시작 이전 기간에는
NULL이 나올 수 있다(관측 시작 시점은 지점마다 다르다 — 특정 지점을 쓸 때는
`WHERE station_name = 'X' AND solar_radiation IS NOT NULL`로 실제 값 존재
구간을 먼저 확인하라).

일사량의 시간 라벨(구간시작/구간종료)은 기상청 공식 문구를 찾지 못해
**아직 추정 단계**이며 보정을 적용하지 않았다. 시각 정밀도가 중요한
분석(예: 특정 시각의 순간 일사량과 발전량 1:1 대조)에는 최대 1시간의
오차 가능성을 감안하라.

---

## demand DB

이 DB의 시간 규약은 pv DB만큼 감사되지 않았다. **근거가 없는 컬럼은
시프트하지 않았다** — 아래 "무보정" 표기는 게으름이 아니라 확정 근거가
생기기 전까지 임의로 손대지 않겠다는 의도적 결정이다.

### research.demand_5min — 전국 계통 수급 (5분)

| 항목 | 값 |
|---|---|
| 행수 | 1,322,047 |
| 기간 | 2014-01-01 ~ 2026-08-05 |
| 갱신 주기 | 매 10분 |
| 단위 | MW (`reserve_rate`만 %) |

**알려진 결측**: 조회 시점(2026-08-11) 기준 최신 데이터가 2026-08-05로
약 6일 갭이 있다. 최신 시점은 `SELECT max("timestamp")`로 먼저 확인하라.

| 컬럼 | 의미 | 비고 |
|---|---|---|
| `timestamp` | KPX 원천 "기준일시"를 그대로 적재 | 구간 라벨인지 순시 시각인지 확정 근거가 없어 무보정 |
| `current_demand` | 현재수요 (MW) | |
| `current_supply` | **공급능력** (MW) | **컬럼명 함정**: 이름은 `current_supply`지만 값은 항상 `current_demand`보다 훨씬 크다(예: 105,616 vs 74,850) — "현재 공급되는 양"이 아니라 시스템이 낼 수 있는 총 공급능력이다 |
| `supply_capacity` | **최대예측수요** (MW) | **컬럼명 함정**: 이름은 `supply_capacity`(공급능력처럼 보임)지만 실제로는 그날의 최대예측수요다 — 같은 날 안에서는 값이 일정하게 유지된다(위 예: 93,500이 반복). **공급능력이 필요하면 `current_supply`를, 최대예측수요가 필요하면 `supply_capacity`를 써라 — 이름과 의미가 뒤바뀌어 있다** |
| `supply_reserve` | 공급예비력 (MW) | `current_supply - current_demand`와 대략 일치 |
| `reserve_rate` | 공급예비율 (%) | `supply_reserve / current_demand × 100`과 대략 일치 |
| `operation_reserve` | 운영예비력 (MW) | |
| `is_holiday` | 공휴일 여부 | |
| `day_type` | 0=평일, 1=주말(토/일), 2=공휴일 | 공휴일이 최우선 판정(주말+공휴일이면 2) |

### research.jeju_supply_demand — 제주 계통 수급 (5분)

| 항목 | 값 |
|---|---|
| 행수 | 588,582 |
| 기간 | 2021-01-01 ~ 2026-08-11 (최신 갱신 정상) |
| 갱신 주기 | 매 10분 |
| 단위 | MW |

| 컬럼 | 의미 |
|---|---|
| `timestamp` | 제주 실시간 원천 시각 그대로(KST). 무보정 |
| `supply_mw` | 공급능력 |
| `demand_mw` | 수요 |
| `renewable_total_mw` | 신재생 합계 |
| `solar_mw`, `wind_mw` | 태양광·풍력 발전량 |

### research.heat_demand — 지역난방 열수요 (시간별)

| 항목 | 값 |
|---|---|
| 행수 | 499,301 |
| 기간 | 2021-01-01 ~ 2023-12-31 |
| 지사 수 | 19 |
| 갱신 주기 | **없음.** 이 저장소에 적재 코드가 없는 외부 유입 데이터로, 2023-12-31 이후 갱신되지 않았다 |

| 컬럼 | 의미 | 단위 |
|---|---|---|
| `timestamp` | KST. 원천 라벨(구간시작/구간종료) 규약을 확인하지 못해 무보정 | |
| `branch` | 지사명 | `research.heat_demand_location.name`과 조인 |
| `heat_demand` | 열수요 | (원천 단위 그대로, 검증 안 됨) |
| `temperature`, `temperature_chill`, `humidity`, `wind_direction`, `wind_speed`, `rain_1h`, `rain_daily` | 동반 기상 관측 | |

### research.heat_demand_location — 열수요 지사 위치

| 항목 | 값 |
|---|---|
| 행수 | 19 (마스터 테이블) |

| 컬럼 | 의미 |
|---|---|
| `name` | 지사명. `research.heat_demand.branch`와 조인 |
| `address` | 주소 |
| `latitude`, `longitude` | 위경도 (검증 범위 불명) |

### research.demand_weather_1h — 시간별 수요 × 기상 (파생)

| 항목 | 값 |
|---|---|
| 행수 | 6,106,191 |
| 기간 | 2019-01-01 ~ 2026-08-04 |
| station_name 수 | 96 (아래 함정 참고) |
| 갱신 주기 | 10분마다 수집, 매시 정각 직후 시간 집계 갱신 |

| 컬럼 | 의미 |
|---|---|
| `timestamp` | **구간시작 KST(확정적)**. `demand_avg`는 `demand_5min`을 시간 단위로 평균한 값이라 버킷 경계는 확정적이지만, 5분 라벨 자체의 의미가 미확정이라 최대 5분 오차가 남을 수 있다 |
| `station_name` | ASOS 관측소명 |
| `temperature`, `humidity` | 기상 관측값 |
| `demand_avg` | 해당 1시간 구간 전국 수요(MW)의 5분값 평균 |
| `is_holiday`, `day_type` | `research.demand_5min`과 동일 |

**함정**: `station_name` 96개 중 하나는 실제 관측소가 아니라 **`UNKNOWN`
(88행, 2026-01-09~2026-05-31)**이다. 지점별 분석에서는
`WHERE station_name <> 'UNKNOWN'`으로 제외하라. 지점 수만큼 수요값이
반복되므로(지점 95~96개 × 같은 시각) 수요값만 필요하면 이 뷰 대신
`research.demand_5min`을 쓰는 것이 낫다.
