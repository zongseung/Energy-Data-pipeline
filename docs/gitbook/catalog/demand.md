# 전력수요와 수급

전국·제주 계통 수급과 지역난방 열수요. 물리적으로는 demand DB에 있지만 FDW로 연결돼 있어 pv DB 계정 하나로 발전량과 함께 조회한다.

---

## 수요·수급 데이터 (demand DB, FDW 경유)

**따로 접속할 필요 없다.** 아래 5개는 물리적으로 demand DB에 있지만 pv DB의
`research` 스키마로 FDW 연결돼 있어 **pv DB 계정 하나로 발전량과 함께 조회하고
조인까지 할 수 있다.** 복제본이 아니라 라이브 프록시라 값도 항상 최신이다.

```sql
-- 발전량(시간별)과 전국 수요(5분)를 한 쿼리에서 조인 — 접속 전환 없이 그대로 된다
SELECT g.timestamp, round(sum(g.gen_kwh)::numeric,0) AS solar_kwh,
       round(avg(d.current_demand)::numeric,0) AS demand_mw
FROM research.generation g
JOIN research.demand_5min d
  ON d.timestamp >= g.timestamp AND d.timestamp < g.timestamp + INTERVAL '1 hour'
WHERE g.fuel_type = 'solar'
  AND g.timestamp >= '2026-08-01' AND g.timestamp < '2026-08-02'
  AND d.timestamp >= '2026-08-01' AND d.timestamp < '2026-08-02'   -- ← 반드시 양쪽에
GROUP BY g.timestamp ORDER BY g.timestamp;
```

> **FDW 조인 주의 — 기간 조건을 양쪽에 걸어라.** 위 쿼리에서 `d.timestamp` 조건을
> 빼면 PostgreSQL이 원격 테이블 132만 행을 통째로 끌어와 2분이 지나도 안 끝난다.
> 조인 조건만으로는 범위가 원격까지 전달되지 않는다. 로컬 뷰와 FDW 테이블을 조인할
> 때는 **양쪽 테이블 각각에** WHERE 로 기간을 명시하는 것이 규칙이다.

이 계열의 시간 규약은 pv DB만큼 감사되지 않았다. **근거가 없는 컬럼은
시프트하지 않았다** — 아래 "무보정" 표기는 게으름이 아니라 확정 근거가
생기기 전까지 임의로 손대지 않겠다고 일부러 내린 결정이다.

### research.demand_5min — 전국 계통 수급 (5분)

| 항목 | 값 |
|---|---|
| 행수 | 1,324,085 |
| 기간 | 2014-01-01 ~ 2026-08-13 |
| 갱신 주기 | 매 10분 |
| 단위 | MW (`reserve_rate`만 %) |

{% hint style="success" %}
`research` 스키마에서 가장 최신인 계열이다. 10분마다 적재돼 조회 시점 기준
20분 전 데이터까지 들어와 있다.
{% endhint %}

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
| 행수 | 588,964 |
| 기간 | 2021-01-01 ~ 2026-08-13 (최신 갱신 정상) |
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
| 기간 | 2021-01-01 ~ 2023-12-31 (3년, 완결) |
| 지사 수 | 19 (전 지사 동일 기간 26,279행씩) |
| 갱신 주기 | 없음 — **완결된 정적 데이터셋** |

> **끊긴 게 아니라 이게 전량이다.** 19개 지사 모두 2021-01-01 ~ 2023-12-31
> 구간에 정확히 26,279행씩 들어 있다. 수집이 중간에 멈춰 잘린 게 아니라
> 처음부터 이 3년치가 데이터셋 전체다. 최신 데이터를 기다리지 말고 그대로 쓰면 된다.
> (다른 뷰와 달리 이 저장소에 수집 코드가 없다 — 외부에서 한 번 유입된 자료다.)

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
| 행수 | 6,110,466 |
| 기간 | 2019-01-01 ~ 2026-08-11 |
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
