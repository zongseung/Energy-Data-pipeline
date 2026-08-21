# 데이터 카탈로그 · 스키마 사전

뷰마다 무엇이 얼마나 있고 컬럼이 무슨 뜻인지 정리했습니다. 아래 목록에서 필요한 데이터를 고르면 그 페이지로 넘어갑니다.

{% hint style="info" %}
**수치 조회 시점: 2026-08-13.** 아래 행수·기간은 이 시점에 DB에서 직접 조회한 값입니다. 데이터는 계속 쌓입니다. 최신 값이 필요하면 각 절의 쿼리를 그대로 다시 실행하세요. 컬럼 의미는 DB의 `COMMENT`에도 달려 있습니다.

```sql
SELECT column_name, col_description('research.generation'::regclass, ordinal_position)
FROM information_schema.columns
WHERE table_schema='research' AND table_name='generation';
```
{% endhint %}

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>발전소와 발전량</strong></td><td>발전소 91기와 시간별 발전량. 함정이 가장 많습니다</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/generation">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/generation</a></td></tr><tr><td><strong>SMP 가격</strong></td><td>하루전시장·제주 실시간·가중평균</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/smp">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/smp</a></td></tr><tr><td><strong>기상 관측과 예보</strong></td><td>ASOS 실측 95개 지점과 동네예보 3종</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/weather">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/weather</a></td></tr><tr><td><strong>전력수요와 수급</strong></td><td>전국·제주 계통과 지역난방 열수요</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/demand">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/demand</a></td></tr><tr><td><strong>국제유가</strong></td><td>브렌트·WTI 시간별 OHLCV</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/oil">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/oil</a></td></tr><tr><td><strong>송배전망</strong></td><td>변전소·송전선로 좌표와 한전 접속 여유용량</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/grid">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/grid</a></td></tr></tbody></table>

***

## 전체 목록

접속은 **pv DB 계정 하나로 끝납니다.** 수요·수급 5종과 송배전망 3종은 물리적으로 다른 DB에 있지만 FDW로 연결돼 있어 발전량과 같은 쿼리에서 조인할 수 있습니다.

### 발전소와 발전량

| 뷰                     | 내용                  | 행수        | 기간                       | 갱신                      |
| --------------------- | ------------------- | --------- | ------------------------ | ----------------------- |
| `research.plants`     | 발전소 마스터 91기 + 품질 등급 | 91        | —                        | 수집기가 신규 발견 시            |
| `research.generation` | 시간별 발전량 (시간 보정 완료)  | 3,246,264 | 2013-01-01 \~ 2026-08-11 | 태양광 매일 09:30 / 그 외 월 1회 |

### SMP 가격

| 뷰                            | 내용               | 행수      | 기간                       | 갱신           |
| ---------------------------- | ---------------- | ------- | ------------------------ | ------------ |
| `research.smp_hourly`        | 하루전시장 시간별 SMP    | 367,224 | 2001-05-01 \~ 2026-08-11 | 매일 09:00     |
| `research.smp_realtime_jeju` | 제주 실시간시장 15분 SMP | 78,912  | 2024-03-01 \~ 2026-05-31 | **원천 공표 중단** |
| `research.smp_weighted_avg`  | 가중평균 SMP (일/월/연) | 16,137  | —                        | 매일           |

### 기상 관측과 예보

| 뷰·함수                    | 내용                   | 행수        | 기간                       | 갱신       |
| ----------------------- | -------------------- | --------- | ------------------------ | -------- |
| `research.weather_asos` | ASOS 시간별 관측 95개 지점   | 4,743,936 | 2019-01-01 \~ 2026-08-11 | 매일 09:00 |
| `research.forecast()`   | 기상청 동네예보 3종 (NAS 직독) | 적재 없음     | 2023-01 \~ 2025-06       | 정적       |

### 국제유가

| 뷰                     | 내용                | 행수    | 기간                                    | 갱신     |
| --------------------- | ----------------- | ----- | ------------------------------------- | ------ |
| `research.oil_hourly` | 브렌트·WTI 시간별 OHLCV | 9,116 | WTI 2026-01-07 \~ / 브렌트 2026-03-05 \~ | 매시 05분 |

### 전력수요와 수급

| 뷰                               | 내용              | 행수        | 기간                       | 갱신        |
| ------------------------------- | --------------- | --------- | ------------------------ | --------- |
| `research.demand_5min`          | 전국 계통 수급 5분     | 1,324,181 | 2014-01-01 \~ 2026-08-13 | **10분마다** |
| `research.jeju_supply_demand`   | 제주 계통 수급 5분     | 589,060   | 2021-01-01 \~ 2026-08-13 | **10분마다** |
| `research.demand_weather_1h`    | 전국 수요 × 기상 시간별  | 6,112,742 | 2019-01-01 \~ 2026-08-11 | 매시        |
| `research.heat_demand`          | 지역난방 열수요 19개 지사 | 499,301   | 2021-01-01 \~ 2023-12-31 | 없음 (완결)   |
| `research.heat_demand_location` | 열수요 지사 위치       | 19        | —                        | 없음        |

### 송배전망

| 뷰                      | 내용                     | 행수        | 기간          | 갱신      |
| ---------------------- | ---------------------- | --------- | ----------- | ------- |
| `research.substations` | OSM 변전소 위치 (좌표)        | 1,185     | —           | 없음 (정적) |
| `research.power_lines` | OSM 송전선로 (전압·길이)       | 4,685     | —           | 없음 (정적) |
| `research.kepco_grid`  | 한전 배전망 접속 여유용량 (주소 단위) | 3,611,724 | 2026-03 스냅샷 | 없음 (정적) |

***

## 모든 뷰에 공통으로 걸리는 규약

어느 페이지로 가든 아래 세 가지는 먼저 알아야 합니다.

### 시간은 전부 KST 구간시작입니다

`timestamp` 값 `09:00`은 `[09:00, 10:00)` 구간을 뜻합니다. 원천(KPX·발전사)은 대부분 hour-ending 표기를 씁니다. "구간이 끝난 시각"을 라벨로 삼는 방식인데, 뷰가 이미 구간시작으로 옮겨 내보냅니다. **직접 시프트하지 마세요.**

보정하지 않은 예외가 셋 있고 각 페이지에 표시해 뒀습니다.

| 대상            | 이유                                              |
| ------------- | ----------------------------------------------- |
| 풍력 발전량        | 원천 라벨이 구간시작인지 구간종료인지 확정 근거가 없습니다 — **±1시간 불확실** |
| ASOS 일사량      | 기상청 공식 문구를 찾지 못해 추정 단계                          |
| demand·열수요 계열 | pv DB만큼 감사하지 못했습니다                              |

### 등급 이름은 값 그대로 써야 합니다

`data_quality`는 한국어 값입니다. `정상`·`시간별무효`·`전면무효`·`미검증` 네 가지입니다. 영어로 번역하면 조회되지 않습니다.

특히 **`미검증`은 "품질이 나쁘다"가 아니라 "이 감사가 태양광만 봤다"는 뜻**입니다. 비태양광 46기가 여기 해당합니다 — 걸러낼 근거가 없다는 것과 정상이라는 것은 다릅니다.

### 필터를 덧붙이기 전에 뷰가 이미 거른 것을 확인하세요

`research.generation`은 시간별로 믿을 수 없는 구간을 **이미 잘라낸 상태**입니다. 여기에 `data_quality = '정상'`이나 `is_aggregate = false`를 더 얹으면 멀쩡한 데이터가 사라집니다. 이유는 발전소와 발전량 페이지에 있습니다.

***

## 이 카탈로그를 읽는 순서

처음이라면 발전소와 발전량부터 읽으세요. 함정이 가장 많고 다른 페이지가 조인하는 대상도 대부분 이 두 뷰입니다.

조회 방법은 직접 SQL과 LLM·MCP 두 페이지에 따로 있습니다.
