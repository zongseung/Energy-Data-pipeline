# 데모 쿼리 배터리 결과


**요약: 통과 11 / 실패 False / 육안 2 (전체 17)**


## agg-by-plant — ❌ 실패
- 질문: 2026년 7월 발전소별 태양광 발전량 상위 5곳을 표로 보여줘
- 판정: 필수 패턴 누락: group\s+by
- SQL1: `SELECT plant_id, plant_name, fuel_type, gen_kwh FROM research.generation WHERE fuel_type = 'solar' AND EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 7 ORDER BY gen_kwh DESC LIMIT 5;`
- 최종답(앞 300자): | plant_id | plant_name         | fuel_type | gen_kwh     |
|----------|--------------------|-----------|-------------|
| 26       | 고흥만 수상태양광  | solar     | 47953.752   |
| 26       | 고흥만 수상태양광  | solar     | 47899.488   |
| 26       | 고흥만 수상태양광  | solar     | 47361.468   |
| 26       | 고흥만 수상태양광  

## fuel-filter — ✅ 통과
- 질문: 태양광 발전량 데이터만 2026년 8월 1일부터 3일치 보여줘. 다른 연료는 빼고.
- 판정: -
- SQL1: `SELECT * FROM research.generation WHERE fuel_type = 'solar' AND timestamp >= '2026-08-01' AND timestamp <= '2026-08-03';`
- 최종답(앞 300자): | plant_id | plant_name | unit_no | operator | fuel_type | timestamp | gen_kwh | is_aggregate | data_quality | hourly_valid_from |
|----------|------------|---------|----------|-----------|-----------|---------|--------------|--------------|-------------------|
| 5        | 한국남부발전(주)_부산복합자재창고 태양광발전실

## period-bounds — ✅ 통과
- 질문: 2026년 7월 한 달간 태양광 총 발전량 알려줘
- 판정: -
- SQL1: `SELECT SUM(gen_kwh) AS total_solar_generation FROM research.generation WHERE EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 7 AND fuel_type = 'solar';`
- 최종답(앞 300자): 2026년 7월 한 달간 태양광 총 발전량은 **10,836,247.15 kWh**입니다.

다운로드 링크: [https://localhost:8098/export-951a2d2b.csv](http://localhost:8098/export-951a2d2b.csv)

이 데이터는 2026년 7월의 태양광 발전량을 나타내며, 전체 데이터는 위 링크에서 CSV 파일로 다운로드할 수 있습니다.

## aggregate-trap — ❌ 실패
- 질문: 2026년 전체 태양광 발전량 총합을 구해줘 (이중계상 없이)
- 판정: 필수 패턴 누락: is_aggregate
- SQL1: `SELECT SUM(gen_kwh) AS total_solar_generation FROM research.generation WHERE fuel_type = 'solar';`
- 최종답(앞 300자): 2026년 전체 태양광 발전량 총합은 **420,870,632.16 kWh**입니다.

다운로드 링크: [CSV 파일 다운로드](http://localhost:8098/export-944f2e74.csv)

이 데이터는 `research.generation` 뷰에서 태양광 연료 타입(`solar`)에 해당하는 행을 기반으로 계산되었습니다. 데이터는 1행만 포함되어 있으며, 전체 데이터는 위 링크에서 CSV 파일로 다운로드할 수 있습니다.

## weather-join — ❌ 실패
- 질문: 2026년 8월 5일 서울 기온과 시간별 전체 태양광 발전량을 같이 보여줘
- 판정: 마지막 SQL 이 에러로 끝남
- SQL1 ⚠에러: `SELECT timestamp, temperature, humidity, solar_radiation FROM research.weather_asos WHERE timestamp >= '2026-08-05' AND timestamp <= '2026-08-05' AND station_name = '서울'; SELECT * FROM research.generation WHERE fuel_type = 'solar' AND timestamp = '2026-08-05';`
- 에러: Error executing tool run_sql: 한 번에 하나의 SQL 문장만 실행할 수 있습니다. 세미콜론으로 여러 문장을 연결하지 마세요.
- 최종답(앞 300자): I'll execute the SQL query separately for each part of your request.

1. First, let's get the temperature and solar radiation data for Seoul on 2026-08-05:
```sql
SELECT timestamp, temperature, humidity, solar_radiation 
FROM research.weather_asos 
WHERE timestamp = '2026-08-05' AND station_name = '

## smp-daily — ✅ 통과
- 질문: 2026년 8월 10일 육지(land) SMP 시간별 가격을 보여줘
- 판정: -
- SQL1: `SELECT * FROM research.smp_hourly WHERE timestamp::date = '2026-08-10' AND region = 'land';`
- 최종답(앞 300자): 2026년 8월 10일 육지(land) SMP 시간별 가격 데이터는 아래와 같습니다:

| timestamp           | region | price  |
|---------------------|--------|--------|
| 2026-08-10T19:00:00 | land   | 178.49 |
| 2026-08-10T20:00:00 | land   | 174.14 |
| 2026-08-10T21:00:00 | land   | 174.87 |
| 2026-08-10T22:00:00 | land   | 175.6  |

## jeju-demand — ✅ 통과
- 질문: 가장 최근 제주 전력수급 상황(태양광·풍력 포함)을 알려줘
- 판정: -
- SQL1: `SELECT * FROM research.jeju_supply_demand ORDER BY timestamp DESC LIMIT 1;`
- 최종답(앞 300자): 가장 최근 제주 전력수급 상황(태양광·풍력 포함)은 다음과 같습니다:

| timestamp        | supply_mw | demand_mw | renewable_total_mw | solar_mw  | wind_mw  |
|------------------|-----------|-----------|--------------------|-----------|----------|
| 2026-08-13T09:35:00 | 1508.0   | 830.0    | 228.314           | 185.672  | 30.83

## csv-extract — ✅ 통과
- 질문: 2026년 8월 1일~10일 태양광 발전량 데이터를 csv로 줘
- 판정: -
- SQL1: `SELECT * FROM research.generation WHERE timestamp >= '2026-08-01' AND timestamp <= '2026-08-10' AND fuel_type = 'solar' ORDER BY timestamp;`
- 최종답(앞 300자): ```sql
SELECT * FROM research.generation WHERE timestamp >= '2026-08-01' AND timestamp <= '2026-08-10' AND fuel_type = 'solar' ORDER BY timestamp;
```

<data>
| plant_id | plant_name                                       | unit_no | operator | fuel_type | timestamp       | gen_kwh | is_aggregate | d

## korean-codes — ✅ 통과
- 질문: 데이터 품질이 정상인 발전소가 몇 개야?
- 판정: -
- SQL1: `SELECT COUNT(*) FROM research.plants WHERE data_quality = '정상';`
- 최종답(앞 300자): 데이터 품질이 정상인 발전소는 **33개**입니다.

```
SELECT COUNT(*)
FROM research.plants
WHERE data_quality = '정상';
```

## round-recovery — ✅ 통과
- 질문: 2026년 8월 육지 SMP 평균 가격을 소수점 1자리로 알려줘
- 판정: -
- SQL1: `SELECT round(avg(price)::numeric, 1) AS average_price FROM research.smp_hourly WHERE EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 8 AND region = 'land';`
- 최종답(앞 300자): 2026년 8월 육지 SMP 평균 가격은 **147.4**입니다.

다운로드 링크: [http://localhost:8098/export-cf915644.csv](http://localhost:8098/export-cf915644.csv)

이 링크를 사용하면 전체 데이터를 CSV 파일로 내려받을 수 있습니다. pandas로 읽을 때는 `read_csv(경로, parse_dates=['timestamp'], index_col='timestamp')` 처럼 시간 컬럼을 지정하라고 함께 안내하라.

```sql
SELECT round(

## hallucination-recovery — ✅ 통과
- 질문: power_plants 테이블에서 발전소 목록 5개만 보여줘
- 판정: -
- SQL1 ⚠에러: `SELECT * FROM research.power_plants LIMIT 5;`
- SQL2: `SELECT * FROM research.plants LIMIT 5;`
- 에러: Error executing tool run_sql: SQL 오류: relation "research.power_plants" does not exist
LINE 1: SELECT * FROM research.power_plants LIMIT 5;
                      ^
힌트: 존재하는 뷰는 다음이 전부다 — research.plants
- 최종답(앞 300자): 다음은 `research.plants` 테이블에서 조회한 발전소 목록 5개입니다:

| plant_id | plant_name              | unit_no | operator | fuel_type | region  | capacity_mw | lat    | lon    | is_aggregate | data_quality | hourly_valid_from | daily_valid_from | daily_valid_to | data_quality_note |
|----------|---------------------

## empty-future — 🖐 육안
- 질문: 2030년 1월 태양광 발전량을 알려줘
- 판정: 환각 숫자 없이 '데이터 없음'이라 답하는지 육안 확인
- SQL1: `SELECT gen_kwh FROM research.generation WHERE EXTRACT(YEAR FROM timestamp) = 2030 AND EXTRACT(MONTH FROM timestamp) = 1 AND fuel_type = 'solar';`
- 최종답(앞 300자): 2030년 1월의 태양광 발전량 데이터는 없습니다. 요청하신 조건에 부합하는 데이터가 존재하지 않아 결과가 빈 데이터 집합입니다.

## wind-caveat — 🖐 육안
- 질문: 2026년 7월 풍력 시간별 발전량을 분석해줘
- 판정: 시간규약 미확정(±1h) 경고를 언급하는지 육안 확인
- SQL1: `SELECT * FROM research.generation WHERE fuel_type = 'wind' AND timestamp >= '2026-07-01' AND timestamp <= '2026-07-31';`
- 최종답(앞 300자): 2026년 7월 풍력 시간별 발전량 데이터를 분석했습니다. 아래는 결과입니다:

| plant_id | plant_name     | unit_no | operator | fuel_type | timestamp           | gen_kwh     | is_aggregate | data_quality | hourly_valid_from |
|----------|----------------|---------|----------|-----------|---------------------|-------------|--------

## monthly-trend — ❌ 실패
- 질문: 2026년 월별 태양광 발전량 추이를 표로 보여줘
- 판정: 마지막 SQL 이 에러로 끝남
- SQL1 ⚠에러: `SELECT * FROM research.generation WHERE fuel_type = 'solar' GROUP BY EXTRACT(YEAR FROM timestamp), EXTRACT(MONTH FROM timestamp) ORDER BY EXTRACT(YEAR FROM timestamp), EXTRACT(MONTH FROM timestamp);`
- 에러: Error executing tool run_sql: SQL 오류: column "generation.plant_id" must appear in the GROUP BY clause or be used in an aggregate function
LINE 1: SELECT * FROM research.generation WHERE fuel_type = 's
- 최종답(앞 300자): The error indicates that the `plant_id` column is not included in the `GROUP BY` clause or used in an aggregate function. To fix this, we need to either include `plant_id` in the `GROUP BY` clause or use an aggregate function if we want to summarize by `plant_id`.

Here is the corrected query:

```s

## fuel-implicit — ✅ 통과
- 질문: 2026년 8월 태양광 발전량이 가장 많았던 날은?
- 판정: -
- SQL1: `SELECT timestamp, gen_kwh FROM research.generation WHERE fuel_type = 'solar' AND EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 8 ORDER BY gen_kwh DESC LIMIT 1;`
- 최종답(앞 300자): 2026년 8월 태양광 발전량이 가장 많았던 날은 **2026년 8월 6일 오전 12시**입니다. 해당 날의 발전량은 **1470.6 kWh**입니다.

다운로드 링크: [https://localhost:8098/export-e22e399a.csv](http://localhost:8098/export-e22e399a.csv)

이 데이터는 CSV 파일로 제공되며, `pandas`로 읽을 때는 `parse_dates=['timestamp']`와 `index_col='timestamp'`를 지정하여 시간 컬럼을 처리해야 합니다.

## demand-column-trap — ✅ 통과
- 질문: 가장 최근 전국 전력 공급능력이 얼마야?
- 판정: -
- SQL1: `SELECT current_supply FROM research.demand_5min ORDER BY timestamp DESC LIMIT 1;`
- 최종답(앞 300자): 가장 최근 전국 전력 공급능력은 **106,789.0 MW**입니다.

```sql
SELECT current_supply FROM research.demand_5min ORDER BY timestamp DESC LIMIT 1;
```

## forecast-fn — ✅ 통과
- 질문: 서울 개포1동의 2023년 1월 1시간기온 단기예보를 보여줘
- 판정: -
- SQL1: `SELECT * FROM research.forecast('단기예보','개포1동','1시간기온','202301','202301')`
- 최종답(앞 300자): | sido      | sigungu | dong_name | element_name | grid  | base_at           | lead_hours | target_at           | value |
|-----------|---------|-----------|--------------|-------|-------------------|------------|---------------------|-------|
| 서울특별시 | 강남구  | 개포1동   | 1시간기온    | 61_125 | 2023-01-01