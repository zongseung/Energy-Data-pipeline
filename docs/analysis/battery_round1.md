# 데모 쿼리 배터리 결과


**요약: 통과 8 / 실패 4 / 육안 2 (전체 14)**


## agg-by-plant — ❌ 실패
- 질문: 2026년 7월 발전소별 태양광 발전량 상위 5곳을 표로 보여줘
- 판정: 필수 패턴 누락: group\s+by; 필수 패턴 누락: solar; 필수 패턴 누락: 2026-0?7
- SQL1: `SELECT plant_id, plant_name, fuel_type, gen_kwh FROM research.generation WHERE EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 7 ORDER BY gen_kwh DESC LIMIT 5;`
- 최종답(앞 300자): | plant_id | plant_name | fuel_type | gen_kwh     |
|----------|------------|-----------|-------------|
| 79       | 영흥_6     | thermal   | 884233.0    |
| 79       | 영흥_6     | thermal   | 884185.0    |
| 79       | 영흥_6     | thermal   | 884134.0    |
| 79       | 영흥_6     | thermal   | 884126.0  

## fuel-filter — ✅ 통과
- 질문: 태양광 발전량 데이터만 2026년 8월 1일부터 3일치 보여줘. 다른 연료는 빼고.
- 판정: -
- SQL1: `SELECT gen_kwh FROM research.generation WHERE fuel_type = 'solar' AND timestamp >= '2026-08-01' AND timestamp <= '2026-08-03'`
- 최종답(앞 300자): | gen_kwh |
|---------|
| 0.0     |
| 0.0     |
| 0.0     |
| 0.0     |
| 0.0     |
| 0.0     |
| 0.0     |
| 0.0     |
| 0.0     |
| 0.0     |

<p>2026년 8월 1일부터 3일까지의 태양광 발전량 데이터는 위 표와 같습니다. 데이터는 10행의 미리보기입니다. 전체 데이터는 <a href="http://localhost:8098/export-8f7ab3cd.csv">이 링크</a>에서 CSV 파일로 다운로드할 수 있습

## period-bounds — ❌ 실패
- 질문: 2026년 7월 한 달간 태양광 총 발전량 알려줘
- 판정: 필수 패턴 누락: 2026-0?7
- SQL1: `SELECT SUM(gen_kwh) AS total_solar_generation FROM research.generation WHERE EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 7 AND fuel_type = 'solar';`
- 최종답(앞 300자): 2026년 7월에 기록된 태양광 총 발전량은 **12,387,046.47 kWh**입니다.

다운로드 링크:  
[https://localhost:8098/export-47a5ae25.csv](http://localhost:8098/export-47a5ae25.csv)

이 데이터는 CSV 파일로 제공되어, `pandas`로 읽을 때는 다음과 같이 시간 컬럼을 지정하라고 안내해 드릴 수 있습니다:

```python
import pandas as pd
df = pd.read_csv("경로", parse_dates=['timestam

## aggregate-trap — ❌ 실패
- 질문: 2026년 전체 태양광 발전량 총합을 구해줘 (이중계상 없이)
- 판정: 필수 패턴 누락: is_aggregate
- SQL1: `SELECT SUM(gen_kwh) AS total_solar_generation FROM research.generation WHERE fuel_type = 'solar';`
- 최종답(앞 300자): 2026년 전체 태양광 발전량 총합은 **537,682,070.08 kWh**입니다.

다운로드 링크: [전체 데이터 CSV 파일](http://localhost:8098/export-13001177.csv)

이 데이터는 `research.generation` 테이블에서 `fuel_type = 'solar'` 조건으로 추출되었습니다. 필요하다면 CSV 파일을 직접 다운로드하여 사용할 수 있습니다. 

이 SQL 쿼리를 실행한 결과는 다음과 같습니다:

```sql
SELECT SUM(gen_kwh) AS total_solar_ge

## weather-join — ✅ 통과
- 질문: 2026년 8월 5일 서울 기온과 시간별 전체 태양광 발전량을 같이 보여줘
- 판정: -
- SQL1: `SELECT timestamp, temperature, solar_radiation FROM research.weather_asos WHERE station_name = '서울' AND timestamp = '2026-08-05'; SELECT timestamp, plant_id, plant_name, fuel_type, gen_kwh FROM research.generation WHERE plant_name = '서울' AND timestamp = '2026-08-05';`
- 에러: Error executing tool run_sql: 한 번에 하나의 SQL 문장만 실행할 수 있습니다. 세미콜론으로 여러 문장을 연결하지 마세요.
- 최종답(앞 300자): I apologize for the error. Let me correct the query to execute only one SQL statement at a time.

First, I will retrieve the temperature and solar radiation data for Seoul on 2026-08-05:

```sql
SELECT timestamp, temperature, solar_radiation FROM research.weather_asos WHERE station_name = '서울' AND t

## smp-daily — ✅ 통과
- 질문: 2026년 8월 10일 육지(land) SMP 시간별 가격을 보여줘
- 판정: -
- SQL1: `SELECT * FROM research.smp_hourly WHERE timestamp::date = '2026-08-10' AND station_type = 'land';`
- 에러: Error executing tool run_sql: SQL 오류: column "station_type" does not exist
LINE 1: ...p_hourly WHERE timestamp::date = '2026-08-10' AND station_ty...
                                                  
- 최종답(앞 300자): 2026년 8월 10일 육지(land) SMP 시간별 가격을 보여줄 수 없습니다. 먼저 뷰의 실제 컬럼 이름을 확인해야 합니다. 아래 명령어를 실행하여 뷰의 컬럼을 확인할 수 있습니다:

```sql
SELECT * FROM research.smp_hourly LIMIT 1;
```

이 명령어는 뷰 `research.smp_hourly`의 첫 번째 행을 반환하여, 실제 컬럼 이름을 확인할 수 있습니다. 확인한 후, 필요하다면 해당 컬럼 이름을 사용하여 다시 쿼리를 실행할 수 있습니다.

## jeju-demand — ✅ 통과
- 질문: 가장 최근 제주 전력수급 상황(태양광·풍력 포함)을 알려줘
- 판정: -
- SQL1: `SELECT * FROM research.jeju_supply_demand ORDER BY timestamp DESC LIMIT 1;`
- 최종답(앞 300자): 가장 최근 제주 전력수급 상황(태양광·풍력 포함)은 다음과 같습니다:

| timestamp         | supply_mw | demand_mw | renewable_total_mw | solar_mw  | wind_mw  |
|-------------------|-----------|-----------|--------------------|-----------|----------|
| 2026-08-12T09:50:00 | 1619.0   | 727.0    | 334.888           | 227.071  | 95.

## csv-extract — ✅ 통과
- 질문: 2026년 8월 1일~10일 태양광 발전량 데이터를 csv로 줘
- 판정: -
- SQL1: `SELECT * FROM research.generation WHERE timestamp >= '2026-08-01' AND timestamp <= '2026-08-10'`
- 최종답(앞 300자): ```sql
SELECT * FROM research.generation WHERE timestamp >= '2026-08-01' AND timestamp <= '2026-08-10'
```

### 데이터 다운로드
전체 데이터는 [이곳](http://localhost:8098/export-4fd48ef0.csv)에서 CSV 파일로 다운로드할 수 있습니다. 파일에는 총 3,256개의 행이 포함되어 있습니다.

### 데이터 요약
| plant_id | plant_name | unit_no | operator | fuel_type |

## korean-codes — ✅ 통과
- 질문: 데이터 품질이 정상인 발전소가 몇 개야?
- 판정: -
- SQL1: `SELECT COUNT(*) FROM research.plants WHERE data_quality = '정상';`
- 최종답(앞 300자): 데이터 품질이 정상인 발전소는 총 **33개**입니다.

결과는 아래와 같습니다:

| count |
|-------|
| 33    |

다운로드 링크: [전체 데이터를 CSV 파일로 내려받기](http://localhost:8098/export-f84daeb1.csv)

사용자가 DB 클라이언트에 붙여넣어 전체 데이터를 직접 추출할 수 있게 제공해드렸습니다.

## round-recovery — ❌ 실패
- 질문: 2026년 8월 육지 SMP 평균 가격을 소수점 1자리로 알려줘
- 판정: 에러에서 회복 못함
- SQL1: `SELECT ROUND(AVG(price), 1) AS average_price FROM research.smp_hourly WHERE EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 8;`
- 에러: Error executing tool run_sql: SQL 오류: function round(double precision, integer) does not exist
LINE 1: SELECT ROUND(AVG(price), 1) AS average_price FROM research.s...
               ^
HINT:  No functi
- 최종답(앞 300자): PostgreSQL에서 `ROUND` 함수를 사용할 때, 두 번째 인수는 `integer` 타입이어야 하며, 첫 번째 인수는 `numeric` 타입이어야 합니다. 이 경우, `AVG(price)`가 `double precision` 타입이므로, `numeric` 타입으로 캐스트해야 합니다.

다음은 수정된 쿼리입니다:

```sql
SELECT ROUND(AVG(price::numeric), 1) AS average_price FROM research.smp_hourly WHERE EXTRACT(YEAR FROM timestamp)

## hallucination-recovery — ✅ 통과
- 질문: power_plants 테이블에서 발전소 목록 5개만 보여줘
- 판정: -
- SQL1: `SELECT * FROM research.plants LIMIT 5;`
- 최종답(앞 300자): | plant_id | plant_name               | unit_no | operator | fuel_type | region  | capacity_mw | lat    | lon      | is_aggregate | data_quality | hourly_valid_from | daily_valid_from | daily_valid_to | data_quality_note |
|----------|--------------------------|---------|----------|-----------|-----

## empty-future — 🖐 육안
- 질문: 2030년 1월 태양광 발전량을 알려줘
- 판정: 환각 숫자 없이 '데이터 없음'이라 답하는지 육안 확인
- SQL1: `SELECT * FROM research.generation WHERE EXTRACT(YEAR FROM timestamp) = 2030 AND EXTRACT(MONTH FROM timestamp) = 1 AND fuel_type = 'solar'`
- 최종답(앞 300자): 죄송합니다. 2030년 1월의 태양광 발전량 데이터는 없습니다. 요청하신 데이터가 존재하지 않거나, 해당 기간의 데이터가 기록되지 않았을 수 있습니다. 더 궁금한 점이 있다면 언제든지 문의해 주세요.

## wind-caveat — 🖐 육안
- 질문: 2026년 7월 풍력 시간별 발전량을 분석해줘
- 판정: 시간규약 미확정(±1h) 경고를 언급하는지 육안 확인
- SQL1: `SELECT * FROM research.generation WHERE EXTRACT(YEAR FROM timestamp) = 2026 AND EXTRACT(MONTH FROM timestamp) = 7 AND fuel_type = 'wind';`
- 최종답(앞 300자): | plant_id | plant_name     | unit_no | operator | fuel_type | timestamp           | gen_kwh     | is_aggregate | data_quality | hourly_valid_from |
|----------|----------------|---------|----------|-----------|---------------------|------------|-------------|--------------|-------------------|
| 45

## monthly-trend — ✅ 통과
- 질문: 2026년 월별 태양광 발전량 추이를 표로 보여줘
- 판정: -
- SQL1: `SELECT EXTRACT(YEAR FROM timestamp) AS year, EXTRACT(MONTH FROM timestamp) AS month, SUM(gen_kwh) AS solar_generation FROM research.generation WHERE fuel_type = 'solar' GROUP BY EXTRACT(YEAR FROM timestamp), EXTRACT(MONTH FROM timestamp) ORDER BY year, month;`
- 최종답(앞 300자): | year | month | solar_generation       |
|------|-------|------------------------|
| 2015 | 10    | 19268.25898760557      |
| 2015 | 11    | 33215.9819893986       |
| 2015 | 12    | 40046.104042027146     |
| 2016 | 1     | 42690.05697123706      |
| 2016 | 2     | 62017.873027950525     |
| 2016