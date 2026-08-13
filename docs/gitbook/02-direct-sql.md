# 직접 SQL로 조회

psql·pandas·R 같은 도구로 PostgreSQL에 직접 연결해 조회하는 방법입니다. 재현 가능한 분석·통계·그래프 작업이라면 이 방법이 낫습니다.

## 시작하기 전에

다음 세 가지가 준비돼 있어야 합니다.

1. [이용조건](05-terms.md) 서약 완료
2. 관리자가 보낸 Tailscale 초대
3. 개인별 DB 계정(role·비밀번호) — 별도 채널로 수령

## 1. Tailscale 설치와 연결 확인

1. [tailscale.com/download](https://tailscale.com/download)에서 OS에 맞는 클라이언트를 설치합니다.
2. 관리자에게 받은 초대 링크로 로그인하면 폐쇄망(tailnet)에 합류합니다.
3. 접속 상태를 확인합니다:
   ```bash
   tailscale status
   ```
   목록에 DB 호스트가 보이면 연결된 상태입니다. 이 문서에 나오는 `<tailnet-host>`는
   관리자가 안내하는 실제 호스트명(Tailscale MagicDNS 이름 또는 tailnet IP)으로
   바꿔서 씁니다.

## 2. 개인 접속정보 이해하기

DB는 두 개입니다. role과 비밀번호도 DB마다 따로 발급합니다.

| DB | 포트 | DB명 | 접속 계정 |
|---|---|---|---|
| pv (발전량·SMP·기상) | `5436` | `pv` | 발급받은 role — `<발급받은_ID>` |
| demand (전국/제주 수급·열수요) | `5433` | `demand` | 발급받은 role — `<발급받은_ID>` |

발급받은 role에는 서버 쪽에서 다음이 이미 걸려 있습니다 (설정을 바꿀 필요 없음):

- `search_path`가 `research`로 고정 — 스키마 이름 없이 `plants`, `generation`처럼 바로 조회 가능
- `statement_timeout` 60초 — 오래 걸리는 쿼리는 자동으로 끊깁니다. 큰 범위를 조회할 땐 `WHERE timestamp BETWEEN ...`으로 기간을 좁히세요
- 동시 연결 5개 제한
- **모든 쿼리가 로그로 남습니다** (`log_statement=all`) — [이용조건](05-terms.md) 참고

## 3. 첫 조회

쓰는 도구를 고르세요. 접속정보는 셋 다 같습니다.

{% tabs %}
{% tab title="psql" %}
```bash
psql "postgresql://<발급받은_ID>:<발급받은_비밀번호>@<tailnet-host>:5436/pv"
```

demand DB는 포트만 바꿉니다.

```bash
psql "postgresql://<발급받은_ID>:<발급받은_비밀번호>@<tailnet-host>:5433/demand"
```

접속한 뒤 `\dv`를 치면 뷰 목록이, `\d+ generation`을 치면 컬럼과 코멘트가 나옵니다.

첫 쿼리는 기간을 좁힌 작은 조회로 시작하세요.

```sql
SELECT "timestamp", plant_name, gen_kwh
FROM generation
WHERE plant_name = '구미태양광'
ORDER BY "timestamp" DESC
LIMIT 100;
```
{% endtab %}

{% tab title="Python · pandas" %}
```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://<발급받은_ID>:<발급받은_비밀번호>@<tailnet-host>:5436/pv"
)

df = pd.read_sql(
    """
    SELECT "timestamp", gen_kwh
    FROM generation
    WHERE plant_name = '구미태양광'
      AND "timestamp" >= '2025-01-01'
    ORDER BY "timestamp"
    """,
    engine,
)
```

demand DB는 포트 `5433`, DB명 `demand`로 엔진을 하나 더 만들면 됩니다.
{% endtab %}

{% tab title="R" %}
```r
library(DBI)
library(RPostgres)

con <- dbConnect(
  RPostgres::Postgres(),
  host = "<tailnet-host>", port = 5436, dbname = "pv",
  user = "<발급받은_ID>", password = "<발급받은_비밀번호>"
)

df <- dbGetQuery(con, "
  SELECT date_trunc('month', \"timestamp\")::date AS month, sum(gen_kwh) AS gen_kwh_sum
  FROM generation
  WHERE plant_name = '구미태양광'
  GROUP BY 1 ORDER BY 1
")
dbDisconnect(con)
```
{% endtab %}
{% endtabs %}

## 4. 자주 쓰는 검증된 쿼리

아래 쿼리는 전부 실제 DB에서 실행해 결과를 확인했습니다. 발전소명·기간만
바꿔 쓰면 됩니다. 단위·시간 규약·품질 등급은
[데이터 카탈로그](04-data-catalog.md)를 먼저 확인하세요.

### 4-1. 특정 발전소 시간별 발전량

```sql
SELECT "timestamp", plant_name, gen_kwh
FROM generation
WHERE plant_name = '구미태양광'
ORDER BY "timestamp" DESC
LIMIT 100;
```

### 4-2. 월별 발전량 합계

```sql
SELECT date_trunc('month', "timestamp")::date AS month,
       sum(gen_kwh) AS gen_kwh_sum
FROM generation
WHERE plant_name = '구미태양광'
GROUP BY 1
ORDER BY 1 DESC;
```

### 4-3. 발전소별 비교 (총발전량 순위)

```sql
SELECT plant_name, sum(gen_kwh) AS gen_kwh_sum
FROM generation
WHERE fuel_type = 'solar'
  AND "timestamp" >= '2024-01-01' AND "timestamp" < '2025-01-01'
GROUP BY plant_name
ORDER BY gen_kwh_sum DESC
LIMIT 20;
```

{% hint style="warning" %}
**`data_quality`와 `is_aggregate`를 여기서 또 거를 필요는 없다.**
`research.generation`은 시간별로 믿을 수 없는 구간을 **이미 제외한 상태**라
`data_quality = '정상'`을 덧붙이면 영흥태양광 #3의 정상 구간(2025-07-01 이후)까지
날아가고, 비태양광 46기(`미검증`)도 통째로 사라진다.
`is_aggregate = false`는 2019~2021년 영암 발전량을 지운다 — 자세한 이유는
[데이터 카탈로그의 is_aggregate 절](catalog/generation.md#함정--is_aggregate-지금은-빼면-안-된다)에 있다.
{% endhint %}

### 4-4. 발전량 × 일사량 결합

이 프로젝트는 발전소–기상관측소 최근접 매칭 테이블을 제공하지 않습니다
(좌표 신뢰도가 태양광도 ±2km 근사 수준이라 자동 매칭이 위험합니다 —
[데이터 카탈로그](04-data-catalog.md) "좌표" 참고). 아래는 발전소명과
관측소명이 우연히 겹치는 `구미태양광`↔`구미` 관측소를 예시로 썼습니다.
실제 분석에서는 발전소 소재지를 직접 확인하고 `has_solar_sensor=true`인
관측소를 골라야 합니다:

```sql
SELECT g."timestamp", g.gen_kwh, w.solar_radiation
FROM generation g
JOIN weather_asos w
  ON w."timestamp" = g."timestamp" AND w.station_name = '구미'
WHERE g.plant_name = '구미태양광'
  AND g."timestamp" >= '2025-06-01' AND g."timestamp" < '2025-06-02'
ORDER BY g."timestamp";
```

### 4-5. 발전량 × SMP 결합 (매출 근사)

```sql
SELECT g."timestamp", g.gen_kwh, s.price AS smp_land_price,
       round((g.gen_kwh * s.price)::numeric, 0) AS revenue_krw
FROM generation g
JOIN smp_hourly s
  ON s."timestamp" = g."timestamp" AND s.region = 'land'
WHERE g.plant_name = '구미태양광'
  AND g."timestamp" >= '2025-06-01' AND g."timestamp" < '2025-06-02'
ORDER BY g."timestamp";
```

`gen_kwh`(kWh) × `price`(원/kWh) = 원. `plants.region='mainland'`인
발전소는 `smp_hourly.region='land'`와 짝지어야 합니다(코드값이 다릅니다 —
[데이터 카탈로그](04-data-catalog.md) 참고).

### 4-6. 제주 계통수급 최근 데이터 (demand DB)

```sql
SELECT "timestamp", supply_mw, demand_mw, renewable_total_mw, solar_mw, wind_mw
FROM jeju_supply_demand
ORDER BY "timestamp" DESC
LIMIT 100;
```

### 4-7. 지사별 열수요 × 기온 (demand DB)

```sql
SELECT "timestamp", branch, heat_demand, temperature
FROM heat_demand
WHERE branch = '강남'
ORDER BY "timestamp" DESC
LIMIT 100;
```

### 4-8. 월별 최대수요 (demand DB)

```sql
SELECT date_trunc('month', "timestamp")::date AS month,
       max(current_demand) AS peak_mw
FROM demand_5min
WHERE "timestamp" >= '2024-01-01' AND "timestamp" < '2025-01-01'
GROUP BY 1 ORDER BY 1;
```

`current_supply`/`supply_capacity` 컬럼명은 원천 항목과 뒤바뀌어 있습니다 —
공급능력이 필요하면 `current_supply`를 쓰세요
([데이터 카탈로그](04-data-catalog.md) 참고).

## 문제 해결

| 증상 | 먼저 확인할 것 | 해결 |
|---|---|---|
| 연결 시간 초과 | `tailscale status` | Tailscale 재연결 후 관리자에게 호스트 확인 |
| 인증 실패 | 개인 role·비밀번호 | 문서나 채팅에 비밀번호를 붙이지 말고 재발급 요청 |
| 60초 후 쿼리 종료 | 조회 기간 | 기간·발전소·지점 조건과 `LIMIT` 추가 |
| 뷰를 찾을 수 없음 | 접속 DB와 `search_path` | pv/demand 포트와 발급 계정 재확인 |
