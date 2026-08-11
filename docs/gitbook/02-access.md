# 접속 가이드 & 예제 쿼리

## 1. Tailscale 설치·로그인

1. [tailscale.com/download](https://tailscale.com/download)에서 OS에 맞는 클라이언트를 설치한다.
2. 관리자에게 받은 초대 링크로 로그인하면 폐쇄망(tailnet)에 합류한다.
3. 접속 상태 확인:
   ```bash
   tailscale status
   ```
   목록에 DB 호스트가 보이면 연결된 것이다. 이하 문서의 `<tailnet-host>`는
   관리자가 안내하는 실제 호스트명(Tailscale MagicDNS 이름 또는 tailnet IP)으로
   바꿔서 쓴다.

## 2. 접속 정보

DB는 두 개다. 각각 별도 role/비밀번호가 발급된다.

| DB | 포트 | DB명 | 접속 계정 |
|---|---|---|---|
| pv (발전량·SMP·기상) | `5436` | `pv` | 발급받은 role — `<발급받은_ID>` |
| demand (전국/제주 수급·열수요) | `5433` | `demand` | 발급받은 role — `<발급받은_ID>` |

발급받은 role은 다음이 이미 서버 쪽에서 강제돼 있다 (설정을 바꿀 필요 없음):

- `search_path`가 `research`로 고정 — 스키마 이름 없이 `plants`, `generation`처럼 바로 조회 가능
- `statement_timeout` 60초 — 오래 걸리는 쿼리는 자동으로 끊긴다. 큰 범위를 조회할 땐 `WHERE timestamp BETWEEN ...`로 기간을 좁혀라
- 동시 연결 5개 제한
- **모든 쿼리가 로그로 남는다** (`log_statement=all`) — [03-terms.md](03-terms.md) 참고

## 3. psql

```bash
psql "postgresql://<발급받은_ID>:<발급받은_비밀번호>@<tailnet-host>:5436/pv"
```

```bash
psql "postgresql://<발급받은_ID>:<발급받은_비밀번호>@<tailnet-host>:5433/demand"
```

접속 후 `\dv`로 뷰 목록, `\d+ generation`으로 컬럼과 코멘트를 확인할 수 있다.

## 4. Python (pandas)

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

demand DB는 포트 `5433`, DB명 `demand`로 엔진을 하나 더 만들면 된다.

## 5. R

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

## 6. MCP 서버 (선택)

자연어로 `research` 스키마를 조회하고 싶다면 `energy-mcp` MCP 서버를 쓸 수
있다. Claude Desktop / VS Code / Continue 설정 예시는
[`mcp-server/README.md`](../../mcp-server/README.md)에 있다 — 요약하면:

- 설치: `uvx energy-mcp` (별도 설치 스크립트 없음)
- 제공 기능: 읽기전용 SQL 실행 툴 `run_sql` 하나, 스키마 설명 리소스 `energy://schema` 하나
- 인증은 이 서버가 하지 않는다 — 본인에게 발급된 role의 DSN을
  `ENERGY_MCP_DSN` 환경변수로 넘기면 그 role의 권한과 감사 로그를 그대로 쓴다
- 조회 결과를 외부 LLM 서버로 보내는 것이 꺼려지면
  [appendix-local-llm.md](appendix-local-llm.md)의 로컬 LLM 경로를 쓴다

## 7. 자주 쓰는 예제 쿼리

아래 쿼리는 전부 실제 DB에서 실행해 결과를 확인했다. 발전소명·기간만 바꿔
쓰면 된다.

### 7-1. 특정 발전소 시간별 발전량

```sql
SELECT "timestamp", plant_name, gen_kwh
FROM generation
WHERE plant_name = '구미태양광'
ORDER BY "timestamp" DESC
LIMIT 100;
```

### 7-2. 월별 발전량 합계

```sql
SELECT date_trunc('month', "timestamp")::date AS month,
       sum(gen_kwh) AS gen_kwh_sum
FROM generation
WHERE plant_name = '구미태양광'
GROUP BY 1
ORDER BY 1 DESC;
```

### 7-3. 발전소별 비교 (총발전량 순위)

정상 등급만, 이중계상 계열(`is_aggregate`) 제외:

```sql
SELECT plant_name, sum(gen_kwh) AS gen_kwh_sum
FROM generation
WHERE fuel_type = 'solar'
  AND data_quality = '정상'
  AND is_aggregate = false
  AND "timestamp" >= '2024-01-01' AND "timestamp" < '2025-01-01'
GROUP BY plant_name
ORDER BY gen_kwh_sum DESC
LIMIT 20;
```

### 7-4. 발전량 × 일사량 결합

이 프로젝트는 발전소–기상관측소 최근접 매칭 테이블을 제공하지 않는다
(좌표 신뢰도가 태양광도 ±2km 근사 수준이라 자동 매칭이 위험하다 —
[01-data.md](01-data.md) "좌표" 참고). 아래는 발전소명과 관측소명이 우연히
겹치는 `구미태양광`↔`구미` 관측소로 예시를 든 것이다. 실제 분석에서는
발전소 소재지를 직접 확인하고 `has_solar_sensor=true`인 관측소를 골라야
한다:

```sql
SELECT g."timestamp", g.gen_kwh, w.solar_radiation
FROM generation g
JOIN weather_asos w
  ON w."timestamp" = g."timestamp" AND w.station_name = '구미'
WHERE g.plant_name = '구미태양광'
  AND g."timestamp" >= '2025-06-01' AND g."timestamp" < '2025-06-02'
ORDER BY g."timestamp";
```

### 7-5. 발전량 × SMP 결합 (매출 근사)

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
발전소는 `smp_hourly.region='land'`와 짝지어야 한다(코드값이 다르다 —
[01-data.md](01-data.md) 참고).

### 7-6. 제주 계통수급 최근 데이터 (demand DB)

```sql
SELECT "timestamp", supply_mw, demand_mw, renewable_total_mw, solar_mw, wind_mw
FROM jeju_supply_demand
ORDER BY "timestamp" DESC
LIMIT 100;
```

### 7-7. 지사별 열수요 × 기온 (demand DB)

```sql
SELECT "timestamp", branch, heat_demand, temperature
FROM heat_demand
WHERE branch = '강남'
ORDER BY "timestamp" DESC
LIMIT 100;
```

### 7-8. 월별 최대수요 (demand DB)

```sql
SELECT date_trunc('month', "timestamp")::date AS month,
       max(current_demand) AS peak_mw
FROM demand_5min
WHERE "timestamp" >= '2024-01-01' AND "timestamp" < '2025-01-01'
GROUP BY 1 ORDER BY 1;
```

`current_supply`/`supply_capacity` 컬럼명은 원천 항목과 뒤바뀌어 있다 —
공급능력이 필요하면 `current_supply`를 써라([01-data.md](01-data.md) 참고).
