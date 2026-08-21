# 국제유가

브렌트·WTI 시간별 OHLCV. 발전량·SMP와 같은 `timestamp`로 조인해 연료비와 전력 가격을 함께 볼 때 씁니다.

***

### research.oil\_hourly — 시간별 국제유가 (브렌트·WTI)

{% hint style="warning" %}
**현물 고시가가 아닙니다.** Hyperliquid XYZ builder DEX의 체결 캔들이라 원유 현물 종가와는 다른 계열입니다. 논문에 "브렌트유 가격"으로 인용하기 전에 이 출처가 목적에 맞는지 먼저 판단하세요.
{% endhint %}

| 항목    | 값                                                                 |
| ----- | ----------------------------------------------------------------- |
| 행수    | 9,116 (브렌트 3,876 · WTI 5,240)                                     |
| 기간    | WTI 2026-01-07 \~ / 브렌트 2026-03-05 \~ (원천에 있는 만큼)                 |
| 갱신 주기 | 매시 05분                                                            |
| 단위    | USD (`volume`은 DEX 거래량)                                           |
| 원천    | Hyperliquid `api.hyperliquid.xyz/info` — `xyz:BRENTOIL`, `xyz:CL` |

**컬럼**

| 컬럼                             | 의미                | 비고                               |
| ------------------------------ | ----------------- | -------------------------------- |
| `timestamp`                    | **구간시작 KST**      | `09:00`은 `[09:00, 10:00)` 구간의 캔들 |
| `symbol`                       | `brent` 또는 `wti`  |                                  |
| `open`, `high`, `low`, `close` | 시가·고가·저가·종가 (USD) |                                  |
| `volume`                       | 해당 구간 거래량         | **DEX 거래량이지 현물 시장 거래량이 아닙니다**    |
| `trades`                       | 해당 구간 체결 건수       |                                  |

#### 적재본이 없습니다

이 뷰는 수집기가 쓰는 CSV 파일을 `file_fdw`로 **직접** 읽습니다. 중간 적재 단계가 없어서 수집이 끝나는 즉시 조회에 반영됩니다. 동기화 지연이 생길 여지도 없습니다.

원본 wide 형태가 필요하면 `research.oil_hourly_raw`를 보세요(브렌트·WTI가 한 행에 나란히 붙어 있습니다).

#### 브렌트와 WTI의 시작일이 다릅니다

WTI는 2026-01-07부터, 브렌트는 2026-03-05부터입니다. 원천에 그만큼만 있을 뿐 결측이 아닙니다. 두 종목을 나란히 비교하려면 겹치는 구간부터 잡으세요.

```sql
-- 두 종목이 모두 있는 구간
SELECT min("timestamp") FROM research.oil_hourly WHERE symbol = 'brent';
```

#### 24시간 거래됩니다

DEX라 주말·야간에도 캔들이 나옵니다. "일별 종가"를 만들 때 국내 장 마감 시각을 가정하지 마세요. 어느 시각을 종가로 볼지는 분석자가 정합니다.

***

### 쓰는 법

**시간별 유가와 SMP 함께 보기**

```sql
SELECT o."timestamp",
       round(o.close::numeric, 2) AS wti_usd,
       round(s.price::numeric, 1) AS smp_krw
FROM research.oil_hourly o
JOIN research.smp_hourly s
  ON s."timestamp" = o."timestamp" AND s.region = 'land'
WHERE o.symbol = 'wti'
  AND o."timestamp" >= '2026-08-01'
ORDER BY o."timestamp";
```

**종목별 일별 종가·변동폭**

```sql
SELECT "timestamp"::date AS day, symbol,
       round(max(high)::numeric, 2) AS high,
       round(min(low)::numeric, 2)  AS low,
       round((array_agg(close ORDER BY "timestamp" DESC))[1]::numeric, 2) AS last_close
FROM research.oil_hourly
WHERE "timestamp" >= '2026-08-01'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

**발전량·유가·SMP 3자 결합 — 발전량을 먼저 집계하세요**

```sql
WITH gen AS (                      -- ← 먼저 시각별로 접는다
  SELECT "timestamp", sum(gen_kwh) AS solar_kwh
  FROM research.generation
  WHERE fuel_type = 'solar'
    AND "timestamp" >= '2026-08-01' AND "timestamp" < '2026-08-08'
  GROUP BY 1
)
SELECT g."timestamp",
       round(g.solar_kwh::numeric, 0) AS solar_kwh,
       round(o.close::numeric, 2)     AS wti_usd,
       round(s.price::numeric, 1)     AS smp_krw
FROM gen g
JOIN research.oil_hourly o
  ON o."timestamp" = g."timestamp" AND o.symbol = 'wti'
JOIN research.smp_hourly s
  ON s."timestamp" = g."timestamp" AND s.region = 'land'
ORDER BY g."timestamp";
```

{% hint style="warning" %}
**`generation`을 집계하지 않고 그대로 3자 조인하면 90초를 넘깁니다.** `research.generation`은 320만 행이라 발전소 × 시각 조합만큼 불어난 채로 조인에 들어갑니다. 위처럼 CTE에서 시각별로 먼저 접으면 168행짜리 조인이 되어 **0.7초**에 끝납니다. 같은 결과, 100배 이상 차이입니다.
{% endhint %}
