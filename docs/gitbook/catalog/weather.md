# 기상 관측

**실제로 관측된 기상**입니다 — ASOS 시간별(`research.weather_asos`)과 AWS·ASOS 일자료(`research.aws_obs_daily`).

{% hint style="warning" %}
**예보는 이 페이지에 없습니다.** "뭐라고 예보했나"는 [기상 예보](forecast.md)에 따로 있습니다.

두 페이지를 섞어 쓸 때 **시간 기준이 다릅니다** — 이 페이지의 관측은 **KST**, 예보는 **UTC**입니다. 예보를 관측·발전량·수요와 붙이려면 시차를 옮겨야 합니다. 자세한 것은 예보 페이지의 시차 항목을 보세요.
{% endhint %}

***

### research.weather\_asos — ASOS 시간별 기상

| 항목    | 값                        |
| ----- | ------------------------ |
| 행수    | 4,741,656                |
| 기간    | 2019-01-01 \~ 2026-08-11 |
| 지점 수  | 95 (일사 관측 63 / 미관측 32)   |
| 갱신 주기 | 매일 09:00 (전날 기상 데이터)     |

| 컬럼                 | 의미          | 단위                                                          |
| ------------------ | ----------- | ----------------------------------------------------------- |
| `timestamp`        | 관측 시각       | 기온/습도는 KST, 시간 라벨 규약 확인됨. **일사량만 시간 라벨이 아직 "추정" 단계**(아래 참고) |
| `station_name`     | 관측소명        |                                                             |
| `temperature`      | 기온          | ℃                                                           |
| `humidity`         | 습도          | %                                                           |
| `solar_radiation`  | 일사량         | MJ/m²                                                       |
| `has_solar_sensor` | 일사 관측 지점 여부 | 아래 "함정" 참고                                                  |

#### 함정 — has\_solar\_sensor로 판별하세요

일사 관측 지점은 **63개**입니다(`has_solar_sensor=true`). **`solar_radiation IS NOT NULL`로 지점을 세면 안 됩니다** — 실제로 세보면 65개가 나옵니다. 미관측 지점 중 `성산`·`제천` 2곳에 각각 **1건씩, 값 0인 이상치**가 섞여 있기 때문입니다(정상 관측 지점은 평균 23,382행씩 값이 있으니 명백한 이상치입니다). 관측 지점을 판별할 때는 반드시 `has_solar_sensor`를 쓰세요:

```sql
-- 틀린 방법 (65개로 잘못 나옴)
SELECT DISTINCT station_name FROM research.weather_asos WHERE solar_radiation IS NOT NULL;

-- 맞는 방법 (63개)
SELECT DISTINCT station_name FROM research.weather_asos WHERE has_solar_sensor;
```

`has_solar_sensor=false`인 32개 지점은 `solar_radiation`이 항상 NULL입니다. 결측이 아니라 애초에 관측하지 않으니 정상입니다. `has_solar_sensor=true`인 지점도 야간이거나 관측 시작 이전 기간에는 NULL이 나올 수 있습니다(관측 시작 시점은 지점마다 다릅니다 — 특정 지점을 쓸 때는 `WHERE station_name = 'X' AND solar_radiation IS NOT NULL`로 값이 실제로 있는 구간을 먼저 확인하세요).

일사량의 시간 라벨(구간시작/구간종료)은 기상청 공식 문구를 찾지 못해 **아직 추정 단계**입니다. 보정도 하지 않았습니다. 시각 정밀도가 중요한 분석(예: 특정 시각의 순간 일사량과 발전량 1:1 대조)이라면 최대 1시간의 오차를 감안하세요.

***

### research.aws\_obs\_daily — AWS·ASOS 일자료

| 항목    | 값                        |
| ----- | ------------------------ |
| 행수    | 377,822                  |
| 지점    | 107 (AWS 93 · ASOS 14)   |
| 기간    | 2015-01-01 \~ 2026-06-15 |
| 갱신 주기 | 없음 (정적)                  |

**일 단위입니다.** 시간별 기상은 위의 `research.weather_asos`를 쓰세요. 컬럼은 `date`, `station_id`, `station_name`, `station_type`, 기온 `ta`, 풍향 `wd`, 풍속 `ws`, 일강수량 `rn_day`, 1시간최다강수량 `rn_hr1`, 습도 `hm`, 현지기압 `pa`, 해면기압 `ps`, 그리고 `imputed`입니다.

{% hint style="warning" %}
**`imputed`를 먼저 보세요.** 결측을 채운 컬럼 이름이 쉼표로 들어갑니다(예: `ta,hm,ps`). 비어 있으면 원본 관측값입니다. AWS 31.9만 행 중 **18.7만 행(58%)에 보정이 들어가 있어서**, 관측 진실값이 필요한 분석이라면 이 컬럼이 빈 행만 쓰거나 보정된 요소를 빼야 합니다.

`station_type`으로 AWS(방재기상관측 93지점)와 ASOS(종관기상 14지점)가 섞여 있습니다. 두 관측망은 장비와 검증 수준이 다르니 섞어서 통계 내기 전에 나눠 보세요.
{% endhint %}

***
