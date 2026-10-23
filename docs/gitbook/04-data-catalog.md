# 데이터 카탈로그 · 스키마 사전

어떤 데이터가 있는지, 기간과 단위가 무엇인지, 컬럼이 무슨 뜻인지 정리한 안내입니다. 아래 카드에서 필요한 데이터를 고르면 자세한 페이지로 넘어갑니다.

{% hint style="info" %}
기준일은 **2026-10-20**입니다. 데이터는 계속 쌓이므로 기간의 끝은 이후에 늘어납니다. 가장 최신 시점이 궁금하면 LibreChat에 "발전량 데이터가 언제까지 있어?"처럼 물어보세요.
{% endhint %}

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>발전소와 발전량</strong></td><td>태양광·석탄·LNG복합·풍력 등 발전소 142기와 시간별 발전량</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/generation">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/generation</a></td></tr><tr><td><strong>SMP 가격</strong></td><td>전력 도매가격 (시간별·15분·평균)</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/smp">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/smp</a></td></tr><tr><td><strong>전력수요와 수급</strong></td><td>전국·제주 전력 수요, 발전원별 발전, 열수요</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/demand">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/demand</a></td></tr><tr><td><strong>기상 관측</strong></td><td>실제로 관측된 날씨 (시간별·일별)</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/weather">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/weather</a></td></tr><tr><td><strong>기상 예보</strong></td><td>기상청 동네예보 (시각이 UTC)</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/forecast">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/forecast</a></td></tr><tr><td><strong>국제유가</strong></td><td>브렌트·WTI 시간별 시세</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/oil">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/oil</a></td></tr><tr><td><strong>송배전망</strong></td><td>변전소·송전선로 위치, 한전 접속 여유용량</td><td><a href="https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/grid">https://eax-1.gitbook.io/energy-research-data/data/04-data-catalog/grid</a></td></tr></tbody></table>

***

## 한눈에 보는 데이터

| 분야   | 데이터               | 영문 이름                         | 기간                                | 갱신             |
| ---- | ----------------- | ----------------------------- | --------------------------------- | -------------- |
| 발전   | 발전소 목록            | `plants`                      | 142기                              | 발전소가 추가될 때     |
| 발전   | 시간별 발전량 (kWh)     | `generation`                  | 2013-01 \~ 어제                     | 발전사마다 다름       |
| 가격   | SMP 시간별           | `smp_hourly`                  | 2001-05 \~ 어제                     | 매일             |
| 가격   | 제주 실시간 SMP 15분    | `smp_realtime_jeju`           | 2024-03 \~ 어제 (중간 공백 있음)          | 매일             |
| 가격   | SMP 가중평균 (일·월·연)  | `smp_weighted_avg`            | 2001 \~                           | 일별 매일, 월·연별 매월 |
| 수요   | 전국 수요·공급 5분 (MW)  | `demand_5min`                 | 2014-01 \~ 현재                     | 10분마다          |
| 수요   | 발전원별 발전 5분 (MW)   | `gen_mix_5min`                | 2026-08 \~ 현재                     | 5분마다           |
| 수요   | 제주 수요·공급 5분 (MW)  | `jeju_supply_demand`          | 2021-01 \~ 현재                     | 10분마다          |
| 수요   | 제주 시간별 수요 (MW)    | `jeju_demand_hourly`          | 2021-01 \~ 2026-06                | 분기마다           |
| 수요   | 제주 연료원별 거래량 (MWh) | `jeju_generation_mix`         | 2024년 한 해                         | 갱신 없음          |
| 수요   | 지역난방 열수요          | `heat_demand`                 | 2021-01 \~ 2023-12                | 완결(갱신 없음)      |
| 기상   | ASOS 시간별 관측       | `weather_asos`                | 2019-01 \~ 어제                     | 매일             |
| 기상   | AWS·ASOS 일자료      | `aws_obs_daily`               | 2015-01 \~ 2026-06                | 갱신 없음          |
| 기상   | 동네예보 (NAS 파일)     | `forecast()` 함수               | 확인한 지역은 2023년부터                   | 없는 월은 요청해 보충   |
| 유가   | 브렌트·WTI 시간별       | `oil_hourly`                  | 2026-01(WTI) · 2026-03(브렌트) \~ 현재 | 매시             |
| 송배전망 | 변전소 · 송전선로        | `substations` · `power_lines` | 정적                                | 갱신 없음          |
| 송배전망 | 한전 배전망 접속 여유용량    | `kepco_grid`                  | 2026-03 스냅샷                       | 갱신 없음          |

영문 이름은 모두 `research.` 뒤에 붙습니다. 예를 들어 `generation`은 `research.generation`입니다.

## 모든 데이터에 공통인 세 가지

**1. 시각은 한국 시간(KST)의 "구간 시작"입니다.** `09:00`이라는 값은 09:00부터 10:00 전까지의 한 시간을 뜻합니다. 원천은 "구간이 끝나는 시각"으로 적는 경우가 많은데, 이미 구간 시작으로 옮겨 놓았으니 다시 옮기지 마세요. 예외가 있습니다.

| 예외        | 내용                                         |
| --------- | ------------------------------------------ |
| 기상 예보     | 시각이 UTC입니다. 한국 시간으로 쓰려면 9시간을 더합니다          |
| 풍력 발전량    | 원천 시각이 구간 시작인지 끝인지 확정하지 못해 **±1시간 불확실**합니다 |
| ASOS 일사량  | 위와 같은 이유로 최대 1시간 불확실합니다                    |
| 수요 5분·열수요 | 원천 시각을 그대로 썼습니다. 보정하지 않았습니다                |

**2. 발전소 품질 등급은 한국어 값입니다.** `정상`, `시간별무효`, `전면무효`, `미검증` 네 가지입니다. 영어로 바꿔 쓰면 조회되지 않습니다. `미검증`은 "품질이 나쁘다"가 아니라 "품질 검사를 하지 않았다"는 뜻입니다.

**3. 발전량은 이미 걸러져 있습니다.** `generation`은 시간별로 믿을 수 없는 구간을 빼고 보여 줍니다. 품질 등급으로 한 번 더 거르면 멀쩡한 자료까지 사라집니다. 자세한 설명은 발전소와 발전량 페이지에 있습니다.

## 컬럼 이름은 영어입니다

질문은 한국어로 하면 됩니다. 영어 컬럼 이름은 답변에 나온 SQL을 읽을 때, 그리고 값의 뜻을 확인할 때 쓰세요. 각 페이지의 컬럼 표에 뜻과 단위를 적어 두었습니다.
