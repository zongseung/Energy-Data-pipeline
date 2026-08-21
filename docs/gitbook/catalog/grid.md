# 송배전망

변전소 위치·송전선로와 한전 배전망 접속 여유용량. 물리적으로는 energy-hub DB에 있지만 FDW로 연결돼 있어 pv DB 계정 하나로 발전소·발전량과 함께 조회하고 조인할 수 있습니다. 복제본이 아니라 라이브 프록시입니다.

<figure><img src="https://raw.githubusercontent.com/zongseung/Energy-Data-pipeline/main/docs/gitbook/images/grid_map.png" alt="OSM 송전선로·변전소와 발전소 위치 지도"><figcaption><p>OSM 송전선로(빨강 345kV+·파랑 154kV)·변전소(검정 삼각형)와 research.plants 발전소 위치. 제주 HVDC 연계선까지 보인다.</p></figcaption></figure>

***

## 성격이 다른 두 계열

| 계열                            | 출처               | 위치 표현       | 쓰임                        |
| ----------------------------- | ---------------- | ----------- | ------------------------- |
| `substations` · `power_lines` | OSM(오픈스트리트맵)     | 좌표 (WGS84)  | 지도 표시·근접 탐색. **공식 전수 아님** |
| `kepco_grid`                  | 한전 분산전원 연계정보 크롤링 | 리·지번 **주소** | 배전망 접속 여유용량 조회            |

셋 다 정적 스냅샷입니다. 갱신 파이프라인이 없으므로 "지금" 값이 필요한 질문에는 맞지 않습니다.

### research.substations — 변전소 위치

| 항목 | 값                 |
| -- | ----------------- |
| 행수 | 1,185 (전국 17개 시도) |
| 갱신 | 없음 (OSM 스냅샷)      |

| 컬럼                 | 의미            | 비고                                                 |
| ------------------ | ------------- | -------------------------------------------------- |
| `name` / `name_en` | 변전소명 (한/영)    | OSM 특성상 **NULL인 행이 있습니다**                          |
| `voltage`          | 전압(V) 문자열     | 다중 전압은 `;` 연결 (예: `154000;55000;27500`). NULL 144개 |
| `operator`         | 운영사           |                                                    |
| `sido`             | 시도            |                                                    |
| `lon` / `lat`      | 경도/위도 (WGS84) |                                                    |

{% hint style="warning" %}
OSM 기반이라 **공식 전수가 아닙니다.** 누락·좌표 오차·이름 공백이 있을 수 있습니다. "○○ 근처 변전소" 같은 탐색용으로 쓰고 인허가·계통 검토처럼 공식 근거가 필요한 일에는 쓰지 마세요.
{% endhint %}

```sql
-- 태양광 발전소별 최근접 변전소 — 위경도 근사거리(도당 111km)
SELECT p.plant_name, s.name AS substation, s.voltage,
       round((111 * sqrt((p.lat-s.lat)^2 + (cos(radians(p.lat))*(p.lon-s.lon))^2))::numeric, 1) AS approx_km
FROM research.plants p
JOIN LATERAL (
  SELECT * FROM research.substations s
  ORDER BY (p.lat-s.lat)^2 + (cos(radians(p.lat))*(p.lon-s.lon))^2
  LIMIT 1
) s ON true
WHERE p.fuel_type = 'solar' AND p.lat IS NOT NULL AND NOT p.is_aggregate;
```

### research.power\_lines — 송전선로

| 항목 | 값            |
| -- | ------------ |
| 행수 | 4,685 (전국)   |
| 갱신 | 없음 (OSM 스냅샷) |

컬럼은 `name`, `power_type`(line 가공 / cable 지중 / minor\_line 소규모), `voltage`, `sido`, `length_km`(좌표 기하에서 계산한 선로 길이)입니다. 선형 좌표 자체는 노출하지 않습니다 — 필요하면 energy-hub DB의 PostGIS 원본(`power_line`)을 쓰세요.

### research.kepco\_grid — 한전 배전망 접속 여유용량

| 항목    | 값                                    |
| ----- | ------------------------------------ |
| 행수    | 3,611,724 (리·지번 주소 단위)               |
| 기준 시점 | 2026-03-23 \~ 03-30 크롤링 (**정적 스냅샷**) |
| 설비 규모 | 변전소 681 · 배전선로(DL) 3,954             |

주소 컬럼(`addr_do`/`addr_si`/`addr_gu`/`addr_lidong`/`addr_li`/`addr_jibun`)과 설비 3계층으로 구성됩니다. 계층은 변전소(`subst_nm`, `subst_pwr`, `subst_capa`, `g_subst_capa`), 주변압기(`mtr_*`), 배전선로(`dl_nm`, `dl_pwr`, `dl_capa`, `g_dl_capa`) 순입니다. `g_` 접두는 여유용량으로 보입니다. **용량 단위는 원천 문서를 확인하지 못해 kW 추정**입니다.

{% hint style="danger" %}
**함정 셋.** ① 행이 설비가 아니라 **주소** 단위입니다 — 같은 변전소·선로 값이 주소 수만큼 반복되므로 설비 기준 집계는 반드시 `DISTINCT subst_cd`(변전소)·`DISTINCT dl_nm`(선로)으로 하세요. ② 361만 행이라 `addr_do` 필터 없이 조회하면 느립니다. ③ `dl_cd`는 전체에 39종뿐이라 식별자로 못 씁니다 — 선로는 `dl_nm`으로 구분하세요.
{% endhint %}

{% hint style="warning" %}
**시군구 커버리지가 고르지 않습니다.** 시(市)는 `addr_si`, 군(郡)·구(區)는 `addr_gu`에 들어갑니다. 빈 쪽은 `-기타지역` 채움값입니다. 지역별 누락도 있습니다 — 예를 들어 전라남도는 군 단위만 있고 나주·목포 등 시 단위가 아예 없습니다. 특정 지역을 조회하기 전에 `SELECT DISTINCT addr_si, addr_gu FROM research.kepco_grid WHERE addr_do = '…'`로 존재부터 확인하세요.
{% endhint %}

```sql
-- 전라남도 고흥군의 배전선로별 접속 여유용량 상위 10
SELECT dl_nm, max(g_dl_capa) AS free_capa
FROM research.kepco_grid
WHERE addr_do = '전라남도' AND addr_gu = '고흥군'
GROUP BY dl_nm
ORDER BY free_capa DESC
LIMIT 10;
```
