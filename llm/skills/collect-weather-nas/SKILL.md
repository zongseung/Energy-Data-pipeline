---
name: collect-weather-nas
description: Use when a user asks about 기상청 동네예보(단기예보, 초단기예보, 초단기실황) data, or when NAS 예보 CSV for a region and month is missing and the user wants it collected with the existing weather-data collector.
---

# NAS 기상예보 조회·수집

예보 조회는 `run_sql`로 `research.forecast()` 계열 함수를 직접 호출한다. NAS에 없는 월만 `collect_forecast`로 받는다. ASOS 관측값은 `research.weather_asos`를 `run_sql`로 바로 조회하며 이 스킬이 필요 없다. 이 스킬은 기상 예보 질문의 보조 안내일 뿐, 다른 데이터 질문을 막지 않는다.

## 조회

1. 지역·예보종·요소·기간 중 질문에 이미 있는 것은 그대로 쓰고, 빠진 것은 합리적으로 가정해 먼저 조회한 뒤 가정을 밝힌다. 되묻는 것은 결과가 크게 달라질 때만 한다.
2. 지역 이름이 불확실하면 `research.forecast_regions(예보종, 시도, 시군구)`로 실제 읍면동을, `research.forecast_months(...)`로 있는 월을, `research.forecast_elements(...)`로 요소를 확인한다. 이런 확인 조회는 허락받지 말고 바로 한다.
3. `research.forecast()` 는 NAS 디렉터리를 읽는다. 느려지지 않게 `sido_filter`와 `sigungu_filter`를 함께 주고 기간을 필요한 만큼만 잡는다. 시각은 UTC다(한국 시각은 +9시간).
4. 결과는 표로 보여주고, 사용한 SQL과 `download_url`(CSV 링크)을 함께 안내한다.

## 수집

- 요청한 월이 NAS에 없으면 `collect_forecast(forecast={forecast_type,sido,sigungu,dong,element,from_ym,to_ym}, confirmed=false)`로 누락 월을 확인해 알려준다.
- 사용자가 수집을 요청했을 때만 같은 조건으로 `confirmed=true`를 전달한다. 한 번에 최대 12개월이다. 상태는 반환된 `job_id`를 `forecast_collection_status`에 넣어 확인하고, `completed`이면 다시 조회한다.
- `partial`, `unavailable`, `failed`, `credentials_required`, `nas_unavailable`, `busy`는 반환된 내용 그대로 알린다. 원천에도 없는 자료를 만들거나 다른 기간으로 대체하지 않는다.
- 기상청 계정·비밀번호·NAS 접속 정보는 채팅과 도구 인자에 넣지 않는다. 서버가 처리한다.

## 참고: 백엔드 수집 기준

LibreChat 모델은 shell 명령을 쓰지 않고 위 MCP 도구만 쓴다. 아래는 수집기 쪽 규칙이다.

- 수집 루트는 `/mnt/nvme/weather-data/nas-weather`(실제 SMB 마운트, 조회 DB에는 `/nas-weather:ro`). 마운트가 없으면 로컬 디렉터리로 대신 저장하지 않는다.
- 월별 날짜는 단기예보·초단기예보에서 해당 월 1일부터 다음 월 1일까지, 초단기실황은 해당 월 1일부터 마지막 날까지다. 예: `202101` 단기예보는 `2021-01-01`~`2021-02-01`.
- 지역은 `지역코드 copy.csv`의 `Level1,Level2,Level3`로 정하고, 시군구의 공백 차이(`성남시 수정구`/`성남시수정구`)는 서버가 실제 이름으로 맞춘다. 복정동은 경기도 성남시수정구다.
- 종료 코드나 완료 로그만으로 성공 처리하지 않는다. 요청한 월의 CSV 헤더·데이터 행이 유효한지, 조회 DB에서 월이 보이는지까지 확인된 월만 성공이다.
