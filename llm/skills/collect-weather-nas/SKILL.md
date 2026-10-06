---
name: collect-weather-nas
description: Use when a user requests missing 기상청 동네예보 CSV data for NAS-backed queries in LibreChat, or asks to collect 단기예보, 초단기예보, or 초단기실황 with the existing weather-data collector.
---

# NAS 기상예보 수집

기존 `weather-data` 수집기를 실행해 NAS에 없는 월 자료를 받고, 기존 MCP 승인·조회 흐름으로 실제 값을 반환한다. ASOS 관측 수집에는 적용하지 않는다.

## 실행 환경

- 코드: `/mnt/nvme/weather-data`, Python: `/mnt/nvme/weather-data/.venv/bin/python`.
- 저장 루트: `/mnt/nvme/weather-data/nas-weather`. 실제 SMB(CIFS) 마운트이며, 조회 DB에는 `/nas-weather:ro`로 연결돼 있다.
- 수집 작업자는 코드와 NAS에 접근하고 NAS 쓰기 권한을 가져야 한다. 실행 전 `findmnt --target /mnt/nvme/weather-data/nas-weather --output TARGET,FSTYPE`로 해당 경로 자체가 CIFS 마운트인지 확인한다. 마운트가 없으면 로컬 디렉터리를 만들어 대신 저장하지 않는다.
- LibreChat에서는 연결된 `energy-db` MCP의 `collect_forecast`와 `forecast_collection_status`를 사용한다. 기존 수집기는 MCP 서버에서 실행되므로 채팅 모델이 직접 NAS나 shell에 접근할 필요가 없다. 이 도구가 없는 실행 환경에서는 수집 실행이 불가능하다고 알린다. 외부 API의 실행 환경은 이 서버 경로에 자동으로 접근하지 못한다.

## 조건과 계정

시도, 시군구, 읍면동, 예보종, 요소, 시작월·종료월(`YYYYMM`)을 확정한다. 질문에 이미 있는 조건은 재사용하고 빠진 조건만 묻는다. 지역 목록은 실제 예보 값이나 파일 존재를 증명하지 않는다.

지역은 `/mnt/nvme/weather-data/지역코드 copy.csv`의 `Level1,Level2,Level3`와 정확히 일치하는 행으로 선택하고 `ReqList_Last`를 사용한다. 예보종과 요소는 `src/weather_downloader/config.py`의 `WeatherConfig`로 검증한다. 초단기실황 기온의 요소명은 `기온`이다.

기상청 계정은 실행 백엔드의 자격증명 또는 `KMA_ID`, `KMA_PW`를 사용한다. 비밀번호·쿠키·NAS 접속 정보는 채팅, 모델 인자, 생성 파일, 로그에 넣지 않는다. 계정이 없으면 서비스의 계정 등록 경로를 안내한다. 사용자별 `connection_id`를 받는 환경에서는 백엔드가 사용자 소유권을 확인하고 계정을 주입해야 한다. 이 연결 ID 처리 기능이 이미 구현됐다고 가정하지 않는다.

## 실행 순서

1. `collect_forecast`에 `forecast={forecast_type,sido,sigungu,dong,element,from_ym,to_ym}`와 `confirmed=false`를 전달해 실제 파일 월을 확인한다. `available`이면 수집하지 않고 기존 조회로 진행한다. `needs_collection_confirmation`이면 반환된 누락 월을 안내한다.
2. 사용자가 수집을 요청했으면 같은 조건에 `confirmed=true`로 `collect_forecast`를 호출한다. 비밀번호는 인자로 전달하지 않는다. `running`의 실제 `job_id`를 `forecast_collection_status`에 전달해 상태를 확인한다. `completed`이면 재조회하고, `partial`, `unavailable`, `failed`, `credentials_required`, `nas_unavailable`, `busy`는 누락·오류를 그대로 안내한다. SQL 승인 전 자동 실행은 허용하지 않는다.

## 백엔드 실행 기준

다음은 연결된 작업자의 실행 기준이다. LibreChat 모델은 shell 명령 대신 위 MCP 도구를 사용한다.

1. 수집 요청이 허용된 범위에서 없는 월만 작업으로 만든다. 작업별 임시 지역 CSV에는 선택한 읍면동 한 행만, 임시 설정에는 예보종 하나와 요소 하나만 넣는다. 공유 `config.json`, 전체 지역 CSV를 수정하거나 기존 원장 기록을 삭제하지 않는다. 수집기가 작업 결과를 원장에 추가하는 정상 동작은 유지한다.
2. 월별 날짜는 단기예보·초단기예보에서 해당 월 1일부터 다음 월 1일까지, 초단기실황에서 해당 월 1일부터 마지막 날까지 지정한다. 예: `202101` 단기예보는 `2021-01-01`~`2021-02-01`. 초단기실황에 다음 월 1일을 넣으면 다음 월도 수집하므로 구분한다.
3. 기존 수집기를 NAS 루트에 직접 저장하도록 실행한다. 로그인 없는 임시 설정의 `login`은 `{}`로 두고 백엔드 환경의 계정을 사용한다. 설정의 `date_range`, `forecast_types`, `variables_by_type`에는 확정한 월·예보종·요소만 넣는다.

   ```bash
   SLACK_WEBHOOK_URL= WEATHER_OUT_DIR=/mnt/nvme/weather-data/nas-weather \
     /mnt/nvme/weather-data/.venv/bin/python \
     /mnt/nvme/weather-data/src/scripts/run_collection.py \
     --config "$job_config" --csv "$job_region_csv" --concurrency 1
   ```

   `job_config`, `job_region_csv`는 백엔드가 만든 작업별 임시 파일 경로다. Python에서 직접 호출하면 `WeatherDownloader(out_dir="/mnt/nvme/weather-data/nas-weather")`와 `DownloadConfig`를 사용한다. `out_dir` 아래에 예보종부터 붙는다. 임시 디렉터리를 `out_dir`로 쓰면 수집기의 마운트 검사에서 대기하므로 마운트 검사에 사용하는 `out_dir`는 NAS 경로로 유지한다. 운영 MCP 작업자는 다운로드를 로컬 임시 경로에서 검증한 뒤 NAS에 원자적으로 반영한다. 별도 알림 요청이 없으면 실행 프로세스의 `SLACK_WEBHOOK_URL`을 빈 문자열로 설정한다. 변수 삭제만 하면 CLI가 `.env`에서 다시 로드하므로 빈 값으로 유지한다.

4. 장시간 재시도·마운트 대기가 있는 수집기는 작업으로 실행한다. 실행기가 반환한 작업 ID가 있으면 상태를 확인하고, 없으면 작업 ID를 만들어 답하지 않는다. 마운트 장애·인증 오류·미수신을 자료 없음과 구분하고 자동으로 작업을 반복 제출하지 않는다.
5. 작업 종료 후 요청한 월의 CSV 헤더·데이터 행·요소 값이 유효한지 확인하고 조회 DB에서도 월 존재를 다시 확인한다. 종료 코드나 완료 로그만으로 성공 처리하지 않는다. 기존 원장에 `done`이지만 파일이 없어 건너뛰었으면 누락으로 보고하며 원장을 자동 삭제하지 않는다. 미수신이면 성공 월과 누락 월을 구분한다. 원천에도 없는 자료를 생성하거나 다른 기간으로 대체하지 않는다.
6. 검증된 자료로 `plan_query`를 호출한다. 첫 호출의 `answers`에도 일곱 조건과 출력 방식을 전달한다. 구체화 중인 workflow는 같은 ID로 이어가고, 만료되거나 종료된 workflow는 새로 계획한다. SQL·승인 링크를 보여주고 사용자의 기존 승인 절차가 끝난 뒤 `execute_query`로 조회한다.

응답에는 확정 조건, 수집·누락 월, 실제 조회 결과와 CSV 링크를 표시한다. SQL을 실행했다면 실행 SQL도 제공한다. `SKILL.md` 작성이나 등록만으로 수집·저장·조회가 완료됐다고 말하지 않는다.
