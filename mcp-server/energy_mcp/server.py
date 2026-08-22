"""energy-mcp stdio 서버 — 연구원용 읽기전용 `run_sql` 툴 + `research` 스키마 리소스.

이 파일은 Energy-Data-pipeline 저장소의 다른 코드를 import하지 않는다(독립 패키지).
연구원 본인의 role DSN(`ENERGY_MCP_DSN`)으로 Postgres에 접속하므로 개인별 감사
추적(`log_statement='all'`, role 발급 시 설정됨)이 유지된다.

읽기전용은 두 겹으로 강제한다.
  1) DB 권한 — role 자체가 `research` 스키마 SELECT만 가짐 (여기서 설정하지 않음)
  2) 이 서버 — 커넥션마다 `set_session(readonly=True)`로 세션 전체를 읽기전용으로
     고정한다. 이는 PostgreSQL의 `SET SESSION CHARACTERISTICS AS TRANSACTION
     READ ONLY`를 실행하는 것과 같다 — 정규식으로 SQL 문자열에서 INSERT/UPDATE
     같은 키워드를 걸러내는 방식(우회하기 쉽다)이 아니라, DB 엔진 자체가 쓰기
     문장을 거부하게 만든다.
"""

from __future__ import annotations

import contextlib
import csv
import datetime
import os
import uuid
from decimal import Decimal
from typing import Any

import psycopg2
from mcp.server.fastmcp import FastMCP

from energy_mcp.hints import hint_for

DSN_ENV = "ENERGY_MCP_DSN"
TIMEOUT_ENV = "ENERGY_MCP_STATEMENT_TIMEOUT_S"
ROW_LIMIT_ENV = "ENERGY_MCP_ROW_LIMIT"
# 설정되면 row_limit 초과 결과를 이 디렉터리에 CSV로 떨구고 download_url을 돌려준다.
# (데모 스택 전용 — 정식 MCP 클라이언트에서는 미설정으로 두면 기존 동작 그대로)
EXPORT_DIR_ENV = "ENERGY_MCP_EXPORT_DIR"
EXPORT_URL_ENV = "ENERGY_MCP_EXPORT_URL"

DEFAULT_TIMEOUT_S = 60
DEFAULT_ROW_LIMIT = 10_000
EXPORT_ROW_LIMIT_ENV = "ENERGY_MCP_EXPORT_ROW_LIMIT"
DEFAULT_EXPORT_ROW_LIMIT = 100_000  # CSV 파일 상한 — 초대량 추출은 DB 직접 접속이 정답

# 스키마 리소스에 항상 고정으로 박아 넣는 함정 요약. research 스키마의 COMMENT가
# 바뀌거나 누락되더라도 이 6개는 반드시 LLM에게 전달돼야 한다.
KNOWN_PITFALLS_MD = """\
## 반드시 알아야 할 함정 6가지

1. **시간 표기**: 모든 timestamp 컬럼은 이미 **KST 구간시작**으로 통일돼 있다
   (예: 09:00 값은 [09:00, 10:00) 구간). 원천 데이터의 hour-ending 표기는 뷰
   단계에서 이미 보정 완료됐으므로 추가로 시간을 옮기지 마라.
2. **`research.generation`은 이미 걸러져 있다.** 시간별로 믿을 수 없는 구간
   (`data_quality='전면무효'` 전체, `'시간별무효'`의 `hourly_valid_from` 이전)은
   이 뷰에 아예 없다. 그러니 `data_quality`로 또 필터링하지 마라 — 이중으로
   걸면 영흥태양광 #3의 2025-07-01 이후 정상 구간까지 날아간다.
   `research.plants`에는 여전히 91기 전부가 있으므로, **발전소 개수를 세는
   질문은 `plants`를, 발전량은 `generation`을** 봐야 한다.
3. **`research.plants.data_quality`는 4단계다**: `정상`(태양광 33기) /
   `시간별무효`(10기) / `전면무효`(2기) / `미검증`(비태양광 46기 — "깨졌다"가
   아니라 "감사 대상이 아니었다"는 뜻. 정상이라 단정하지 마라).
   제외된 구간의 **일별** 합계는 유효할 수 있다(`daily_valid_from`~`daily_valid_to`)
   — 필요하면 관리자에게 요청하라고 안내하라.
4. **`research.plants.is_aggregate`**: true인 행(plant_id 140, 영암태양광)은
   개별 호기가 아니라 사업 전체 통합 계열이다. 다만 **지금은 빼면 안 된다** —
   140은 2019~2021, 141·142(영암1·2차)는 2022년부터라 기간이 겹치지 않아
   이중계상이 없고, 빼면 2019~2021 영암 발전량이 통째로 사라진다. 이 플래그는
   "지금 중복"이 아니라 "겹치면 중복될 계열"이라는 표시다.
5. **일사량(`research.weather_asos.solar_radiation`)은 일부 지점만 관측한다.**
   관측 여부는 `has_solar_sensor` 컬럼으로 판별하라 — `solar_radiation IS
   NOT NULL`로 세지 마라(미관측 지점에도 0값 이상치가 극소수 섞여 있다).
   미관측 지점의 `solar_radiation`은 사실상 전부 NULL이다. 이 컬럼의 시간
   라벨이 구간시작/구간종료 중 무엇인지는 아직 "추정" 단계라 보정되지 않았다.
6. **풍력 발전량의 시간 규약은 미확정이다.** `research.plants.data_quality`가
   `미검증`으로 뜨고, `research.generation.timestamp`는 풍력(`fuel_type='wind'`)에
   한해 보정을 적용하지 않았다 — 실제 값과 ±1시간 어긋날 수 있다.
"""

RESOURCE_URI = "energy://schema"

mcp = FastMCP("energy-mcp")


# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{name} 환경변수는 정수여야 합니다: {raw!r}") from None
    if value <= 0:
        raise RuntimeError(f"{name} 환경변수는 양수여야 합니다: {value}")
    return value


def _require_dsn() -> str:
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        raise RuntimeError(
            f"{DSN_ENV} 환경변수가 설정되지 않았습니다. "
            "연구원 본인의 읽기전용 role DSN을 설정하세요."
        )
    return dsn


def _connect(dsn: str) -> "psycopg2.extensions.connection":
    """실제 접속. 테스트에서 이 함수만 monkeypatch하면 DB 없이 검증할 수 있다."""
    return psycopg2.connect(dsn)


@contextlib.contextmanager
def _readonly_cursor(dsn: str, timeout_s: int):
    """읽기전용 세션을 강제한 커넥션에서 커서를 연다.

    `run_sql` 실행 경로와 스키마 리소스 조회 경로가 이 함수 하나를 공유한다 —
    읽기전용 강제(`set_session(readonly=True)`) 로직이 두 군데로 중복되면
    한쪽만 고치고 다른 쪽을 놓치는 사고가 나기 쉽다.
    """
    try:
        conn = _connect(dsn)
    except psycopg2.Error as exc:
        raise RuntimeError(f"DB 연결 실패: {exc}") from None

    try:
        # 애플리케이션 층 읽기전용 강제 (DB role 권한과는 별도의 방어선).
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {timeout_s * 1000}")
            yield cur
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# 쿼리 실행
# ---------------------------------------------------------------------------


def _reject_multi_statement(query: str) -> None:
    """세미콜론으로 이어진 여러 문장을 거부한다.

    read-only 세션은 데이터 변경만 막을 뿐 `SET statement_timeout = 0` 같은
    세션 설정 변경은 막지 않는다. 한 호출에 문장 하나만 허용하면 우리가 앞서
    설정한 statement_timeout을 뒤 문장이 덮어쓸 길이 없어진다.
    """
    body = query.strip()
    if not body:
        raise ValueError("빈 쿼리입니다.")
    if body.endswith(";"):
        body = body[:-1]
    if ";" in body:
        raise ValueError(
            "한 번에 하나의 SQL 문장만 실행할 수 있습니다. "
            "세미콜론으로 여러 문장을 연결하지 마세요."
        )


def _jsonable(value: Any) -> Any:
    """JSON으로 안전하게 직렬화되지 않는 Postgres 타입을 변환한다."""
    if isinstance(value, Decimal):
        return str(value)  # float 반올림으로 인한 정밀도 손실 방지 (가격·발전량)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return value


def _summarize(columns: list[str], raw_rows: list[tuple]) -> dict[str, Any]:
    """대량 결과의 미리보기 대체 — 컬럼별 최소/최대/중간값만 계산한다.

    수치 컬럼: min/max/median, 시간 컬럼: min/max. 그 외 타입은 건너뛴다.
    LLM 이 미리보기 행을 표로 다시 그리느라 느려지는 것을 막기 위한 것.
    """
    summary: dict[str, Any] = {}
    for i, col in enumerate(columns):
        values = [r[i] for r in raw_rows if r[i] is not None]
        if not values:
            continue
        v = values[0]
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float, Decimal)):
            values.sort()
            summary[col] = {
                "min": _jsonable(values[0]),
                "max": _jsonable(values[-1]),
                "median": _jsonable(values[len(values) // 2]),
            }
        elif isinstance(v, (datetime.datetime, datetime.date)):
            summary[col] = {
                "min": _jsonable(min(values)),
                "max": _jsonable(max(values)),
            }
    return summary


def _execute(query: str) -> dict[str, Any]:
    _reject_multi_statement(query)

    dsn = _require_dsn()
    row_limit = _env_int(ROW_LIMIT_ENV, DEFAULT_ROW_LIMIT)
    timeout_s = _env_int(TIMEOUT_ENV, DEFAULT_TIMEOUT_S)

    with _readonly_cursor(dsn, timeout_s) as cur:
        try:
            cur.execute(query)
        except psycopg2.Error as exc:
            message = f"SQL 오류: {str(exc).strip()}"
            hint = hint_for(message)
            if hint:
                message = f"{message}\n{hint}"
            raise RuntimeError(message) from None

        if cur.description is None:
            return {
                "columns": [],
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "note": "결과 집합이 없는 쿼리입니다.",
            }

        columns = [d.name for d in cur.description]
        fetched = cur.fetchmany(row_limit + 1)
        truncated = len(fetched) > row_limit
        preview = fetched[:row_limit] if truncated else fetched

        rows = [dict(zip(columns, (_jsonable(v) for v in row))) for row in preview]

        result: dict[str, Any] = {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
        }

        export_dir = os.environ.get(EXPORT_DIR_ENV)
        if export_dir and fetched:
            export_limit = _env_int(EXPORT_ROW_LIMIT_ENV, DEFAULT_EXPORT_ROW_LIMIT)
            remainder = (
                cur.fetchmany(export_limit - len(fetched)) if truncated else []
            )
            name = f"export-{uuid.uuid4().hex[:8]}.csv"
            # utf-8-sig: 엑셀이 한글 컬럼을 바로 읽도록 BOM 포함
            with open(os.path.join(export_dir, name), "w", newline="",
                      encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(fetched)
                writer.writerows(remainder)
            total = len(fetched) + len(remainder)
            capped = total >= export_limit
            base = os.environ.get(EXPORT_URL_ENV, "http://localhost:8098").rstrip("/")
            result["download_url"] = f"{base}/{name}"
            result["download_rows"] = total
            if truncated:
                # 대량 결과에는 컬럼별 요약통계를 함께 준다 — 미리보기 몇 행만
                # 보고 전체 경향을 일반화하는 것을 막는다. 실데이터는 CSV 링크로.
                result["summary"] = _summarize(columns, fetched + remainder)
            result["note"] = (
                f"미리보기 {len(rows)}행. 전체 {total}행"
                + (" (파일 상한 도달 — 기간을 나눠 조회하면 나머지를 받을 수 있다)"
                   if capped else "")
                + "은 download_url 에서 CSV 파일로 받을 수 있다 — 답변에 이 "
                "다운로드 링크를 반드시 포함하라. pandas 로 읽을 때는 "
                "read_csv(경로, parse_dates=['timestamp'], index_col='timestamp') "
                "처럼 시간 컬럼을 지정하라고 함께 안내하라."
            )
        elif truncated:
            result["note"] = (
                f"결과가 {row_limit}행에서 잘렸습니다 "
                f"({ROW_LIMIT_ENV}로 조정 가능). 조건을 좁혀 다시 조회하세요."
            )
        return result


@mcp.tool()
def run_sql(query: str) -> dict[str, Any]:
    """읽기전용 SQL을 `research` 스키마에 대해 실행한다.

    ## 반드시 지킬 3가지 (틀리면 답 자체가 틀린다)

    1. **연료를 지정한 질문에는 반드시 `fuel_type` 을 WHERE 에 넣어라.**
       `research.generation` 은 태양광 전용이 아니라 5개 연료가 전부 섞인
       뷰다(solar 146만행 / thermal 95만 / wind 36만 / fuel_cell 32만 /
       hydro 16만). 필터를 빠뜨리면 "태양광 상위 5곳" 질문에 화력발전소가
       나온다.
         태양광→'solar'  풍력→'wind'  수력→'hydro'  화력→'thermal'
         연료전지→'fuel_cell'
    2. **"발전소별/월별/지역별/연도별 …" 집계 질문에는 GROUP BY 와 집계함수를
       같이 써라.** 원시 행을 ORDER BY … LIMIT 로 자르면 같은 발전소의 시간별
       행이 반복돼 순위가 통째로 틀린다. 그리고 GROUP BY 를 쓸 때는 `SELECT *`
       를 쓰면 안 된다 — 묶지 않은 컬럼이 있다고 에러난다.
         틀림: SELECT plant_name, gen_kwh FROM … ORDER BY gen_kwh DESC LIMIT 5
         틀림: SELECT * FROM … GROUP BY plant_name
         맞음: SELECT plant_name, sum(gen_kwh) AS total FROM …
               GROUP BY plant_name ORDER BY total DESC LIMIT 5
    3. **PostgreSQL 문법이다.** `round(x, n)` 의 x 는 numeric 이어야 한다 —
       `round(avg(price)::numeric, 1)` 처럼 캐스트하라.

    ## 나머지 규칙

    - **한 번에 한 문장만** 실행할 수 있다. 세미콜론으로 두 문장을 이어 보내면
      무조건 에러다. "A 와 B 를 같이 보여줘" 는 JOIN 하나로 쓰거나, run_sql 을
      **두 번 나눠서** 호출하라 — 한 번에 두 문장을 보내는 선택지는 없다.
    - 데이터 추출/다운로드/CSV 요청에는 컬럼을 고르지 말고 `SELECT *` 를 써라 —
      timestamp·plant_name 같은 식별 컬럼이 빠진 CSV 는 쓸모가 없다.
      **단 집계(GROUP BY) 쿼리에는 절대 `SELECT *` 를 쓰지 마라** (위 규칙 2).
    - 세션이 read-only로 고정돼 있어 INSERT/UPDATE/DELETE/DROP 등은 DB가
      거부한다.
    - 행 수는 기본 10,000행으로 제한된다. 응답의 `truncated`가 true면 결과가
      잘린 것이다 — `note`를 확인하라.
    - 코드성 컬럼 값은 **한국어**다. 영어로 번역하지 마라. 예:
      `data_quality IN ('정상','시간별무효','전면무효','미검증')`,
      `fuel_type IN ('solar','wind','hydro','thermal','fuel_cell')` (이건 영어).
    - 존재하는 뷰는 아래가 전부다. 다른 테이블 이름을 지어내지 마라:
      - `research.plants` — 발전소 마스터(plant_id, plant_name, fuel_type,
        capacity_mw, data_quality, is_aggregate). is_aggregate=true는 합계
        계열이므로 합산에서 제외하라.
      - `research.generation` — 시간별 발전량(timestamp, plant_id, plant_name,
        fuel_type, gen_kwh). plants와 plant_id로 조인돼 있다. **5개 연료가 모두
        섞여 있다 — 위 규칙 1 참조.** 시간별로 신뢰할 수 없는 구간은 이 뷰에서
        이미 제외돼 있으니 data_quality 로 또 거를 필요는 없다.
      - `research.smp_hourly` — 하루전 시간별 SMP. 컬럼은 timestamp,
        **region('land'|'jeju'|'unified')**, price 세 개뿐이다. 육지는
        `region='land'`, 제주는 `'jeju'`(2010-01-01 이전은 단일시장이라
        `'unified'`). station_type 같은 컬럼은 없다.
      - `research.smp_realtime_jeju` — 제주 실시간 15분 SMP(timestamp, region,
        price, is_confirmed). 음수 가격이 정상적으로 나온다.
      - `research.smp_weighted_avg` — SMP 가중평균.
      - `research.weather_asos` — 시간별 기상(timestamp, station_name,
        temperature, humidity, solar_radiation, has_solar_sensor).
      - `research.jeju_supply_demand` — 제주 계통 수급 5분(실시간, timestamp,
        supply_mw, demand_mw, renewable_total_mw, solar_mw, wind_mw).
      - `research.gen_mix_5min` — **전국 발전원별 5분 실시간**(timestamp,
        solar_market, solar_ppa, solar_btm, wind, nuclear, gas, coal_total,
        coal_domestic, oil, hydro, pumped, renewable_total, renewable_new,
        renewable_renew, ess). 단위 MW. **태양광이 세 갈래다** — solar_market 만
        계량값이고 solar_ppa·solar_btm 은 KPX 추정치다. "지금 태양광 얼마나
        발전하나" 같은 실시간 질문은 이 뷰를 써라(generation 은 하루~수개월
        지연된다). 2026-08-23 수집 시작이라 그 이전 구간은 없다.
      - `research.jeju_demand_hourly` — 제주 시간별 수요(timestamp, demand_mw,
        source). 분기 파일이 원천이라 최신 분기는 비어 있을 수 있다.
      - `research.jeju_generation_mix` — 제주 연료원별 시간별 거래량(timestamp,
        fuel_type, gen_mwh). **2024년 1년치가 전부다.** fuel_type 은
        lng/oil/solar/wind/renewable_other/other 로 plants 의 코드와 다르다.
      - `research.aws_obs_daily` — 방재기상(AWS)·종관기상(ASOS) **일자료**
        (date, station_id, station_name, station_type, ta, wd, ws, rn_day,
        rn_hr1, hm, pa, ps, imputed). 시간별이 아니다 — 시간별은
        weather_asos 를 써라. imputed 가 비지 않은 행은 결측 보정값이다.
      - `research.demand_5min` — 전국 5분 전력수요. **컬럼 이름이 의미와
        어긋난다**: 공급능력은 `current_supply`, 최대예측수요는
        `supply_capacity` 다(이름이 서로 바뀐 것처럼 보이지만 그게 맞다).
        그 외 timestamp, current_demand, supply_reserve, reserve_rate.
      - `research.demand_weather_1h` — 전국 수요+기상 시간별 결합
        (timestamp, station_name, temperature, humidity, demand_avg).
        지점 수만큼 수요값이 반복되므로 수요만 필요하면 demand_5min 을 써라.
      - `research.heat_demand` / `research.heat_demand_location` — 열수요
        (2023년까지의 정적 데이터셋).
      - `research.substations` — OSM 기반 변전소 위치(name, voltage, operator,
        sido, lon, lat). 전국 1,185개 — 공식 전수 아님.
      - `research.power_lines` — OSM 기반 송전선로(name, power_type, voltage,
        sido, length_km). 전국 4,685개.
      - `research.kepco_grid` — 한전 배전망 접속 여유용량(2026-03 스냅샷,
        리·지번 주소 단위 361만 행). **반드시 addr_do 로 필터**하라. 시군구는
        addr_si(시)·addr_gu(군·구) 로 나뉘고 빈 쪽은 '-기타지역' 채움값이며
        지역별 누락이 있다 — 먼저 DISTINCT 로 존재를 확인하라. 설비 기준
        집계는 DISTINCT subst_cd·dl_nm 으로(주소 수만큼 반복됨).
    - **기상 질문의 90%는 예보가 아니라 관측이다.** "기온·습도·일사량이
      얼마였나", "발전량과 날씨를 같이 보자" 처럼 **실제로 어땠는지** 묻는
      질문에는 반드시 `research.weather_asos` 를 써라. 아래 `research.forecast`
      는 **"기상청이 뭐라고 예보했나"** 를 물을 때만 쓴다 — 질문에 '예보'라는
      말이 없으면 쓰지 마라. 둘을 섞어 조인하지도 마라.
    - 기상청 동네예보 3종은 뷰가 아니라 **함수**로 제공된다(NAS 에서 직접 읽으며
      적재본이 없다). 반드시 읍면동과 요소를 지정하고 기간을 좁혀서 불러라:
        `SELECT * FROM research.forecast('단기예보','개포1동','1시간기온','202301','202303')`
      - 인자: 예보종(`단기예보`|`초단기예보`|`초단기실황`), 읍면동, 요소,
        시작 YYYYMM, 종료 YYYYMM. 기간을 생략하면 30개월치를 통째로 읽는다.
      - 결과: sido, sigungu, dong_name, element_name, grid, base_at(발표시각),
        lead_hours(예보 리드타임), target_at(대상시각), value.
        초단기실황은 관측값이라 lead_hours 가 NULL 이고 target_at = base_at 이다.
      - 요소 이름을 모르면 `SELECT * FROM research.forecast_elements('단기예보')`,
        지역 이름을 모르면 `SELECT * FROM research.forecast_regions('단기예보','서울특별시')`
        를 먼저 불러라. 요소·읍면동 이름을 지어내면 에러가 난다.
    - 상세 컬럼·함정은 `energy://schema` 리소스에 있다(읽을 수 있는 클라이언트만).
    - 조회 결과는 **마크다운 표**로 정리해 보여줘라.
    - 응답에 `download_url`이 있으면 반드시 마크다운 링크로 안내하라 —
      전체 데이터를 CSV 파일로 내려받는 링크다.
    - 답변 끝에 사용한 SQL을 ```sql 코드블록으로 항상 보여줘라 — 사용자가
      DB 클라이언트에 붙여넣어 전체 데이터를 직접 추출할 수 있게.
    """
    return _execute(query)


# ---------------------------------------------------------------------------
# 스키마 리소스
# ---------------------------------------------------------------------------

_SCHEMA_QUERY = """
SELECT
    c.table_name,
    obj_description(format('research.%I', c.table_name)::regclass, 'pg_class')
        AS view_comment,
    c.column_name,
    c.data_type,
    col_description(
        format('research.%I', c.table_name)::regclass, c.ordinal_position
    ) AS column_comment
FROM information_schema.columns c
WHERE c.table_schema = 'research'
ORDER BY c.table_name, c.ordinal_position;
"""


def _fetch_schema_markdown() -> str:
    dsn = _require_dsn()
    timeout_s = _env_int(TIMEOUT_ENV, DEFAULT_TIMEOUT_S)

    with _readonly_cursor(dsn, timeout_s) as cur:
        cur.execute(_SCHEMA_QUERY)
        table_rows = cur.fetchall()

    return _render_schema_markdown(table_rows)


def _render_schema_markdown(table_rows: list[tuple]) -> str:
    sections: dict[str, dict[str, Any]] = {}
    for table_name, view_comment, column_name, data_type, column_comment in table_rows:
        section = sections.setdefault(
            table_name, {"comment": view_comment, "columns": []}
        )
        section["columns"].append((column_name, data_type, column_comment))

    lines = ["# research 스키마 사전", "", KNOWN_PITFALLS_MD, "## 뷰 목록", ""]
    for table_name in sorted(sections):
        section = sections[table_name]
        lines.append(f"### research.{table_name}")
        if section["comment"]:
            lines.append(f"{section['comment']}")
        lines.append("")
        lines.append("| 컬럼 | 타입 | 설명 |")
        lines.append("|---|---|---|")
        for column_name, data_type, column_comment in section["columns"]:
            note = (column_comment or "").replace("\n", " ").replace("|", "\\|")
            lines.append(f"| {column_name} | {data_type} | {note} |")
        lines.append("")

    if not sections:
        lines.append(
            "(뷰를 찾지 못했습니다 — ENERGY_MCP_DSN이 가리키는 DB에 "
            "`research` 스키마가 없거나 접속 role에 권한이 없습니다.)"
        )

    return "\n".join(lines)


@mcp.resource(RESOURCE_URI)
def schema_dictionary() -> str:
    """`research` 스키마의 뷰·컬럼·COMMENT를 DB에서 직접 읽어 마크다운으로 낸다.

    정적 문서를 따로 관리하지 않고 매번 실제 DB 상태를 읽으므로 뷰 정의가
    바뀌어도 어긋나지 않는다. 6대 함정은 DB COMMENT와 무관하게 항상 포함된다.
    """
    return _fetch_schema_markdown()


def main() -> None:
    # stdio(기본) 외에 streamable-http 를 지원한다 — LibreChat 처럼 별도
    # 컨테이너에서 접속하는 클라이언트용. (mcp SDK 1.29 는 FASTMCP_* 환경변수를
    # 읽지 않아 settings 에 직접 넣는다. 기본 바인드는 127.0.0.1:8000)
    transport = os.environ.get("ENERGY_MCP_TRANSPORT", "stdio")
    if transport != "stdio":
        from mcp.server.transport_security import TransportSecuritySettings

        mcp.settings.host = os.environ.get("ENERGY_MCP_HOST", "127.0.0.1")
        mcp.settings.port = int(os.environ.get("ENERGY_MCP_PORT", "8000"))
        # 기본 DNS rebinding 보호는 Host 가 localhost 가 아니면 421 을 준다.
        # 이 포트는 도커 내부망 전용(호스트 미공개)이라 보호가 불필요하다.
        mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=False
        )
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
