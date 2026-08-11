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
import datetime
import os
from decimal import Decimal
from typing import Any

import psycopg2
from mcp.server.fastmcp import FastMCP

DSN_ENV = "ENERGY_MCP_DSN"
TIMEOUT_ENV = "ENERGY_MCP_STATEMENT_TIMEOUT_S"
ROW_LIMIT_ENV = "ENERGY_MCP_ROW_LIMIT"

DEFAULT_TIMEOUT_S = 60
DEFAULT_ROW_LIMIT = 10_000

# 스키마 리소스에 항상 고정으로 박아 넣는 함정 요약. research 스키마의 COMMENT가
# 바뀌거나 누락되더라도 이 6개는 반드시 LLM에게 전달돼야 한다.
KNOWN_PITFALLS_MD = """\
## 반드시 알아야 할 함정 6가지

1. **시간 표기**: 모든 timestamp 컬럼은 이미 **KST 구간시작**으로 통일돼 있다
   (예: 09:00 값은 [09:00, 10:00) 구간). 원천 데이터의 hour-ending 표기는 뷰
   단계에서 이미 보정 완료됐으므로 추가로 시간을 옮기지 마라.
2. **`research.plants.data_quality`는 4단계다**: `정상`(태양광 33기) /
   `시간별무효`(10기, 시간별 분석 금지·일별 합계는 daily_valid_from~to 확인 후
   가능) / `전면무효`(2기, 사용 비권장) / `미검증`(비태양광 46기 — "깨졌다"가
   아니라 "감사 대상이 아니었다"는 뜻. 정상이라 단정하지 마라).
3. **`research.plants.hourly_valid_from`**: `data_quality`가 `시간별무효`여도
   이 날짜 이후는 시간별 값을 믿을 수 있다(영흥태양광 #3 3기, 2025-07-01부터).
   `data_quality`만 보고 걸러내면 13개월치 정상 구간을 통째로 버리게 된다.
4. **`research.plants.is_aggregate`**: true인 행(plant_id 140, 영암태양광_합계)은
   실제 발전소가 아니라 다른 두 발전소(141·142)의 합계 계열이다. 전체 발전량을
   합산할 때 포함하면 이중계상된다.
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


def _execute(query: str) -> dict[str, Any]:
    _reject_multi_statement(query)

    dsn = _require_dsn()
    row_limit = _env_int(ROW_LIMIT_ENV, DEFAULT_ROW_LIMIT)
    timeout_s = _env_int(TIMEOUT_ENV, DEFAULT_TIMEOUT_S)

    with _readonly_cursor(dsn, timeout_s) as cur:
        try:
            cur.execute(query)
        except psycopg2.Error as exc:
            raise RuntimeError(f"SQL 오류: {str(exc).strip()}") from None

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
        if truncated:
            fetched = fetched[:row_limit]

        rows = [dict(zip(columns, (_jsonable(v) for v in row))) for row in fetched]

        result: dict[str, Any] = {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
        }
        if truncated:
            result["note"] = (
                f"결과가 {row_limit}행에서 잘렸습니다 "
                f"({ROW_LIMIT_ENV}로 조정 가능). 조건을 좁혀 다시 조회하세요."
            )
        return result


@mcp.tool()
def run_sql(query: str) -> dict[str, Any]:
    """읽기전용 SQL을 `research` 스키마에 대해 실행한다.

    - SELECT 계열 단일 문장만 실행할 수 있다(세미콜론으로 여러 문장 연결 불가).
    - 세션이 read-only로 고정돼 있어 INSERT/UPDATE/DELETE/DROP 등은 DB가
      거부한다.
    - 행 수는 기본 10,000행으로 제한된다. 응답의 `truncated`가 true면 결과가
      잘린 것이다 — `note`를 확인하라.
    - 코드성 컬럼 값은 **한국어**다. 영어로 번역하지 마라. 예:
      `data_quality IN ('정상','시간별무효','전면무효','미검증')`,
      `fuel_type IN ('solar','wind','hydro','thermal','fuel_cell')` (이건 영어).
    - 먼저 `energy://schema` 리소스를 읽고 테이블/컬럼과 함정을 파악한 뒤
      쿼리를 작성하라.
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
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
