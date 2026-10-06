from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from energy_mcp import planner, server


CONDITIONS = dict(forecast_type="단기예보", sido="서울특별시", sigungu="중구",
                  dong="필동", element="1시간기온", from_ym="202301", to_ym="202301")


def decision(**changes):
    return planner.PlannerDecision(status="ready", summary="필동 기온 예보",
                                   forecast=CONDITIONS | changes)


@pytest.mark.parametrize("changes", [dict(from_ym="202313"), dict(to_ym="202212"),
                                    dict(dong=""), dict(forecast_type="../../etc")])
def test_forecast_rejects_invalid_conditions(changes):
    with pytest.raises(ValidationError):
        planner.ForecastRequest(**(CONDITIONS | changes))


def test_raw_forecast_sql_cannot_bypass_structured_conditions():
    with pytest.raises(ValidationError, match="forecast"):
        planner.PlannerDecision(status="ready", summary="기온",
                               sql="SELECT * FROM research.forecast('단기예보','서울특별시','필동')")


def test_forecast_cte_cannot_be_schema_qualified():
    with pytest.raises(ValidationError, match="forecast_data"):
        planner.PlannerDecision(status="ready", summary="기온", forecast=CONDITIONS,
                               sql="SELECT * FROM research.forecast_data")


def prepare(monkeypatch, months, request):
    cursor = MagicMock()
    canonical = '성남시수정구' if request.forecast.sigungu == '성남시 수정구' else request.forecast.sigungu
    cursor.fetchall.side_effect = lambda: ([(canonical,)] if 'forecast_regions' in cursor.execute.call_args.args[0]
                                          else [(month,) for month in months])
    cursor.mogrify.side_effect = lambda sql, params: (sql % tuple("'" + p.replace("'", "''") + "'" for p in params)).encode()

    @contextmanager
    def readonly(*args):
        yield cursor

    monkeypatch.setenv(server.DSN_ENV, "postgresql://invalid")
    monkeypatch.setattr(server, "_readonly_cursor", readonly)
    return server._prepare_forecast_decision(request)


def test_forecast_uses_named_arguments_instead_of_llm_sql(monkeypatch):
    result = prepare(monkeypatch, ["202301"], decision())
    assert result.status == "ready"
    assert "sido_filter => '서울특별시'" in result.sql
    assert "sigungu_filter => '중구'" in result.sql
    assert "dong => '필동'" in result.sql
    assert "element => '1시간기온'" in result.sql
    assert "from_ym => '202301'" in result.sql
    assert result.condition_mapping()["시군구"] == "중구"
    assert result.condition_mapping()["요소"] == "1시간기온"


def test_forecast_resolves_spaced_district_to_actual_nas_name(monkeypatch):
    result = prepare(monkeypatch, ['202301'], decision(sido='경기도', sigungu='성남시 수정구', dong='복정동'))
    assert result.status == 'ready'
    assert result.forecast.sigungu == '성남시수정구'
    assert "sigungu_filter => '성남시수정구'" in result.sql


def test_forecast_preserves_spaces_in_actual_catalog_name(monkeypatch):
    result = prepare(monkeypatch, ['202301'], decision(sido='경상남도', sigungu='창원시 마산합포구', dong='가포동'))
    assert result.forecast.sigungu == '창원시 마산합포구'
    assert "sigungu_filter => '창원시 마산합포구'" in result.sql


def test_unavailable_year_asks_again_without_saving_executable_sql(monkeypatch):
    result = prepare(monkeypatch, ["202301", "202506"],
                     decision(from_ym="202101", to_ym="202112"))
    assert result.status == "needs_clarification"
    assert result.sql is None
    assert "202101" in result.questions[0]
    assert "202301" in result.questions[0]


def test_missing_months_are_not_silently_exported_as_complete(monkeypatch):
    result = prepare(monkeypatch, ["202301", "202303"], decision(to_ym="202303"))
    assert result.status == "needs_clarification"
    assert "202302" in result.questions[0]


def test_forecast_aggregation_keeps_the_verified_source(monkeypatch):
    request = planner.PlannerDecision(status="ready", summary="기온 평균", forecast=CONDITIONS,
                                     sql="SELECT avg(value) AS mean_c FROM forecast_data")
    result = prepare(monkeypatch, ["202301"], request)
    assert result.sql.startswith("WITH forecast_data AS (SELECT * FROM research.forecast(")
    assert result.sql.endswith("SELECT avg(value) AS mean_c FROM forecast_data")
