"""SQL 오류 → 교정 힌트 매핑.

LLM 이 run_sql 실패를 받고 스스로 재시도할 때 다음 시도를 올바르게 유도한다.
의존성 없는 순수 모듈 — 레포 루트 pytest 에서 직접 import 해 테스트한다.
"""

from __future__ import annotations

# 존재하는 뷰 전체 — relation 오류 시 통째로 알려준다 (환각 이름 재발 방지)
KNOWN_VIEWS = (
    "research.plants, research.generation, research.smp_hourly, "
    "research.smp_realtime_jeju, research.smp_weighted_avg, research.weather_asos, "
    "research.jeju_supply_demand, research.demand_5min, research.demand_weather_1h, "
    "research.heat_demand, research.heat_demand_location"
)


def hint_for(error_message: str) -> str | None:
    """PG 오류 메시지에 맞는 교정 힌트를 돌려준다. 없으면 None."""
    msg = error_message.lower()
    if "function round(double precision" in msg:
        return "힌트: PostgreSQL 에서는 round(값::numeric, 자릿수) 로 캐스트해야 한다."
    if "relation" in msg and "does not exist" in msg:
        return (
            f"힌트: 존재하는 뷰는 다음이 전부다 — {KNOWN_VIEWS}. "
            "다른 이름을 지어내지 마라."
        )
    if "column" in msg and "does not exist" in msg:
        return "힌트: SELECT * FROM <뷰> LIMIT 1 로 실제 컬럼 이름을 먼저 확인하라."
    if "statement timeout" in msg or "canceling statement due to" in msg:
        return "힌트: 조회 범위가 너무 크다. WHERE 로 기간을 좁히거나 GROUP BY 집계로 바꿔라."
    if "syntax error" in msg:
        return "힌트: 세미콜론 없이 단일 SELECT 문만 실행할 수 있다."
    if "division by zero" in msg:
        return "힌트: 분모에 NULLIF(분모, 0) 을 사용하라."
    return None
