-- =============================================================================
-- demand-postgres 의 research 뷰 5개를 pv-db research 스키마로 FDW 연결
--
-- 목적: 연구원·LLM 데모가 pv-db 한 곳만 붙어도 수요·수급 데이터까지 조회 가능.
--       데이터 복제 없음(라이브 프록시) — "pv DB에 jeju 중복 금지" 규칙 준수.
--
-- 전제: demand-postgres 컨테이너가 pv-pipeline-network 에 연결돼 있어야 한다
--       (weather-pipeline/docker/docker-compose.yml 의 demand-db networks 참조).
--
-- 적용: docker exec -i pv-data-postgres psql -U pv -d pv < sql/research/demand_fdw.sql
-- 재실행 안전: DROP SERVER CASCADE 가 매핑·외부테이블까지 정리 후 다시 만든다.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS demand_fdw CASCADE;
CREATE SERVER demand_fdw FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'demand-postgres', port '5432', dbname 'demand');

-- PUBLIC 매핑: pv·연구원 계정 모두 커버. demand 쪽 자격증명은 개발용(demand/demand).
-- 실제 접근 제어는 pv-db 쪽 research_ro GRANT 가 담당한다.
CREATE USER MAPPING FOR PUBLIC SERVER demand_fdw
    OPTIONS (user 'demand', password 'demand');

IMPORT FOREIGN SCHEMA research
    LIMIT TO (demand_5min, jeju_supply_demand, heat_demand,
              heat_demand_location, demand_weather_1h)
    FROM SERVER demand_fdw INTO research;

-- 뷰 재생성 스크립트(pv_research.sql)의 GRANT 는 뷰에만 걸리므로 여기서도 한 번.
GRANT SELECT ON research.demand_5min, research.jeju_supply_demand,
               research.heat_demand, research.heat_demand_location,
               research.demand_weather_1h TO research_ro;
