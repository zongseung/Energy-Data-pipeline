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
              heat_demand_location, demand_weather_1h, gen_mix_5min)
    FROM SERVER demand_fdw INTO research;

-- 뷰 재생성 스크립트(pv_research.sql)의 GRANT 는 뷰에만 걸리므로 여기서도 한 번.
GRANT SELECT ON research.demand_5min, research.jeju_supply_demand,
               research.heat_demand, research.heat_demand_location,
               research.demand_weather_1h, research.gen_mix_5min TO research_ro;


-- =============================================================================
-- COMMENT — 여기서 다시 붙여야 하는 이유
--
-- IMPORT FOREIGN SCHEMA 는 컬럼 구조만 가져오고 **COMMENT 는 안 가져온다.**
-- demand_research.sql 이 demand DB 에 붙여 둔 설명은 pv-db 로 따라오지 않아,
-- 이쪽 research 스키마에서는 수요 5개 테이블만 설명이 통째로 비어 있었다.
--
-- 이게 왜 치명적이냐 — mcp-server 의 energy://schema 리소스는 정적 문서가 아니라
-- **DB COMMENT 를 그대로 읽어서** LLM 에게 줄 데이터 사전을 만든다
-- (mcp-server/energy_mcp/server.py:_SCHEMA_QUERY). 설명이 비면 LLM 은 컬럼
-- 이름만 보고 의미를 추측하는데, 하필 demand_5min 은 이름이 의미와 어긋난다.
--
-- 위 DROP SERVER CASCADE 가 외부테이블을 매번 새로 만들므로 COMMENT 도 매번
-- 다시 붙어야 한다. 그래서 별도 파일이 아니라 이 파일 안에 있다.
-- =============================================================================

COMMENT ON FOREIGN TABLE research.demand_5min IS
    'KPX 전국 계통 수급 5분 데이터(MW, 예비율만 %). demand-postgres 의 같은 이름 뷰를 FDW 로 프록시한다 — pv-db 에 복제본은 없다.';
COMMENT ON COLUMN research.demand_5min."timestamp" IS
    'KPX 원천의 기준일시를 그대로 쓴다(KST). 구간 라벨인지 순시 시각인지 확정 근거가 없어 보정하지 않았다.';
COMMENT ON COLUMN research.demand_5min.current_demand IS
    '현재수요(MW). 5분마다 실제로 변한다.';
-- 아래 두 컬럼은 이름이 의미와 어긋난다. 2026-08-12 실측으로 확인한 내용:
--   current_supply  하루 288행 중 278개가 서로 다른 값(실시간 변동), 항상 수요보다 큼,
--                   supply_reserve = current_supply - current_demand 로 일치 → 공급능력
--   supply_capacity 하루 종일 86,900 고정(고유값 2개) → 그날의 최대예측수요
COMMENT ON COLUMN research.demand_5min.current_supply IS
    '**공급능력**(MW). 이름은 "현재 공급량"처럼 보이지만 아니다 — 예비력(supply_reserve = 이 값 - current_demand)이 이 컬럼을 기준으로 계산된다. 공급능력이 필요하면 supply_capacity 가 아니라 이 컬럼을 써라.';
COMMENT ON COLUMN research.demand_5min.supply_capacity IS
    '**그날의 최대예측수요**(MW). 이름은 "공급능력"처럼 보이지만 아니다 — 하루 종일 같은 값으로 고정된다(예: 86,900 이 288행 내내 반복). 공급능력은 current_supply 다.';
COMMENT ON COLUMN research.demand_5min.supply_reserve IS
    '공급예비력(MW). current_supply - current_demand 와 일치한다.';
COMMENT ON COLUMN research.demand_5min.reserve_rate IS
    '공급예비율(%). supply_reserve / current_demand × 100 과 일치한다.';
COMMENT ON COLUMN research.demand_5min.operation_reserve IS
    '운영예비력(MW). 원천 정의 문서를 확인하지 못했다.';
COMMENT ON COLUMN research.demand_5min.day_type IS
    '요일/휴일 구분 코드. 원천 정의 문서를 확인하지 못했다.';

COMMENT ON FOREIGN TABLE research.jeju_supply_demand IS
    '제주 계통 수급 5분 데이터(MW). 원본 컬럼 ts 를 timestamp 로 이름만 바꿨다. research 스키마에서 가장 최신인 계열이다(10분 주기 적재).';
COMMENT ON COLUMN research.jeju_supply_demand."timestamp" IS
    '제주 실시간 원천의 시각을 그대로 쓴다(KST). 무보정.';
COMMENT ON COLUMN research.jeju_supply_demand.renewable_total_mw IS
    '재생에너지 합계(MW). solar_mw + wind_mw 외 소규모 계열이 포함될 수 있다.';

COMMENT ON FOREIGN TABLE research.heat_demand IS
    '지사별 시간별 열수요 + 동반 기상. branch 는 research.heat_demand_location.name 과 조인한다. **완결된 정적 데이터셋이다** — 2021-01-01~2023-12-31 3년치가 전량이며, 19개 지사 모두 26,279행씩 같은 구간을 갖는다. 수집이 끊겨 잘린 것이 아니므로 최신 데이터를 기다리지 마라.';
COMMENT ON COLUMN research.heat_demand."timestamp" IS
    'KST. 이 저장소에 적재 코드가 없어 원천 라벨 규약(구간시작/구간종료)을 확인하지 못했다 — 보정하지 않았다.';
COMMENT ON COLUMN research.heat_demand.heat_demand IS
    '열수요(원천 단위 그대로, 단위 검증 안 됨).';

COMMENT ON FOREIGN TABLE research.heat_demand_location IS
    '열수요 지사 위치 19곳. name 이 research.heat_demand.branch 와 대응한다.';

COMMENT ON FOREIGN TABLE research.demand_weather_1h IS
    'demand_5min 시간평균 × ASOS 관측을 시각으로 조인한 파생 테이블. **지점 수(95~96개)만큼 같은 수요값이 반복되므로 수요만 필요하면 research.demand_5min 을 써라** — 안 그러면 전국 수요를 96배로 합산하게 된다. station_name = ''UNKNOWN'' 행은 제외할 것.';
COMMENT ON COLUMN research.demand_weather_1h."timestamp" IS
    '구간시작 KST. demand_avg 는 demand_5min 의 5분 라벨 기준 [H, H+1) 평균이라 버킷 경계는 확정적이지만, 5분 라벨 자체의 의미가 미확정이라 최대 5분 오차가 남을 수 있다. 기상 컬럼은 ASOS 라벨 규약을 따른다.';
COMMENT ON COLUMN research.demand_weather_1h.demand_avg IS
    '해당 1시간 구간 전국 수요(MW)의 5분값 평균.';


COMMENT ON FOREIGN TABLE research.gen_mix_5min IS
    'KPX 실시간 발전원별 발전량 5분(계통기준 전국, MW). demand_5min 이 수요·예비력만 담아 발전원 구분이 없던 자리를 채운다. **태양광이 세 갈래다** — solar_market(전력시장 계량), solar_ppa·solar_btm(KPX 추정치). 원천이 당일치만 제공해 5분마다 수집하므로 수집 시작(2026-08-23) 이전 구간은 없다.';
COMMENT ON COLUMN research.gen_mix_5min."timestamp" IS 'KST naive. KPX 페이지의 5분 라벨 그대로다(demand_5min 과 동일 규약).';
COMMENT ON COLUMN research.gen_mix_5min.solar_market IS '전력시장 참여 태양광(MW). 계량 기반이라 셋 중 가장 신뢰도가 높다.';
COMMENT ON COLUMN research.gen_mix_5min.solar_ppa IS '기업PPA 태양광(MW). **KPX 추정치**다.';
COMMENT ON COLUMN research.gen_mix_5min.solar_btm IS '자가소비(Behind-The-Meter) 태양광(MW). **KPX 추정치**다. 계통에 안 잡히는 자가발전을 추정한 값이라 태양광 전체 규모를 볼 때만 쓰고 정산·검증에는 쓰지 마라.';
COMMENT ON COLUMN research.gen_mix_5min.coal_total IS '석탄 합계(MW) = 유연탄 + coal_domestic(국내탄).';
COMMENT ON COLUMN research.gen_mix_5min.pumped IS '양수(MW). 양수 발전 출력이며 펌핑(충전)은 여기 안 잡힌다.';
COMMENT ON COLUMN research.gen_mix_5min.ess IS 'ESS(MW). **충전 중이면 음수**다.';
COMMENT ON COLUMN research.gen_mix_5min.renewable_total IS '신재생 합계(MW) = renewable_new(신에너지) + renewable_renew(재생에너지). 태양광·풍력이 여기 포함되므로 발전원 합계를 낼 때 이중 계산에 주의하라.';
