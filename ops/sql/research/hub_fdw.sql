-- =============================================================================
-- energy-hub-db 의 연구용 자료 6종을 pv-db research 스키마로 FDW 연결
--   송배전망: substations · power_lines · kepco_grid
--   제주:     jeju_demand_hourly · jeju_generation_mix
--   기상:     aws_obs_daily
--
-- 목적: 연구원·LLM 데모가 pv-db 한 곳만 붙어도 전부 조회 가능.
--       데이터 복제 없음(라이브 프록시).
--
-- 전제: 1) energy-hub-db 컨테이너가 pv-pipeline-network 에 연결돼 있어야 한다.
--       2) hub 쪽에 hub_research.sql 이 먼저 적용돼 있어야 한다
--          (내보낼 뷰 + pv_fdw 계정).
--
-- 적용: docker exec -i pv-data-postgres psql -U pv -d pv < sql/research/hub_fdw.sql
-- 재실행 안전: DROP SERVER CASCADE 가 매핑·외부테이블까지 정리 후 다시 만든다.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS hub_fdw CASCADE;
CREATE SERVER hub_fdw FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'energy-hub-db', port '5432', dbname 'energy_hub');

-- PUBLIC 매핑: pv·연구원 계정 모두 커버. pv_fdw 는 hub 쪽 FDW 전용 읽기 계정
-- (grid_research.sql 이 만든다). 실제 접근 제어는 pv-db 의 research_ro GRANT.
CREATE USER MAPPING FOR PUBLIC SERVER hub_fdw
    OPTIONS (user 'pv_fdw', password 'pv_fdw');

IMPORT FOREIGN SCHEMA research
    LIMIT TO (substations, power_lines, kepco_grid,
              jeju_demand_hourly, jeju_generation_mix, aws_obs_daily)
    FROM SERVER hub_fdw INTO research;

GRANT SELECT ON research.substations, research.power_lines, research.kepco_grid,
               research.jeju_demand_hourly, research.jeju_generation_mix,
               research.aws_obs_daily TO research_ro;


-- =============================================================================
-- COMMENT — energy://schema 리소스가 DB COMMENT 를 읽어 LLM 데이터 사전을
-- 만들므로 여기서 붙인다. DROP SERVER CASCADE 재실행 시마다 다시 붙어야
-- 하므로 별도 파일이 아니라 이 파일 안에 있다 (demand_fdw.sql 과 같은 이유).
-- =============================================================================

COMMENT ON FOREIGN TABLE research.substations IS
    'OSM(오픈스트리트맵) 기반 변전소 위치. 전국 17개 시도 1,185개 — 공식 전수가 아니라 누락·좌표 오차가 있을 수 있다. energy-hub-db 를 FDW 로 프록시한다.';
COMMENT ON COLUMN research.substations.voltage IS
    '전압(V) 문자열. 다중 전압은 세미콜론으로 연결된다(예: ''154000;55000;27500''). 숫자 비교가 필요하면 split 후 캐스팅하라. NULL 144개.';
COMMENT ON COLUMN research.substations.lon IS '경도(WGS84).';
COMMENT ON COLUMN research.substations.lat IS '위도(WGS84).';

COMMENT ON FOREIGN TABLE research.power_lines IS
    'OSM 기반 송전선로. 전국 4,685개. 선형 좌표는 제외하고 시도·전압·길이만 노출한다. energy-hub-db 를 FDW 로 프록시한다.';
COMMENT ON COLUMN research.power_lines.power_type IS
    'line(가공 송전선) / cable(지중 케이블) / minor_line(소규모 선로).';
COMMENT ON COLUMN research.power_lines.length_km IS
    '선로 길이(km). 좌표 기하에서 계산한 값이다.';

COMMENT ON FOREIGN TABLE research.kepco_grid IS
    '한전 배전망 접속 여유용량(분산전원 연계정보) 크롤링 스냅샷 — 2026-03-23~30 수집 정적 데이터셋, 최신 아님. 361만 행이 리·지번 주소 단위라 같은 변전소·배전선로 값이 주소 수만큼 반복된다: 설비 기준 집계는 반드시 DISTINCT subst_cd(변전소 681개)·dl_nm(배전선로 3,954개) 로 하라. 전체 스캔이 느리므로 addr_do/addr_si 로 필터해서 조회하라. energy-hub-db 를 FDW 로 프록시한다.';
COMMENT ON COLUMN research.kepco_grid.addr_do IS '시도(예: ''충청북도''). 조회 시 필수 필터.';
COMMENT ON COLUMN research.kepco_grid.addr_si IS '시(市) 이름. 시가 아닌 행은 ''-기타지역'' 채움값이 들어 있다(그 경우 실제 지역은 addr_gu). **커버리지가 고르지 않다** — 전라남도는 시 단위가 아예 없는 등 지역별 누락이 있으니, 필터 전에 DISTINCT addr_si, addr_gu 로 존재 여부를 먼저 확인하라.';
COMMENT ON COLUMN research.kepco_grid.addr_gu IS '군(郡)·구(區) 이름(광역시의 구 포함). 해당 없으면 ''-기타지역''. 시군구 필터는 addr_si 와 addr_gu 를 OR 로 함께 걸어라.';
COMMENT ON COLUMN research.kepco_grid.subst_nm IS '변전소명.';
COMMENT ON COLUMN research.kepco_grid.subst_pwr IS '변전소 접속(이용 중) 용량 추정 — 원천 정의 미확인. 단위도 미확인(kW 추정).';
COMMENT ON COLUMN research.kepco_grid.subst_capa IS '변전소 설비용량 추정 — 원천 정의·단위(kW 추정) 미확인.';
COMMENT ON COLUMN research.kepco_grid.g_subst_capa IS '변전소 접속 여유용량 추정(g_ = 여유) — 원천 정의·단위(kW 추정) 미확인.';
COMMENT ON COLUMN research.kepco_grid.mtr_no IS '주변압기 번호. pwr/capa/g_capa 컬럼 의미는 변전소와 같은 패턴(주변압기 기준).';
COMMENT ON COLUMN research.kepco_grid.dl_nm IS '배전선로(DL)명. 선로 식별은 dl_cd 가 아니라 이 컬럼으로 하라.';
COMMENT ON COLUMN research.kepco_grid.dl_cd IS '배전선로 코드 — 전체에 39종뿐이라 식별자로 쓸 수 없다. dl_nm 을 써라.';
COMMENT ON COLUMN research.kepco_grid.vol_1 IS '의미 미확인 (vol_2, vol_3 동일).';
COMMENT ON COLUMN research.kepco_grid.crawled_at IS '크롤링 시각. 데이터 기준 시점으로 간주하라.';


-- ── 제주 · 기상 자료 설명 ───────────────────────────────────────────────────

COMMENT ON FOREIGN TABLE research.jeju_demand_hourly IS
    '제주 시간별 전력수요(발전단 실적, MW). 원본 컬럼 ts 를 timestamp 로 이름만 바꿨다. 공공데이터포털 15065239(한국전력거래소_시간별 제주전력수요) 분기 파일이 원천이라 **분기마다 끊겨서 들어온다** — 원천이 분기 종료 후 열흘쯤 뒤에 올라오므로 최신 분기가 비어 보이는 것이 정상이다. 5분 단위 실시간이 필요하면 research.jeju_supply_demand 를 써라(이쪽은 10분마다 갱신된다).';
COMMENT ON COLUMN research.jeju_demand_hourly."timestamp" IS
    '구간시작 KST. 원천의 1~24시 표기를 적재 시 0~23시 구간시작으로 옮긴 값이다.';
COMMENT ON COLUMN research.jeju_demand_hourly.demand_mw IS
    '제주 전력수요(MW). 2023-09-01 이전 원천은 kWh 단위라 적재 시 환산했다.';
COMMENT ON COLUMN research.jeju_demand_hourly.source IS
    '출처 표시. datagokr = 공공데이터포털 분기 파일, sukub = KPX 5분 수급을 시간평균한 백필값.';

COMMENT ON FOREIGN TABLE research.jeju_generation_mix IS
    '제주 연료원별 시간별 전력거래량(MWh). **2024년 1년치가 전부인 정적 데이터셋이다** — 원천(공공데이터포털 15100214)이 연 1회 갱신인데 2025년치가 아직 없고, 이 데이터셋 자체가 2026-08-19 자로 다른 파일에 통합·폐기 예정이다. 최신 제주 발전 구성이 필요하면 research.jeju_supply_demand 의 solar_mw/wind_mw 를 써라.';
COMMENT ON COLUMN research.jeju_generation_mix."timestamp" IS '구간시작 KST.';
COMMENT ON COLUMN research.jeju_generation_mix.fuel_type IS
    'lng · oil · solar · wind · renewable_other · other 6종. research.plants 의 fuel_type(solar/wind/thermal/hydro/fuel_cell)과 코드 체계가 다르니 그대로 조인하지 마라.';
COMMENT ON COLUMN research.jeju_generation_mix.gen_mwh IS '해당 시간 거래량(MWh).';

COMMENT ON FOREIGN TABLE research.aws_obs_daily IS
    '방재기상관측(AWS)·종관기상(ASOS) 일자료. **일 단위이고 시간별이 아니다** — 시간별 기상은 research.weather_asos 를 써라. AWS 93지점(31.9만 행)과 ASOS 14지점(5.8만 행)이 station_type 으로 섞여 있다. 2015-01-01 ~ 2026-06-15 이며 갱신이 멈춰 있다.';
COMMENT ON COLUMN research.aws_obs_daily."date" IS '관측일(KST).';
COMMENT ON COLUMN research.aws_obs_daily.station_type IS
    '''AWS''(방재기상관측 93지점) / ''ASOS''(종관기상 14지점). 두 관측망은 장비·검증 수준이 다르므로 섞어 통계 내기 전에 나눠서 확인하라.';
COMMENT ON COLUMN research.aws_obs_daily.imputed IS
    '**결측을 채운 컬럼 이름 목록**(쉼표 구분, 예: ''ta,hm,ps''). 비어 있으면 원본 관측값이다. AWS 31.9만 행 중 18.7만 행(58%)에 보정이 들어가 있으니, 관측 진실값이 필요한 분석에서는 이 컬럼이 빈 행만 쓰거나 보정된 요소를 제외하라.';
COMMENT ON COLUMN research.aws_obs_daily.ta IS '기온(℃). 일 대표값이며 평균·최고·최저 구분은 원천에서 확인하지 못했다.';
COMMENT ON COLUMN research.aws_obs_daily.wd IS '풍향(도).';
COMMENT ON COLUMN research.aws_obs_daily.ws IS '풍속(m/s).';
COMMENT ON COLUMN research.aws_obs_daily.rn_day IS '일강수량(mm).';
COMMENT ON COLUMN research.aws_obs_daily.rn_hr1 IS '1시간 최다강수량(mm).';
COMMENT ON COLUMN research.aws_obs_daily.hm IS '습도(%).';
COMMENT ON COLUMN research.aws_obs_daily.pa IS '현지기압(hPa).';
COMMENT ON COLUMN research.aws_obs_daily.ps IS '해면기압(hPa).';
