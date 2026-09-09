-- =============================================================================
-- research.oil_hourly — 시간별 국제유가를 CSV 파일에서 그대로 읽는다
--
-- 적용: docker exec -i pv-data-postgres psql -U pv -d pv < sql/research/oil_fdw.sql
-- 전제: docker/docker-compose.yml 의 pv-db 에
--       /mnt/nvme/Energy-Data-pipeline/data/oil:/oil-data:ro 바인드 마운트
--
-- 왜 적재하지 않고 file_fdw 인가
--   수집기(fetch_data/oil/oil_hourly.py)가 매시 CSV 한 개를 갱신한다. 그 파일을
--   외부 테이블로 직접 가리키면 적재 단계가 통째로 없어지고, 수집기가 파일을
--   쓰는 순간부터 다음 조회에 반영된다. 동기화 지연도, 적재 실패도 없다.
--
--   예보 3종에는 같은 방식을 쓰지 않았다(research.forecast() 는 함수다).
--   거기는 대상이 690,154 파일이라 "외부 테이블 하나 = 파일 하나" 제약이 곧
--   테이블 69만 개를 뜻했다. 여기는 파일이 하나뿐이라 그 제약이 없다.
--
-- 원천
--   Hyperliquid XYZ builder DEX 1시간 캔들 — 브렌트(xyz:BRENTOIL), WTI(xyz:CL).
--   현물 고시가가 아니라 DEX 체결 기반이라 원유 현물 종가와는 다른 계열이다.
-- =============================================================================

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS file_fdw;

DROP SERVER IF EXISTS oil_files CASCADE;
CREATE SERVER oil_files FOREIGN DATA WRAPPER file_fdw;

-- 원본 CSV 그대로의 wide 형태. 컬럼 이름·순서는 수집기 출력과 1:1 이어야 한다.
CREATE FOREIGN TABLE research.oil_hourly_raw (
    t        bigint,
    ts_kst   text,
    brent_o  double precision,
    brent_h  double precision,
    brent_l  double precision,
    brent_c  double precision,
    brent_v  double precision,
    brent_n  bigint,
    wti_o    double precision,
    wti_h    double precision,
    wti_l    double precision,
    wti_c    double precision,
    wti_v    double precision,
    wti_n    bigint
) SERVER oil_files
OPTIONS (filename '/oil-data/oil_hourly_all.csv', format 'csv', header 'true');


-- -----------------------------------------------------------------------------
-- 연구원용 뷰 — 종목별 long 형태.
--   원본은 브렌트·WTI 가 한 행에 나란한 wide 형태다. 그대로 두면 "종목별 평균"
--   같은 질문에 컬럼을 일일이 나열해야 하고, 종목이 늘면 쿼리를 다시 짜야 한다.
--   research 스키마의 다른 뷰(generation 의 fuel_type)와 같은 모양으로 맞춘다.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW research.oil_hourly AS
SELECT ts::timestamp AS "timestamp", symbol, open, high, low, close, volume, trades
FROM (
    SELECT ts_kst AS ts, 'brent' AS symbol,
           brent_o AS open, brent_h AS high, brent_l AS low, brent_c AS close,
           brent_v AS volume, brent_n AS trades
    FROM research.oil_hourly_raw
    UNION ALL
    SELECT ts_kst, 'wti',
           wti_o, wti_h, wti_l, wti_c, wti_v, wti_n
    FROM research.oil_hourly_raw
) s
WHERE open IS NOT NULL;   -- 한쪽 종목만 캔들이 온 시각은 그 종목 행만 남긴다

COMMENT ON VIEW research.oil_hourly IS
    '시간별 국제유가 OHLCV(브렌트·WTI). Hyperliquid XYZ builder DEX 의 1시간 캔들이며 **현물 고시가가 아니다** — 원유 현물 종가와 비교할 때는 계열이 다르다는 점을 감안하라. CSV 파일을 file_fdw 로 직접 읽으므로 적재본이 없고 수집 즉시 반영된다.';
COMMENT ON COLUMN research.oil_hourly."timestamp" IS
    '구간시작 KST. 값 09:00 은 [09:00, 10:00) 구간의 캔들이다.';
COMMENT ON COLUMN research.oil_hourly.symbol IS
    '''brent''(xyz:BRENTOIL) 또는 ''wti''(xyz:CL).';
COMMENT ON COLUMN research.oil_hourly.close IS
    '종가(USD). 일별 종가만 필요하면 시각별 마지막 값을 쓰지 말고 원천 성격을 먼저 확인하라 — DEX 는 24시간 거래된다.';
COMMENT ON COLUMN research.oil_hourly.volume IS
    'DEX 거래량. 현물 시장 거래량이 아니다.';
COMMENT ON COLUMN research.oil_hourly.trades IS
    '해당 구간 체결 건수.';

GRANT SELECT ON research.oil_hourly_raw, research.oil_hourly TO research_ro;


-- =============================================================================
-- 검증 — 한 번 돌리고 버리는 쿼리
-- =============================================================================
--
-- (1) 파일이 읽히는가
--     SELECT count(*), min(ts_kst), max(ts_kst) FROM research.oil_hourly_raw;
--
-- (2) long 뷰가 종목별로 갈라지는가
--     SELECT symbol, count(*), min("timestamp"), max("timestamp")
--     FROM research.oil_hourly GROUP BY 1;
--
-- (3) 연구원 role 이 읽는가 (검증용 role 을 만들고 반드시 DROP 한다)
--     PGPASSWORD=probe psql -h localhost -p 5436 -U research_probe -d pv \
--       -c "SELECT count(*) FROM research.oil_hourly;"
-- =============================================================================
