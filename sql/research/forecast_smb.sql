-- =============================================================================
-- research.forecast() — 기상청 동네예보 3종을 NAS SMB 에서 "적재 없이" 읽는다
--
-- 적용: docker exec -i pv-data-postgres psql -U pv -d pv < sql/research/forecast_smb.sql
-- 전제: docker/docker-compose.yml 의 pv-db 에
--       /mnt/nvme/weather-data/nas-weather:/nas-weather:ro 바인드 마운트가 있어야 한다.
--
-- 왜 적재하지 않나
--   단기예보만 690,154 파일 × 32,576 행 ≈ 225억 행이다. pv-db 가 올라간 LUN 은
--   98G 중 31G 만 남았고, 전 읍면동 예보는 이 DB 의 목적(발전량 연구)과도 맞지 않는다.
--   반면 SMB 는 콜드 경로에서도 읍면동 1곳 × 요소 1개 × 30개월을 0.53초에 읽는다.
--   즉 "좁은 조회"는 즉시 되고 "전체 스캔"만 불가능하다 — 함수가 정확히 그 형태다.
--
-- 왜 file_fdw 가 아닌가
--   외부 테이블은 filename 이나 program 이 **고정**이라 질의마다 범위를 못 좁힌다.
--   파일당 테이블을 만들면 69만 개, 하나로 묶으면 매 질의가 3.4시간짜리 전체 스캔이다.
--
-- 원천 포맷
--   경로 : {예보종}/{시도}/{시군구}/{읍면동}/{요소}/{읍면동}_{요소}_{시작}_{종료}.csv
--   헤더 :  format: day,hour,forecast,value  location:61_125 Start : 20230101
--   행   :  1,0200,+6,1.000000      (예보 2종 — day, 발표시각, 리드타임, 값)
--            1, 0000, 0.000000       (초단기실황 — day, 시각, 값. 리드타임 없음)
--
-- =============================================================================
-- 보안 — 이 함수는 SECURITY DEFINER 로 superuser 권한을 빌려 서버 파일을 읽는다.
-- 검증이 없으면 그대로 임의 파일 유출 통로가 된다. 방어는 두 겹이다.
--
--   1) 경로를 사용자 입력으로 조립하지 않는다.
--      예보종만 3값 화이트리스트로 검사하고, 그 아래 모든 경로 조각(시도·시군구·
--      읍면동·요소·파일명)은 pg_ls_dir() 이 돌려준 **실존 항목**에서만 가져온다.
--      사용자 입력은 그 목록과 `=` 로 비교하는 데만 쓴다. '../../etc/passwd' 같은
--      문자열은 실존 디렉터리 항목과 같아질 수 없으므로 애초에 매치되지 않는다.
--      정규식 블랙리스트(.. 금지 등)와 달리 우회 인코딩을 고민할 필요가 없다.
--
--   2) search_path 를 고정한다. SECURITY DEFINER 함수에서 search_path 를 열어 두면
--      호출자가 같은 이름의 함수·연산자를 자기 스키마에 심어 definer 권한으로
--      실행시킬 수 있다.
--
-- 추가로 읽는 파일 수에 상한을 둔다 — 범위를 넓게 줘서 트리 전체를 읽게 만드는
-- 자원 고갈을 막는다.
-- =============================================================================

\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION research.forecast(
    forecast_type text,
    dong          text,
    element       text,
    from_ym       text DEFAULT NULL,   -- 'YYYYMM', NULL 이면 처음부터
    to_ym         text DEFAULT NULL    -- 'YYYYMM', NULL 이면 끝까지
)
RETURNS TABLE (
    sido       text,
    sigungu    text,
    dong_name  text,
    element_name text,
    grid       text,          -- 기상청 격자 nx_ny
    base_at    timestamp,     -- 발표 시각 (KST)
    lead_hours int,           -- 예보 리드타임(시간). 초단기실황은 NULL
    target_at  timestamp,     -- 예보 대상 시각 = base_at + lead_hours
    value      double precision
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $fn$
DECLARE
    root      constant text := '/nas-weather';
    max_files constant int  := 400;   -- 읍면동 1곳 × 요소 1개면 실제로는 ~30개
    v_sido    text;
    v_sigungu text;
    v_file    text;
    v_dir     text;
    v_read    int := 0;
    v_found   boolean := false;
    v_body    text;
    v_grid    text;
    v_ymd     text;
    v_ym      text;
BEGIN
    -- (1) 예보종 — 경로에 직접 들어가는 유일한 사용자 입력이라 값 자체를 고정한다.
    IF forecast_type IS NULL
       OR forecast_type NOT IN ('단기예보', '초단기예보', '초단기실황') THEN
        RAISE EXCEPTION
            '예보종은 단기예보 / 초단기예보 / 초단기실황 중 하나여야 한다 (받은 값: %)',
            forecast_type;
    END IF;

    IF dong IS NULL OR element IS NULL THEN
        RAISE EXCEPTION '읍면동(dong)과 요소(element)는 반드시 지정해야 한다. '
                        '요소 예: 1시간기온, 습도, 풍속, 하늘상태, 강수확률';
    END IF;

    IF from_ym IS NOT NULL AND from_ym !~ '^\d{6}$' THEN
        RAISE EXCEPTION 'from_ym 은 YYYYMM 형식이어야 한다 (받은 값: %)', from_ym;
    END IF;
    IF to_ym IS NOT NULL AND to_ym !~ '^\d{6}$' THEN
        RAISE EXCEPTION 'to_ym 은 YYYYMM 형식이어야 한다 (받은 값: %)', to_ym;
    END IF;

    -- (2) 시도 → 시군구를 훑어 읍면동을 찾는다. 여기서 나오는 이름은 전부
    --     파일시스템 실측값이다. 17개 시도 전수 탐색이 0.19초라 인덱스가 필요 없다.
    FOR v_sido IN
        SELECT f FROM pg_catalog.pg_ls_dir(root || '/' || forecast_type) AS f
        WHERE f NOT LIKE '.%'          -- .DS_Store 등 (디렉터리가 아니라 pg_ls_dir 이 실패한다)
        ORDER BY f
    LOOP
        FOR v_sigungu IN
            SELECT f FROM pg_catalog.pg_ls_dir(
                       root || '/' || forecast_type || '/' || v_sido) AS f
            WHERE f NOT LIKE '.%'
            ORDER BY f
        LOOP
            -- 사용자가 준 dong 을 실존 항목 목록과 동등비교만 한다(경로 조립 아님).
            CONTINUE WHEN NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_ls_dir(
                           root || '/' || forecast_type || '/' || v_sido
                           || '/' || v_sigungu) AS f
                WHERE f = dong);

            v_dir := root || '/' || forecast_type || '/' || v_sido || '/'
                     || v_sigungu || '/' || dong;

            CONTINUE WHEN NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_ls_dir(v_dir) AS f WHERE f = element);

            v_found := true;
            v_dir := v_dir || '/' || element;

            -- (3) 월 파일을 범위로 좁혀 읽는다.
            FOR v_file IN
                SELECT f FROM pg_catalog.pg_ls_dir(v_dir) AS f
                WHERE f LIKE '%.csv'
                ORDER BY f
            LOOP
                -- 파일명 첫 날짜에서 YYYYMM 추출
                --   개포1동_1시간기온_20230101_20230201.csv → 202301
                --   청운효자동_강수형태_202401_202401.csv     → 202401
                v_ym := pg_catalog.substring(v_file, '_(\d{6})');
                CONTINUE WHEN v_ym IS NULL;
                CONTINUE WHEN from_ym IS NOT NULL AND v_ym < from_ym;
                CONTINUE WHEN to_ym   IS NOT NULL AND v_ym > to_ym;

                v_read := v_read + 1;
                IF v_read > max_files THEN
                    RAISE EXCEPTION
                        '읽을 파일이 %개를 넘었다. from_ym/to_ym 으로 기간을 좁혀라.',
                        max_files;
                END IF;

                v_body := pg_catalog.pg_read_file(v_dir || '/' || v_file);

                -- 헤더에서 격자와 시작일을 뽑는다.
                --  format: day,hour,forecast,value  location:61_125 Start : 20230101
                v_grid := pg_catalog.substring(v_body, 'location:\s*(\S+)');
                v_ymd  := pg_catalog.substring(v_body, 'Start\s*:\s*(\d{8})');
                CONTINUE WHEN v_ymd IS NULL;

                -- 원천은 CRLF 이고 값 앞뒤에 공백이 붙는다(' 1,0200,+6,1.000000 \r').
                -- btrim 기본 문자셋은 공백뿐이라 \r 이 남고, 그러면 숫자 정규식이
                -- 전부 어긋나 value 가 통째로 NULL 이 된다. 트림셋을 명시한다.
                RETURN QUERY
                WITH raw AS (
                    SELECT pg_catalog.string_to_array(
                               pg_catalog.btrim(line, E' \t\r\n'), ',') AS c
                    FROM pg_catalog.regexp_split_to_table(v_body, E'\n') AS line
                    -- 헤더( format: ... )와 빈 줄을 버린다. 데이터 행은 숫자로 시작한다.
                    WHERE pg_catalog.btrim(line, E' \t\r\n') ~ '^\d'
                ), parsed AS (
                    SELECT
                        pg_catalog.btrim(c[1], E' \t\r\n')::int AS d,
                        pg_catalog.btrim(c[2], E' \t\r\n')      AS hh,
                        CASE WHEN pg_catalog.array_length(c, 1) >= 4
                             THEN pg_catalog.btrim(c[3], E' \t\r\n') END AS lead_raw,
                        pg_catalog.btrim(
                            c[pg_catalog.array_length(c, 1)], E' \t\r\n') AS val_raw
                    FROM raw
                    WHERE pg_catalog.array_length(c, 1) IN (3, 4)
                )
                SELECT
                    v_sido, v_sigungu, dong, element, v_grid,
                    b.base,
                    b.lead,
                    b.base + pg_catalog.make_interval(hours => COALESCE(b.lead, 0)),
                    b.val
                FROM (
                    SELECT
                        pg_catalog.make_timestamp(
                            pg_catalog.substr(v_ymd, 1, 4)::int,
                            pg_catalog.substr(v_ymd, 5, 2)::int,
                            p.d,
                            pg_catalog.substr(pg_catalog.lpad(p.hh, 4, '0'), 1, 2)::int,
                            0, 0) AS base,
                        -- '+6' → 6. 형식이 어긋나면 조용히 NULL 로 둔다.
                        CASE WHEN p.lead_raw ~ '^[+-]?\d+$'
                             THEN p.lead_raw::int END AS lead,
                        CASE WHEN p.val_raw ~ '^[+-]?\d+(\.\d+)?$'
                             THEN p.val_raw::double precision END AS val
                    FROM parsed p
                    -- 그 달에 없는 날짜(31일 없는 달 등)는 make_timestamp 가 실패하므로 미리 거른다
                    WHERE p.d BETWEEN 1 AND pg_catalog.date_part('day',
                              (pg_catalog.make_date(
                                   pg_catalog.substr(v_ymd, 1, 4)::int,
                                   pg_catalog.substr(v_ymd, 5, 2)::int, 1)
                               + INTERVAL '1 month - 1 day'))::int
                      AND pg_catalog.lpad(p.hh, 4, '0') ~ '^\d{4}$'
                ) b;
            END LOOP;
        END LOOP;
    END LOOP;

    IF NOT v_found THEN
        RAISE EXCEPTION
            '% / % 를 찾지 못했다. 읍면동과 요소 이름이 정확한지 확인하라. '
            '요소 목록은 research.forecast_elements(예보종) 으로 볼 수 있다.',
            dong, element;
    END IF;
END
$fn$;

COMMENT ON FUNCTION research.forecast(text, text, text, text, text) IS
    '기상청 동네예보를 NAS 에서 직접 읽는다(적재본 없음). '
    '예: SELECT * FROM research.forecast(''단기예보'',''개포1동'',''1시간기온'',''202301'',''202303''). '
    '인자: 예보종(단기예보|초단기예보|초단기실황), 읍면동, 요소, 시작YYYYMM, 종료YYYYMM. '
    'base_at 은 발표 시각, target_at 은 예보 대상 시각이다(초단기실황은 둘이 같고 lead_hours 가 NULL). '
    '기간을 안 주면 전 기간(약 30개월)을 읽으니 되도록 좁혀서 부를 것.';


-- -----------------------------------------------------------------------------
-- 요소·읍면동 목록 — 이름을 몰라서 forecast() 를 못 부르는 상황을 막는다.
-- 같은 이유로 SECURITY DEFINER 지만, 반환값이 디렉터리 이름뿐이라 노출면이 작다.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION research.forecast_elements(forecast_type text)
RETURNS TABLE (element_name text)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $fn$
DECLARE
    root constant text := '/nas-weather';
    v_sido text; v_sigungu text; v_dong text;
BEGIN
    IF forecast_type IS NULL
       OR forecast_type NOT IN ('단기예보', '초단기예보', '초단기실황') THEN
        RAISE EXCEPTION '예보종은 단기예보 / 초단기예보 / 초단기실황 중 하나여야 한다.';
    END IF;

    -- 요소 구성은 예보종 안에서 동일하므로 첫 번째 읍면동 하나만 들여다본다.
    SELECT f INTO v_sido FROM pg_catalog.pg_ls_dir(root||'/'||forecast_type) AS f
     WHERE f NOT LIKE '.%' ORDER BY f LIMIT 1;
    SELECT f INTO v_sigungu FROM pg_catalog.pg_ls_dir(root||'/'||forecast_type||'/'||v_sido) AS f
     WHERE f NOT LIKE '.%' ORDER BY f LIMIT 1;
    SELECT f INTO v_dong FROM pg_catalog.pg_ls_dir(root||'/'||forecast_type||'/'||v_sido||'/'||v_sigungu) AS f
     WHERE f NOT LIKE '.%' ORDER BY f LIMIT 1;

    RETURN QUERY
    SELECT f::text FROM pg_catalog.pg_ls_dir(
        root||'/'||forecast_type||'/'||v_sido||'/'||v_sigungu||'/'||v_dong) AS f
    WHERE f NOT LIKE '.%' ORDER BY f;
END
$fn$;

COMMENT ON FUNCTION research.forecast_elements(text) IS
    '해당 예보종이 제공하는 요소 목록. 예: SELECT * FROM research.forecast_elements(''단기예보'').';


CREATE OR REPLACE FUNCTION research.forecast_regions(
    forecast_type text,
    sido          text DEFAULT NULL,
    sigungu       text DEFAULT NULL
)
RETURNS TABLE (sido_name text, sigungu_name text, dong_name text)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $fn$
DECLARE
    root constant text := '/nas-weather';
    v_sido text; v_sigungu text;
BEGIN
    IF forecast_type IS NULL
       OR forecast_type NOT IN ('단기예보', '초단기예보', '초단기실황') THEN
        RAISE EXCEPTION '예보종은 단기예보 / 초단기예보 / 초단기실황 중 하나여야 한다.';
    END IF;

    FOR v_sido IN
        SELECT f FROM pg_catalog.pg_ls_dir(root||'/'||forecast_type) AS f
        WHERE f NOT LIKE '.%' AND (sido IS NULL OR f = sido) ORDER BY f
    LOOP
        -- 시도만 물었으면 시군구까지만 돌려주고 읍면동 전수 나열은 안 한다.
        IF sido IS NULL THEN
            RETURN QUERY
            SELECT v_sido, f::text, NULL::text
            FROM pg_catalog.pg_ls_dir(root||'/'||forecast_type||'/'||v_sido) AS f
            WHERE f NOT LIKE '.%' ORDER BY f;
            CONTINUE;
        END IF;

        FOR v_sigungu IN
            SELECT f FROM pg_catalog.pg_ls_dir(root||'/'||forecast_type||'/'||v_sido) AS f
            WHERE f NOT LIKE '.%' AND (sigungu IS NULL OR f = sigungu) ORDER BY f
        LOOP
            RETURN QUERY
            SELECT v_sido, v_sigungu, f::text
            FROM pg_catalog.pg_ls_dir(
                root||'/'||forecast_type||'/'||v_sido||'/'||v_sigungu) AS f
            WHERE f NOT LIKE '.%' ORDER BY f;
        END LOOP;
    END LOOP;
END
$fn$;

COMMENT ON FUNCTION research.forecast_regions(text, text, text) IS
    '예보 지역 트리 탐색. 인자 없이 부르면 시도·시군구 목록, 시도를 주면 그 안의 읍면동까지 나온다. '
    '예: SELECT * FROM research.forecast_regions(''단기예보'', ''서울특별시'', ''강남구'').';


-- -----------------------------------------------------------------------------
-- 권한 — PUBLIC 에서 회수한 뒤 research_ro 에만 준다.
--   SECURITY DEFINER 함수는 생성 시 PUBLIC 에 EXECUTE 가 자동으로 붙는다.
--   회수하지 않으면 이 DB 에 접속 가능한 누구나 서버 파일 읽기 함수를 부를 수 있다.
-- -----------------------------------------------------------------------------
REVOKE ALL ON FUNCTION research.forecast(text, text, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION research.forecast_elements(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION research.forecast_regions(text, text, text) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION research.forecast(text, text, text, text, text) TO research_ro;
GRANT EXECUTE ON FUNCTION research.forecast_elements(text) TO research_ro;
GRANT EXECUTE ON FUNCTION research.forecast_regions(text, text, text) TO research_ro;


-- =============================================================================
-- 검증 — 한 번 돌리고 버리는 쿼리
-- =============================================================================
--
-- (1) 요소 목록
--     SELECT * FROM research.forecast_elements('단기예보');
--
-- (2) 실제 조회
--     SELECT * FROM research.forecast('단기예보','개포1동','1시간기온','202301','202301') LIMIT 5;
--
-- (3) 경로 이탈 시도가 막히는가 — 전부 예외가 나야 정상
--     SELECT * FROM research.forecast('../../etc','개포1동','1시간기온');
--     SELECT * FROM research.forecast('단기예보','../../../etc','passwd');
--     SELECT * FROM research.forecast('단기예보','개포1동','../../../../etc/passwd');
--
-- (4) PUBLIC 이 못 부르는가
--     SELECT has_function_privilege('public','research.forecast(text,text,text,text,text)','execute');
--     -- 기대: f
-- =============================================================================
