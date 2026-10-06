-- 실제 NAS 및 research_ro 권한으로 검증. 운영 데이터는 변경하지 않는다.
\set ON_ERROR_STOP on
BEGIN READ ONLY;
SET LOCAL ROLE demo_ro;
SET LOCAL statement_timeout = '10s';
DO $check$
DECLARE n int; values_n int;
BEGIN
    SELECT count(*) INTO n FROM research.forecast_months('단기예보','서울특별시','강남구','개포1동','1시간기온') WHERE month='202301';
    ASSERT n = 1, '202301 예보 파일 목록 누락';
    SELECT count(*),count(value) INTO n,values_n
    FROM research.forecast('단기예보','개포1동','1시간기온','202301','202301','서울특별시','강남구');
    ASSERT n > 30000 AND n = values_n, '실제 기온 값 파싱 실패';
    BEGIN
        PERFORM * FROM research.forecast_months('단기예보','서울특별시','종로구','필동','1시간기온');
        RAISE EXCEPTION '잘못된 시군구를 허용함';
    EXCEPTION WHEN SQLSTATE '22023' THEN NULL;
    END;
    BEGIN
        PERFORM * FROM research.forecast_months('단기예보','../../etc','강남구','개포1동','1시간기온');
        RAISE EXCEPTION '경로 이탈을 허용함';
    EXCEPTION WHEN SQLSTATE '22023' THEN NULL;
    END;
    BEGIN
        PERFORM * FROM research.forecast('단기예보','개포1동','1시간기온','202101','202112','서울특별시','강남구');
        RAISE EXCEPTION '없는 기간을 성공으로 반환함';
    EXCEPTION WHEN SQLSTATE '22023' THEN NULL;
    END;
    RAISE NOTICE 'forecast verification passed: % actual values', n;
END
$check$;
ROLLBACK;
