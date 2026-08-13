-- ============================================================
-- P4. 이중 적재 (dual-write) 트리거
-- docs/schema-refactor-plan.md Part D — P4
--
-- 방식: 수집 코드를 수정하지 않고, 구 테이블 INSERT 시
--       트리거가 generation에 자동 미러링.
-- 롤백: DROP TRIGGER (데이터 무손실)
-- 마스터에 없는 발전소가 새로 나타나면 plants에 최소 행을
-- 자동 등록(불확실)하여 누락 없이 적재한다.
-- ============================================================

-- ── 공용: plants upsert + id 반환 ──
CREATE OR REPLACE FUNCTION _ensure_plant(
    p_code varchar, p_name varchar, p_unit varchar,
    p_fuel varchar, p_op varchar
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    v_id int;
BEGIN
    SELECT plant_id INTO v_id FROM plants
    WHERE plant_name = p_name AND unit_no = p_unit;
    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;
    INSERT INTO plants (plant_code, plant_name, unit_no, fuel_type, operator, capacity_confidence)
    VALUES (p_code, p_name, p_unit, p_fuel, p_op, '불확실')
    ON CONFLICT (plant_name, unit_no) DO NOTHING;
    SELECT plant_id INTO v_id FROM plants
    WHERE plant_name = p_name AND unit_no = p_unit;
    RETURN v_id;
END $$;

-- ── 남부발전 ──
CREATE OR REPLACE FUNCTION trg_nambu_to_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (NEW.datetime,
            _ensure_plant(NEW.gencd, NEW.plant_name, NEW.hogi::varchar, 'solar', 'nambu'),
            NEW.generation, 'api')
    ON CONFLICT (timestamp, plant_id) DO UPDATE
        SET gen_kwh = EXCLUDED.gen_kwh, source = 'api';
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS dualwrite_nambu ON nambu_generation;
CREATE TRIGGER dualwrite_nambu AFTER INSERT ON nambu_generation
FOR EACH ROW EXECUTE FUNCTION trg_nambu_to_generation();

-- ── 남동 태양광 ──
-- 시각은 datetime 의 '날짜' + (hour-1)시 로 복원한다(= 구간시작).
-- namdong_generation 의 datetime 포맷이 혼재(과거=자정+hour 컬럼, 라이브=구간시작)해도
-- 이 식이면 양쪽 모두 올바른 hourly timestamp 를 만든다. NEW.datetime 만 쓰면
-- 자정포맷에서 하루 24시간이 1행으로 붕괴되므로 반드시 hour 를 합성해야 한다.
CREATE OR REPLACE FUNCTION trg_namdong_to_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (date_trunc('day', NEW.datetime) + ((NEW.hour - 1) * interval '1 hour'),
            _ensure_plant(NULL, NEW.plant_name, '1', 'solar', 'namdong'),
            NEW.generation, 'api')
    ON CONFLICT (timestamp, plant_id) DO UPDATE
        SET gen_kwh = EXCLUDED.gen_kwh, source = 'api';
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS dualwrite_namdong ON namdong_generation;
CREATE TRIGGER dualwrite_namdong AFTER INSERT ON namdong_generation
FOR EACH ROW EXECUTE FUNCTION trg_namdong_to_generation();

-- ── 풍력 3사 ──
CREATE OR REPLACE FUNCTION trg_wind_namdong_to_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (NEW.timestamp,
            _ensure_plant(NULL, NEW.plant_name, '1', 'wind', 'namdong'),
            NEW.generation, 'api')
    ON CONFLICT (timestamp, plant_id) DO UPDATE
        SET gen_kwh = EXCLUDED.gen_kwh, source = 'api';
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS dualwrite_wind_namdong ON wind_namdong;
CREATE TRIGGER dualwrite_wind_namdong AFTER INSERT ON wind_namdong
FOR EACH ROW EXECUTE FUNCTION trg_wind_namdong_to_generation();

CREATE OR REPLACE FUNCTION trg_wind_seobu_to_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (NEW.timestamp,
            _ensure_plant(NULL, NEW.plant_name, '1', 'wind', 'seobu'),
            NEW.generation, 'api')
    ON CONFLICT (timestamp, plant_id) DO UPDATE
        SET gen_kwh = EXCLUDED.gen_kwh, source = 'api';
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS dualwrite_wind_seobu ON wind_seobu;
CREATE TRIGGER dualwrite_wind_seobu AFTER INSERT ON wind_seobu
FOR EACH ROW EXECUTE FUNCTION trg_wind_seobu_to_generation();

CREATE OR REPLACE FUNCTION trg_wind_hangyoung_to_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (NEW.timestamp,
            _ensure_plant(NULL, NEW.plant_name, '1', 'wind', 'hangyoung'),
            NEW.generation, 'api')
    ON CONFLICT (timestamp, plant_id) DO UPDATE
        SET gen_kwh = EXCLUDED.gen_kwh, source = 'api';
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS dualwrite_wind_hangyoung ON wind_hangyoung;
CREATE TRIGGER dualwrite_wind_hangyoung AFTER INSERT ON wind_hangyoung
FOR EACH ROW EXECUTE FUNCTION trg_wind_hangyoung_to_generation();

-- 확인
SELECT tgname, tgrelid::regclass AS table_name
FROM pg_trigger WHERE tgname LIKE 'dualwrite_%' ORDER BY 1;
