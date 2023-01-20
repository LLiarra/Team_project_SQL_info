--------------------------------------------------------------------------------
-- Создание таблиц БД part4
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS test_table1 (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS test_table2 (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS no_test_table (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS table_not_for_test (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS aboba_only (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS aboba_only2 (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL
);

--------------------------------------------------------------------------------
-- Создание функций БД part4
--------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS fnc_test_table1;

CREATE OR REPLACE FUNCTION fnc_test_table1() RETURNS SETOF test_table1 AS
    $$ SELECT * FROM test_table1; $$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_no_test_table_all() RETURNS SETOF no_test_table AS
    $$ SELECT * FROM no_test_table; $$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_no_test_table(ptitle VARCHAR DEFAULT 'female') RETURNS SETOF no_test_table AS
$$ SELECT
        *
    FROM
        no_test_table
    WHERE
        title = ptitle; $$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_no_test_table(ptitle TEXT, pid INTEGER) RETURNS SETOF no_test_table AS
$$ SELECT
        *
    FROM
        no_test_table
    WHERE
        title = ptitle
        ANd id = pid; $$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_aboba(aboba TEXT DEFAULT 'aboba') RETURNS VARCHAR AS
$$ SELECT aboba; $$
LANGUAGE SQL;

--------------------------------------------------------------------------------
-- Создание триггеров БД part4
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fnc_aboba_only_handle() RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            NEW.title = 'aboba';
            RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
            NEW.title = 'aboba';
            RETURN NEW;
        ELSIF (TG_OP = 'DELETE') THEN
            RETURN OLD;
        END IF;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_aboba_only_handle2() RETURNS TRIGGER AS $$
    BEGIN
        NEW.title = NEW.title || 'aboba';
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_test_table2() RETURNS TRIGGER AS $$
    BEGIN
        RAISE NOTICE 'TEST1!!!!!!!';
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_aboba_only
BEFORE INSERT OR UPDATE OR DELETE ON aboba_only
    FOR EACH ROW EXECUTE FUNCTION fnc_aboba_only_handle();

CREATE OR REPLACE TRIGGER trg_aboba_only
BEFORE INSERT OR UPDATE ON aboba_only2
    FOR EACH ROW EXECUTE FUNCTION fnc_aboba_only_handle();

CREATE OR REPLACE TRIGGER trg_aboba_only2
BEFORE INSERT ON aboba_only2
    FOR EACH ROW EXECUTE FUNCTION fnc_aboba_only_handle2();

CREATE OR REPLACE TRIGGER trg_test_table1
BEFORE INSERT ON test_table1
    FOR EACH ROW EXECUTE FUNCTION fnc_test_table2();

--------------------------------------------------------------------------------
-- Задания
--------------------------------------------------------------------------------
-- Task1
--------------------------------------------------------------------------------
-- Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает
-- все те таблицы текущей базы данных, имена которых начинаются с фразы
-- 'TableName'.
--
-- Мы не хотим удалять служебные таблицы.
-- Поэтому параметр схемы укажем в процедуре с дефолтным значением "public."
-- Альтернативно можно было делать выборку:
-- table_schema NOT LIKE 'pg_%' AND table_schema NOT LIKE 'information_schema' 
CREATE
OR REPLACE PROCEDURE pr_part4_task1(
    IN table_name_pattern TEXT,
    IN table_schema_pattern TEXT DEFAULT 'public'
) AS $$
DECLARE
    rec record;
BEGIN
    FOR rec IN
    SELECT
        table_schema,
        table_name
    FROM
        information_schema.tables
    WHERE
        table_schema LIKE table_schema_pattern || '%'
        AND table_type = 'BASE TABLE'
        AND table_name LIKE table_name_pattern || '%'
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(rec.table_schema) || '.' || quote_ident(rec.table_name) || ' CASCADE';
        RAISE NOTICE 'Table deleted: %', quote_ident(rec.table_schema) || '.' || quote_ident(rec.table_name);
    END LOOP;

END;
$$ LANGUAGE plpgsql;

-- Проверяем работу процедуры
BEGIN;
CALL pr_part4_task1('test');
END;

--------------------------------------------------------------------------------
-- Task 2
--------------------------------------------------------------------------------
-- Создать хранимую процедуру с выходным параметром, которая выводит список имен
-- и параметров всех скалярных SQL функций пользователя в текущей базе данных.
-- Имена функций без параметров не выводить. Имена и список параметров должны
-- выводиться в одну строку. Выходной параметр возвращает количество найденных
-- функций.
--
-- При реализации можно использовать курсоры, как мы делали в Part3, но я всё
-- ещё сомневаюсь, что это будет удобно в будущем, поэтому для разнообразия 
-- вместо курсоров используем временную таблицу
--
-- При реализации через курсор количество строк считаем черех MOVE:
-- MOVE FORWARD ALL FROM result_cur;
-- GET DIAGNOSTICS rows_count := ROW_COUNT;
-- MOVE BACKWARD ALL FROM result_cur;
--
CREATE
OR REPLACE PROCEDURE pr_part4_task2(
    INOUT rows_count INTEGER,
    IN table_schema_pattern TEXT DEFAULT 'public'
) AS $$
BEGIN
    DROP TABLE IF EXISTS tmp_part4_task2;
    CREATE TEMP TABLE tmp_part4_task2 AS
        SELECT
            MAX(routines.routine_name) AS function_name,
            -- Т.к. при агрегации порядок соединения не гарантируется, а мы бы 
            -- хотели тот же порядок, что при объявлении функции, то используем
            -- агрегацию с сортировкой
            string_agg(
                parameters.parameter_name,
                ', '
                ORDER BY
                    parameters.ordinal_position
            ) AS function_parameters
        FROM
            information_schema.routines
            LEFT JOIN information_schema.parameters ON routines.specific_name = parameters.specific_name
        WHERE
            routines.specific_schema = table_schema_pattern
            AND routine_type = 'FUNCTION'
            AND parameter_name IS NOT NULL
        GROUP BY
            parameters.specific_name -- Группировку делаем не по routine_name
            -- чтобы отобразить все перегрузки с одним именем
        ORDER BY
            function_name;

    SELECT COUNT(*) FROM tmp_part4_task2 INTO rows_count;
END;

$$ LANGUAGE plpgsql;

-- Проверяем работу процедуры
BEGIN;
DO $$ DECLARE rows_num INTEGER;
BEGIN CALL pr_part4_task2(rows_num);
RAISE NOTICE 'Count of functions: %',
rows_num;
END;
$$;
SELECT * FROM tmp_part4_task2;
END;

-- При реализации через курсор результат выводили бы так:
-- BEGIN;
-- DO $$ DECLARE rows_num INTEGER;
-- BEGIN CALL pr_part4_task2(rows_num);
-- RAISE NOTICE 'Count of functions: %', rows_num;
-- END;
-- $$;
-- FETCH ALL FROM "rc_resul_part4_task2";
-- END;

--------------------------------------------------------------------------------
-- Task 3
--------------------------------------------------------------------------------
-- Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL
-- DML триггеры в текущей базе данных. Выходной параметр возвращает количество
-- уничтоженных триггеров.
--
-- Все триггеры хранятся в information_schema.triggers, поэтому выборку делаем 
-- из неё.
-- 
-- Если триггер создается для нескольких event_manipulation (INSERT, UPDATE и
-- т.д), то записей будет несколько, пожтому нужен GROUP BY trigger_name
--
-- Триггер с одним названием можно повесить на несколько таблиц, поэтому
-- нужен ещё GROUP BY event_object_table
-- 
-- Блок EXCEPTION WHEN нужен, чтобы отлавливать ошибки удаления, если удалить
-- триггер не смогли по любой причине, то не изменяем INOUT trigger_count
--
CREATE
OR REPLACE PROCEDURE pr_part4_task3(
    INOUT trigger_count INTEGER,
    IN table_schema_pattern TEXT DEFAULT 'public'
) AS $$
DECLARE
    rec record;
BEGIN
    trigger_count := 0;
    FOR rec IN
        SELECT
            quote_ident(trigger_name) || ' ON ' || quote_ident(event_object_table) AS comm_to_drop
        FROM
            information_schema.triggers
        WHERE
            trigger_schema = table_schema_pattern
        GROUP BY trigger_name, event_object_table
    LOOP
        BEGIN
            trigger_count := trigger_count + 1;
            EXECUTE 'DROP TRIGGER ' || rec.comm_to_drop || ';';
        EXCEPTION WHEN OTHERS THEN
            trigger_count := trigger_count - 1; 
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Проверяем работу процедуры
DO $$ DECLARE trigges_deleted INTEGER;
BEGIN CALL pr_part4_task3(trigges_deleted);
RAISE NOTICE 'Triggers deleted: %',
trigges_deleted;
END;
$$;

--------------------------------------------------------------------------------
-- Task 4
--------------------------------------------------------------------------------
-- Создать хранимую процедуру с входным параметром, которая выводит имена и
-- описания типа объектов (только хранимых процедур и скалярных функций), в 
-- тексте которых на языке SQL встречается строка, задаваемая параметром
-- процедуры.
--
-- При реализации используем временную таблицу, как и в Task2
CREATE
OR REPLACE PROCEDURE pr_part4_task4(
    IN search_pattern TEXT,
    IN table_schema_pattern TEXT DEFAULT 'public'
) AS $$
BEGIN
    DROP TABLE IF EXISTS tmp_part4_task4;
    CREATE TEMP TABLE tmp_part4_task4 AS
        SELECT
            routine_name AS name,
            routine_type AS type
        FROM
            information_schema.routines
        WHERE
            routines.specific_schema = table_schema_pattern
            -- ищем заданный паттерн в коде объекта
            AND routine_definition ILIKE '%' || search_pattern || '%'
            -- Т.к. в задании "на языке SQL", то отсавляем только объекты,
            -- написанные на SQL
            AND routine_body = 'SQL'
            -- Т.к. в задании "(только хранимых процедур и скалярных функций)",
            -- хотя на самом деле routine_type может принимать только 2 значения
            AND (
                routine_type = 'FUNCTION'
                OR routine_type = 'PROCEDURE'
            )
        ORDER BY
            name;
END;

$$ LANGUAGE plpgsql;

-- Проверяем работу процедуры
BEGIN;
DO $$
BEGIN CALL pr_part4_task4('select');
END;
$$;
SELECT * FROM tmp_part4_task4;
END;