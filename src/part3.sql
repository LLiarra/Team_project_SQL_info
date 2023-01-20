--------------------------------------------------------------------------------
-- Task 1
--------------------------------------------------------------------------------
-- Написать функцию, возвращающую таблицу TransferredPoints в более
-- человекочитаемом виде
-- 
-- При реализации учитваем, что при JOIN у нас может быть ситуация, что пара
-- p1-p2 у нас может быть в таблице, а p2-p1 может отсутстовать. Чтобы сохранить
-- такие записи нужно второе условие в WHERE (OR t2.id IS NULL)
CREATE
OR REPLACE FUNCTION fnc_part3_task1() RETURNS TABLE (
    "Peer1" VARCHAR,
    "Peer2" VARCHAR,
    "PointsAmount" INTEGER
) AS $$ BEGIN RETURN QUERY
SELECT
    t1."CheckingPeer" AS Peer1,
    t1."CheckedPeer" AS Peer2,
    COALESCE(t1."PointsAmount", 0) - COALESCE(t2."PointsAmount", 0) AS "PointsAmount"
FROM
    TransferredPoints AS t1 FULL
    JOIN TransferredPoints AS t2 ON t1."CheckingPeer" = t2."CheckedPeer"
    AND t2."CheckingPeer" = t1."CheckedPeer"
WHERE
    t1."ID" > t2."ID"
    OR t2."ID" IS NULL;

END;

$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Проверяем работу функции
--------------------------------------------------------------------------------
SELECT * FROM fnc_part3_task1();

--------------------------------------------------------------------------------
-- Task 2
--------------------------------------------------------------------------------
-- Написать функцию, которая возвращает таблицу вида: ник пользователя, название
-- проверенного задания, кол-во полученного XP
--
--
-- Создаем вспомогательную view (в качестве эксперимента)
CREATE
OR REPLACE VIEW mv_checks_result AS
SELECT
    Checks."ID",
    Checks."Peer",
    Checks."Task",
    P2P."State" AS p2p_state,
    Verter."State" AS verter_state
FROM
    Checks
    JOIN P2P ON P2P."Check" = Checks."ID"
    AND (
        P2P."State" = 'Success' :: check_status
        OR P2P."State" = 'Failure' :: check_status
    )
    LEFT JOIN Verter ON Verter."Check" = Checks."ID"
    AND (
        Verter."State" = 'Success' :: check_status
        OR Verter."State" = 'Failure' :: check_status
    );

-- Создаём функцию
CREATE
OR REPLACE FUNCTION fnc_part3_task2() RETURNS TABLE (
    "Peer" VARCHAR,
    "Task" VARCHAR,
    "XP" INTEGER
) AS $$ BEGIN RETURN QUERY
SELECT
    cr."Peer",
    cr."Task",
    XP."XPAmount" AS "XP" -- можно использовать COALESCE(XP.XPAmount, 0)
    -- НО наличие сданного проекта без записи в XP - скорее баг
FROM
    mv_checks_result AS cr
    JOIN XP ON XP."Check" = cr."ID"
WHERE
    cr.p2p_state = 'Success' :: check_status
    AND (
        cr.verter_state = 'Success' :: check_status
        OR cr.verter_state IS NULL
    );

END;

$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Проверяем работу функции
--------------------------------------------------------------------------------
SELECT * FROM fnc_part3_task2();

--------------------------------------------------------------------------------
-- Task 3
--------------------------------------------------------------------------------
-- Написать функцию, определяющую пиров, которые не выходили из кампуса 
-- в течение всего дня
--
-- Под действием "выходить" подразумеваются все покидания кампуса за день,
-- кроме последнего.
--
-- В течение одного дня должно быть одинаковое количество записей с состоянием 1
-- и состоянием 2 для каждого пира.
--
-- Таким образом мы просто находим всех пиров, у которых количество записей со
-- статусом 2 за день равно 1
CREATE
OR REPLACE FUNCTION fnc_part3_task3(pdate DATE) RETURNS TABLE ("Peer" VARCHAR) AS $$ 
BEGIN
RETURN QUERY
    SELECT
        TimeTracking."Peer"
    FROM
        TimeTracking
    WHERE
        "Date" = pdate
        AND "State" = 2
    GROUP BY
        TimeTracking."Peer"
    HAVING
        COUNT("State") = 1;

END;

$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Проверяем работу функции
--------------------------------------------------------------------------------
SELECT * FROM fnc_part3_task3('2022-12-01');

--------------------------------------------------------------------------------
-- Task 4
--------------------------------------------------------------------------------
-- Найти процент успешных и неуспешных проверок за всё время
--
-- Проверка считается успешной, если соответствующий P2P этап успешен, а этап
-- Verter успешен, либо отсутствует.
-- Проверка считается неуспешной, хоть один из этапов неуспешен.
-- То есть проверки, в которых ещё не завершился этап P2P, или этап P2P успешен,
-- но ещё не завершился этап Verter, не относятся ни к успешным, ни к неуспешным.
--
-- При реализации используем вспомогательный VIEW, который сделали в Task2
CREATE
OR REPLACE PROCEDURE prc_part3_task4(
    IN result_cur REFCURSOR = 'rc_resul_part3_task4'
) AS $$
DECLARE
failure_count INTEGER := (
    SELECT
        COUNT("ID")
    FROM
        mv_checks_result
    WHERE
        p2p_state = 'Failure' :: check_status
        OR verter_state = 'Failure' :: check_status
);

total_count INTEGER := (
    SELECT
        COUNT("ID")
    FROM
        mv_checks_result
);

success_count INTEGER := (total_count - failure_count);

BEGIN OPEN result_cur FOR
SELECT
    ROUND((success_count / total_count :: numeric) * 100) AS "SuccessfulChecks",
    ROUND((failure_count / total_count :: numeric) * 100) AS "UnsuccessfulChecks";

END;

$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Проверяем работу процедуры
--------------------------------------------------------------------------------
BEGIN;
    CALL prc_part3_task4();
    FETCH ALL FROM "rc_resul_part3_task4";
END;

--------------------------------------------------------------------------------
-- Task 5
--------------------------------------------------------------------------------
-- Посчитать изменение в количестве пир поинтов каждого пира по таблице
-- TransferredPoints
--
-- При реализации получаем список всех пиров по таблице Peers, чтобы в
-- результате были и пиры которые не участововали в проверках вообще
-- или учатвововали только как проверяющие или только как проверяемые
CREATE
OR REPLACE PROCEDURE prc_part3_task5(
    IN result_cur REFCURSOR = 'rc_resul_part3_task5'
) AS $$ BEGIN OPEN result_cur FOR
SELECT
    t_plus."Nickname" AS "Peer",
    t_plus.total - t_minus.total AS "PointsChange"
FROM
    (
        (
            SELECT
                Peers."Nickname",
                SUM(COALESCE(TransferredPoints."PointsAmount", 0)) AS total
            FROM
                Peers
                LEFT JOIN TransferredPoints ON TransferredPoints."CheckingPeer" = Peers."Nickname"
            GROUP BY
                Peers."Nickname"
        ) AS t_plus
        JOIN (
            SELECT
                Peers."Nickname",
                SUM(COALESCE(TransferredPoints."PointsAmount", 0)) AS total
            FROM
                Peers
                LEFT JOIN TransferredPoints ON TransferredPoints."CheckedPeer" = Peers."Nickname"
            GROUP BY
                Peers."Nickname"
        ) AS t_minus ON t_plus."Nickname" = t_minus."Nickname"
    )
ORDER BY
    "PointsChange" DESC;

END;

$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Проверяем работу процедуры
--------------------------------------------------------------------------------
BEGIN;
    CALL prc_part3_task5();
    FETCH ALL FROM "rc_resul_part3_task5";
END;

--------------------------------------------------------------------------------
-- Task 6
--------------------------------------------------------------------------------
-- Посчитать изменение в количестве пир поинтов каждого пира по таблице,
-- возвращаемой первой функцией из Part 3
--
-- В целом концепция аналогична Task 5, только не считаем изменение поинтов для
-- пиров, а получаем эти данные из fnc_part3_task1()
-- 
-- В первой выборке получаем, сколько очков полчил пир 1 от пира 2
-- Во второй выборке получаем, сколько очков полчил пир 2 от пира 1
-- Т.к. количество отрицательное, если пир 2 получил от пира 1 больше поинтов,
-- то при подсчете реузльтата данные из выборки 1 вычитаем из данных выборки 2
CREATE
OR REPLACE PROCEDURE prc_part3_task6(
    IN result_cur REFCURSOR = 'rc_resul_part3_task6'
) AS $$ BEGIN OPEN result_cur FOR
SELECT
    t_plus."Peer" AS "Peer",
    t_plus."PointsChange" - t_minus."PointsChange" AS "PointsChange"
FROM
    (
        (
            SELECT
                Peers."Nickname" AS "Peer",
                SUM(COALESCE(fnc."PointsAmount", 0)) AS "PointsChange"
            FROM
                Peers
                LEFT JOIN (
                    SELECT
                        *
                    FROM
                        fnc_part3_task1()
                ) AS fnc ON Peers."Nickname" = fnc."Peer1"
            GROUP BY
                Peers."Nickname"
        ) AS t_plus
        JOIN (
            SELECT
                Peers."Nickname" AS "Peer",
                SUM(COALESCE(fnc."PointsAmount", 0)) AS "PointsChange"
            FROM
                Peers
                LEFT JOIN (
                    SELECT
                        *
                    FROM
                        fnc_part3_task1()
                ) AS fnc ON Peers."Nickname" = fnc."Peer2"
            GROUP BY
                Peers."Nickname"
        ) AS t_minus ON t_plus."Peer" = t_minus."Peer"
    )
ORDER BY
    "PointsChange" DESC;

END;

$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Проверяем работу процедуры
--------------------------------------------------------------------------------
BEGIN;
    CALL prc_part3_task6();
    FETCH ALL FROM "rc_resul_part3_task6";
END;

--------------------------------------------------------------------------------
-- Task 7
--------------------------------------------------------------------------------
-- TODO

--------------------------------------------------------------------------------
-- Task 8
--------------------------------------------------------------------------------
-- Определить длительность последней P2P проверки
--
CREATE
OR REPLACE PROCEDURE pr_part3_task8(ref refcursor) AS $$
DECLARE
number_of_check INT := (
    SELECT
        p1."Check"
    FROM
        p2p p1
        LEFT JOIN p2p p2 ON p1."Check" = p2."Check"
    WHERE
        p1."State" = 'Start'
        AND (
            p2."State" = 'Success'
            OR p2."State" = 'Failure'
        )
    ORDER BY
        p1."Check" DESC
    LIMIT
        1
);

begin_check TIME := (
    SELECT
        "Time"
    FROM
        p2p
    WHERE
        "Check" = number_of_check
        AND "State" = 'Start'
);

end_check TIME := (
    SELECT
        "Time"
    FROM
        p2p
    WHERE
        "Check" = number_of_check
        AND (
            "State" = 'Success'
            OR "State" = 'Failure'
        )
);

BEGIN OPEN ref FOR
    SELECT end_check - begin_check AS "CheckDuration";

END;

$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part3_task8('task8');
    FETCH ALL IN task8;
END;

--------------------------------------------------------------------------------
-- Task 9
--------------------------------------------------------------------------------
-- TODO

--------------------------------------------------------------------------------
-- Task 10
--------------------------------------------------------------------------------
-- TODO

--------------------------------------------------------------------------------
-- Task 11
--------------------------------------------------------------------------------
-- Определить процент пиров, которые:
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному
--
CREATE
OR REPLACE PROCEDURE pr_part3_task11(
    ref REFCURSOR,
    block1 VARCHAR(10),
    block2 VARCHAR(10)
) AS $$
DECLARE
all_peers INT := (
    SELECT
        COUNT(*)
    FROM
        peers
);

BEGIN
OPEN ref FOR
    WITH temp_work_block1 AS (
        SELECT
            c."Peer"
        FROM
            checks c
        WHERE
            c."Task" ~ ('^' || block1 || '[0-9]')
        GROUP BY
            "Peer"
    ),
    temp_work_block2 AS (
        SELECT
            c."Peer"
        FROM
            checks c
        WHERE
            c."Task" ~ ('^' || block2 || '[0-9]')
        GROUP BY
            "Peer"
    ),
    temp_work_all_blocks AS (
        (
            SELECT
                *
            FROM
                temp_work_block1
        )
        INTERSECT
        (
            SELECT
                *
            FROM
                temp_work_block2
        )
    ),
    temp_did_not_started AS (
        (
            (
                SELECT
                    p."Nickname"
                FROM
                    peers p
            )
            EXCEPT
                (
                    (
                        SELECT
                            *
                        FROM
                            temp_work_block1
                    )
                    UNION
                    (
                        SELECT
                            *
                        FROM
                            temp_work_block2
                    )
                )
        )
    )
    SELECT
        ROUND(
            (
                SELECT
                    COUNT(*)
                FROM
                    temp_work_block1
            ) * 100 / all_peers :: numeric
        ) AS "StartedBlock1",
        ROUND(
            (
                SELECT
                    COUNT(*)
                FROM
                    temp_work_block2
            ) * 100 / all_peers :: numeric
        ) AS "StartedBlock2",
        ROUND(
            (
                SELECT
                    COUNT(*)
                FROM
                    temp_work_all_blocks
            ) * 100 / all_peers :: numeric
        ) AS "StartedBothBlocks",
        ROUND(
            (
                SELECT
                    COUNT(*)
                FROM
                    temp_did_not_started
            ) * 100 / all_peers :: numeric
        ) AS "DidntStartAnyBlock";

END;

$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Проверяем работу процедуры
--------------------------------------------------------------------------------
BEGIN;
    CALL pr_part3_task11('task11', 'DO', 'C');
    FETCH ALL IN task11;
END;