/*
 Определить пира с наибольшим количеством XP
 ==============================
 Если выпадут два максимальных знчения, то берем один из них. Так как в задании сказано выбрать Пира (одного).
 Из условия Part_3_task_14: если Пир сдавал несколько раз одно задание, то берется максимальное XP из сданных.
 */

CREATE OR REPLACE PROCEDURE pr_part3_task19(INOUT result REFCURSOR = 'pr_result_part3_task19') AS
$$
BEGIN
    OPEN result FOR
        WITH max_peer_tasks AS (
            SELECT "Peer", SUM(XP) AS XP FROM (
                SELECT Checks."Peer", Checks."Task", MAX("XPAmount") AS XP FROM XP
                LEFT JOIN Checks ON Checks."ID" = XP."Check"
                GROUP BY "Peer", "Task") AS count_xp
            GROUP BY "Peer")
        SELECT * FROM max_peer_tasks
        ORDER BY XP DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_part3_task19();
FETCH ALL "pr_result_part3_task19";
END;
