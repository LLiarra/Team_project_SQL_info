/*
 Определить пира с наибольшим числом выполненных заданий
 ==============================
 Если выпадут два максимальных знчения, то берем один из них. Так как в задании сказано выбрать Пира (одного)
 */
CREATE OR REPLACE PROCEDURE pr_part3_task18(INOUT result REFCURSOR = 'pr_result_part3_task18') AS
$$
BEGIN
    OPEN result FOR
        WITH max_peer_tasks AS (
            SELECT "Peer", COUNT("Task") AS XP FROM XP
            LEFT JOIN Checks ON Checks."ID" = XP."Check"
            GROUP BY "Peer")
        SELECT * FROM max_peer_tasks
        ORDER BY XP DESC
        LIMIT 1;

END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_part3_task18();
FETCH ALL "pr_result_part3_task18";
END;