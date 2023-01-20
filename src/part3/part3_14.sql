CREATE OR REPLACE PROCEDURE pr_part3_task14(INOUT result REFCURSOR = 'pr_result_part3_task14') AS
$$
BEGIN
    OPEN result FOR
    SELECT "Peer", SUM(XP) AS XP FROM (
        SELECT Checks."Peer", Checks."Task", MAX("XPAmount") AS XP
        FROM XP LEFT JOIN checks
        ON XP."Check" = Checks."ID"
        GROUP BY Checks."Peer", Checks."Task") AS task_peer_xp
    GROUP BY "Peer"
    ORDER BY XP DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_part3_task14();
FETCH ALL FROM "pr_result_part3_task14";
END;