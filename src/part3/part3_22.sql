CREATE OR REPLACE PROCEDURE pr_part3_task22(ref REFCURSOR, n_day INT, m_count INT) LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
SELECT "Peer"
FROM timetracking t
WHERE t."State" = 2 AND (CURRENT_DATE - t."Date") <= n_day
AND NOT t."Time" = (SELECT MAX(t2."Time") FROM timetracking t2 WHERE t2."Date" = t."Date" AND t2."Peer" = t."Peer")
GROUP BY "Peer"
HAVING COUNT(*) > m_count;
END; $$;

BEGIN;
CALL pr_part3_task22('task22', 30, 2);
FETCH ALL IN task22;
COMMIT;
