CREATE OR REPLACE PROCEDURE pr_part3_task23(ref REFCURSOR, day DATE DEFAULT CURRENT_DATE) LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
WITH min_time AS (
    SELECT t."Peer",
            MIN("Time") AS time,
            t."Date"
    FROM timetracking t
    WHERE "State" = 1 AND "Date" = day
    GROUP BY t."Peer", t."Date"
)
SELECT mt."Peer"
FROM min_time mt
WHERE mt.time = (SELECT MAX(mt2.time) FROM min_time mt2 GROUP BY mt2."Date");
END; $$;

BEGIN;
CALL pr_part3_task23('task23', '2022-12-18');
FETCH ALL IN task23;
COMMIT;