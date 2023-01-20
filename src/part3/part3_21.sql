CREATE OR REPLACE PROCEDURE pr_part3_task21(ref REFCURSOR, min_time TIME, n_count INT) LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
WITH all_peer AS (
    SELECT "Peer",
           "Date"
    FROM timetracking
    WHERE "State" = 1 AND "Time" < min_time
    GROUP BY "Peer", "Date"
)
SELECT "Peer"
FROM all_peer
GROUP BY "Peer"
HAVING count("Peer") >= n_count;
END; $$;

BEGIN;
CALL pr_part3_task21('task21', '08:15:18', 1);
FETCH ALL IN task21;
COMMIT;