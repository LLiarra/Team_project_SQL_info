CREATE OR REPLACE PROCEDURE pr_part3_task24(ref REFCURSOR, min_count INT, day DATE DEFAULT CURRENT_DATE - 1) LANGUAGE plpgsql AS $$
BEGIN
IF min_count > 1440 THEN min_count := 1440; END IF;
OPEN ref FOR
WITH entry_campus AS (
    SELECT row_number() over (ORDER BY t."Peer", t."Time") AS id,
           t."Peer",
           t."Date",
           t."Time"
    FROM timetracking t
    WHERE t."State" = 1 AND t."Date" = day
    AND NOT t."Time" = (SELECT MIN(t1."Time") FROM timetracking t1 WHERE t."Date" = t1."Date" AND t."Peer" = t1."Peer")
    ORDER BY t."Peer", t."Time"
), left_campus AS (
    SELECT row_number() over (ORDER BY t2."Peer", t2."Time") AS id,
           t2."Peer",
           t2."Date",
           t2."Time"
    FROM timetracking t2
    WHERE t2."State" = 2 AND t2."Date" = day
    AND NOT t2."Time" = (SELECT MAX(t3."Time") FROM timetracking t3 WHERE t3."Date" = t2."Date" AND t3."Peer" = t2."Peer")
    ORDER BY t2."Peer", t2."Time"
), out_peers AS (
    SELECT ec."Peer",
           ec."Time" AS entry,
           lc."Time" AS out,
           lc."Date",
           (ec."Time" - lc."Time")::time,
           (ec."Time" - lc."Time")::time > make_time(min_count / 60, min_count - min_count / 60 * 60, 0)
    FROM entry_campus ec
    INNER JOIN left_campus lc ON ec."Peer" = lc."Peer" AND ec.id = lc.id
    AND (ec."Time" - lc."Time")::time > make_time(min_count / 60, min_count - min_count / 60 * 60, 0)
)
SELECT DISTINCT p."Peer"
FROM out_peers p;
END; $$;


BEGIN;
CALL pr_part3_task24('task24', 30, '2022-12-02');
FETCH ALL IN task24;
COMMIT;