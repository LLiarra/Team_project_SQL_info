CREATE OR REPLACE PROCEDURE pr_part3_task20(ref REFCURSOR, day DATE DEFAULT CURRENT_DATE) LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
WITH in_school AS (
    SELECT row_number() over(ORDER BY t."Peer", t."Time") AS id,
           t."Peer",
           t."Time" AS state_in
    FROM timetracking t
    WHERE t."State" = 1 AND t."Date" = day
    ORDER BY t."Peer", t."Time"
), out_school AS (
    SELECT row_number() over(ORDER BY t."Peer", t."Time") AS id,
           t."Peer",
           t."Time" AS state_out
    FROM timetracking t
    WHERE t."State" = 2 AND t."Date" = day
    ORDER BY t."Peer", t."Time"
), all_int AS (
SELECT i."Peer",
       state_in,
       state_out,
       state_out - state_in AS int
FROM in_school i
LEFT JOIN out_school o ON i."Peer" = o."Peer" AND i.id = o.id
)
SELECT ai."Peer"
FROM all_int ai
GROUP BY ai."Peer"
HAVING (SUM(ai.int) = (SELECT SUM(ai2.int) as sum FROM all_int ai2 GROUP BY ai2."Peer" ORDER BY sum DESC LIMIT 1))
LIMIT 1;
END; $$;

BEGIN;
CALL pr_part3_task20('task20', '2022-12-18');
FETCH ALL IN task20;
COMMIT;