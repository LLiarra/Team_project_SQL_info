CREATE OR REPLACE PROCEDURE pr_part3_task9(ref REFCURSOR, block_name VARCHAR(10)) LANGUAGE plpgsql AS $$
DECLARE
task_count INT := (SELECT COUNT(*)
                   FROM tasks t
                   WHERE t."Title" ~ ('^' || block_name || '[0-9]'));
BEGIN
OPEN ref FOR
WITH peer_complete_tasks AS (
SELECT DISTINCT ON(c."Peer", c."Task") c."Peer",
                                       c."Task",
                                       c."Date"
FROM checks c
INNER JOIN p2p p ON c."ID" = p."Check"
FULL OUTER JOIN verter v ON c."ID" = v."Check"
WHERE c."Task" ~ ('^' || block_name || '[0-9]') AND (p."State" = 'Success'
                                         AND (v."State" = 'Success' OR v."State" IS NULL))
ORDER BY c."Peer", c."Task", c."Date" DESC
), uniq_count_tasks AS (
    SELECT "Peer",
           COUNT(*) AS count,
           MAX("Date") AS day
    FROM peer_complete_tasks
    GROUP BY "Peer"
)
SELECT ct."Peer",
       TO_CHAR(ct.day, 'dd.mm.yyyy') AS "Day"
FROM uniq_count_tasks ct
WHERE count = task_count
ORDER BY "Day";
END; $$;

BEGIN;
CALL pr_part3_task9('task9', 'C');
FETCH ALL IN task9;
COMMIT;