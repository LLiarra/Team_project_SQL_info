CREATE OR REPLACE PROCEDURE pr_part3_task7(ref refcursor) LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
WITH count AS (
    SELECT c."Date", c."Task", count(*) AS count
    FROM checks c GROUP BY "Date", "Task"
    ORDER BY c."Date"
), max_count AS (
    SELECT temp."Date", MAX(count) AS max
    FROM count AS temp GROUP BY temp."Date"
)
SELECT TO_CHAR(c."Date", 'DD.MM.YYYY') AS "Day",
       c."Task" AS "Task"
FROM count c
LEFT JOIN max_count m ON  m."Date" = c."Date"
WHERE m.max = count;
END; $$;

BEGIN;
CALL pr_part3_task7('task7');
FETCH ALL IN task7;
COMMIT;

