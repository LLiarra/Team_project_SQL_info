CREATE OR REPLACE PROCEDURE pr_part3_task25(ref REFCURSOR) LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
WITH all_month AS (
    SELECT month::date AS month
    FROM generate_series('2018-01-01', '2018-12-01', interval '1 month') AS month
),  all_entry AS (
    SELECT month,
           SUM(count) AS sum
    FROM (SELECT t."Peer",
                 COUNT(*) AS count,
                 TO_CHAR(p."Birthday", 'MM') AS month
          FROM timetracking t
          LEFT JOIN peers p on t."Peer" = p."Nickname"
          WHERE t."State" = 1
          GROUP BY t."Peer", TO_CHAR(p."Birthday", 'MM')) AS entry
    GROUP BY month
), early_entry AS (
    SELECT month,
           SUM(count) AS sum
    FROM (SELECT t."Peer",
                 COUNT(*) AS count,
                 TO_CHAR(p."Birthday", 'MM') AS month
          FROM timetracking t
          LEFT JOIN peers p on t."Peer" = p."Nickname"
          WHERE t."State" = 1 AND t."Time" < '12:00:00'
          GROUP BY t."Peer", TO_CHAR(p."Birthday", 'MM')) AS entry
    GROUP BY month
)
SELECT TO_CHAR(am.month, 'Month') AS "Month",
       ROUND(COALESCE(ee.sum * 100 / ae.sum, 0)) AS "EarlyEntries"
FROM all_month am
LEFT JOIN all_entry ae ON ae.month = TO_CHAR(am.month, 'MM')
LEFT JOIN early_entry ee ON ee.month = TO_CHAR(am.month, 'MM');
END; $$;

BEGIN;
CALL pr_part3_task25('task25');
FETCH ALL IN task25;
COMMIT;
