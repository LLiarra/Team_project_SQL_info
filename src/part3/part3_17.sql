-- Task 17
-- Найти "удачные" для проверок дни. День считается "удачным", если в нем есть
-- хотя бы N идущих подряд успешных проверки
--
CREATE
OR REPLACE PROCEDURE pr_part3_task17(
    IN N INT,
    IN result REFCURSOR = 'pr_result_part3_task17'
) AS $$
BEGIN
IF N < 1 THEN N := 1; END IF;
OPEN result FOR
WITH all_checks AS (
    SELECT
        Checks."ID",
        Checks."Date",
        P2P."Time",
        CASE
            WHEN (
                XP."XPAmount" IS NULL
                OR XP."XPAmount" < Tasks."MaxXP" * 0.8
                OR P2P."State" = 'Failure' :: check_status
                OR Verter."State" = 'Failure'
            ) THEN 0
            ELSE 1
        END AS check_result
    FROM
        Checks
        JOIN P2P ON P2P."Check" = Checks."ID"
        AND (
            P2P."State" = 'Success' :: check_status
            OR P2P."State" = 'Failure' :: check_status
        )
        LEFT JOIN Verter ON Verter."Check" = Checks."ID"
        AND (
            Verter."State" = 'Success' :: check_status
            OR Verter."State" = 'Failure' :: check_status
        )
        LEFT JOIN XP ON XP."Check" = Checks."ID"
        JOIN Tasks ON Tasks."Title" = Checks."Task"
),
all_checks_with_good_days AS (
    SELECT
        *,
        SUM("check_result") OVER (
            PARTITION BY "Date"
            ORDER BY
                "Date",
                "Time",
                "ID" ROWS BETWEEN N - 1 PRECEDING
                AND CURRENT ROW
        ) AS good_days
    FROM
        all_checks
)
SELECT
    "Date" AS "GoodLuckDays"
FROM
    all_checks_with_good_days
GROUP BY
    "Date"
HAVING
    MAX(good_days) >= N;

END;

$$ LANGUAGE plpgsql;

-- Проверяем работу процедуры
BEGIN;
CALL pr_part3_task17(2);
FETCH ALL FROM "pr_result_part3_task17";
END;