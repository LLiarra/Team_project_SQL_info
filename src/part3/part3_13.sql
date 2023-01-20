CREATE OR REPLACE PROCEDURE pr_part3_task13(ref refcursor) LANGUAGE plpgsql AS $$
DECLARE
    fail INT := (SELECT COUNT(*)
        FROM checks c
        LEFT JOIN p2p p ON c."ID" = p."Check"
        LEFT JOIN peers pr ON c."Peer" = pr."Nickname"
        LEFT JOIN verter v ON c."ID" = v."Check"
        WHERE TO_CHAR(c."Date", 'MM.DD') = to_char(pr."Birthday", 'MM.DD')
          AND ((p."State" = 'Failure' AND (v."State" = 'Failure' OR v."State" IS NULL))
          OR (p."State" = 'Success' AND v."State" = 'Failure')));
    success INT := (SELECT COUNT(*)
        FROM checks c
        LEFT JOIN p2p p ON c."ID" = p."Check"
        LEFT JOIN peers pr ON c."Peer" = pr."Nickname"
        LEFT JOIN verter v ON c."ID" = v."Check"
        WHERE TO_CHAR(c."Date", 'MM.DD') = TO_CHAR(pr."Birthday", 'MM.DD')
            AND (p."State" = 'Success' AND (v."State" = 'Success' OR v."State" IS NULL)));
BEGIN
    OPEN ref FOR
    SELECT ROUND(100 * success / (success + fail)::numeric) AS "SuccessfulChecks",
           ROUND(100 * fail / (success + fail)::numeric) AS "UnsuccessfulChecks";
END; $$;

BEGIN;
CALL pr_part3_task13('task13');
FETCH ALL IN task13;
COMMIT;