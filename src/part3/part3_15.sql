CREATE OR REPLACE PROCEDURE pr_part3_task15(
    IN task_1 VARCHAR(25),
    IN task_2 VARCHAR(25),
    IN task_3 VARCHAR(25),
    INOUT result REFCURSOR = 'pr_result_part3_task15') AS
$$
BEGIN
OPEN result FOR
    WITH all_success_2_tasks AS(
        SELECT "Peer" FROM XP
        LEFT JOIN Checks ON XP."Check" = Checks."ID"
        WHERE "Task" = task_1 OR "Task" = task_2
        GROUP BY "Peer"
        HAVING COUNT("Task") = 2
        ),
    all_done_3d_task AS(
        SELECT DISTINCT "Peer" FROM XP
        LEFT JOIN Checks ON XP."Check" = Checks."ID"
        WHERE "Task" = task_3
        )
    SELECT "Peer" FROM all_success_2_tasks
    EXCEPT
    SELECT "Peer" FROM all_done_3d_task;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_part3_task15('C2_s21_string+', 'C3_s21_math', 'C5_s21_matrix');
FETCH ALL FROM "pr_result_part3_task15";
END;