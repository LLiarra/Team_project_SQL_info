CREATE OR REPLACE PROCEDURE pr_part3_task16(INOUT result REFCURSOR = 'pr_result_part3_task16') AS
$$
BEGIN
    OPEN result FOR
        WITH RECURSIVE count_tasks_before AS (
            (SELECT "Title" AS Task, 0 AS PrevCount FROM Tasks
            WHERE "ParentTask" is NULL)
            UNION ALL
            (SELECT "Title", PrevCount+1 FROM tasks
            INNER JOIN count_tasks_before
            ON count_tasks_before.Task = tasks."ParentTask"))
        SELECT * FROM count_tasks_before;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pr_part3_task16();
FETCH ALL FROM "pr_result_part3_task16";
END;