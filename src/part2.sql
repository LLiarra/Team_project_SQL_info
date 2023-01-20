/*
 Task_1: Написать процедуру добавления P2P проверки
 ==============================
 p2p_start_exists_id - По входным данным проверяем есть ли проверки для конкрентного Пира, задания и проверяющего
                       только со статусом Start; Здесь нужены (Group by и COUNT), т.к. данный пир с конкретным заданием
                       может несколько раз проверятся у одного Пира. Не делаем Distinct, т.к. у одного пира можем
                       находится в статусе проверки только один раз с конкрентным заданием ( Не может быть такого, чтобы
                       Пир №1 находился на проверке у Пира №2 с заданием №12 одновременно в стасе Start дважды.
                       В общем и целом, Cheks.ID не может быть в два раза в статусе Start.
 Пример вывода до Group by:
     ID  State_P2P
     23  Start     (проверяемая: Анна, проверяющий: Стас, задание: C3_s21_math)
     23  Failure   (проверяемая: Анна, проверяющий: Стас, задание: C3_s21_math)
     46  Start     (проверяемая: Анна, проверяющий: Стас, задание: C3_s21_math)
     46  Success   (проверяемая: Анна, проверяющий: Стас, задание: C3_s21_math)
     58  Start     (проверяемая: Анна, проверяющий: Стас, задание: C3_s21_math) Решила еще раз сдать проект.
 Пример вывода p2p_start_exists_id = 58 - Анна уже сдает C3_s21_math Стасу.
 ------------------------------
 checks_new_id - сохраняем id новой строчки, добавленная в таблицу Checks.
 */
CREATE OR REPLACE PROCEDURE fill_p2p(IN peer_tested VARCHAR, IN peer_checking VARCHAR, IN task_name VARCHAR(25),
    IN status_p2p p2p."State"%TYPE, IN time_p2p TIME)
AS $$
DECLARE
    p2p_start_exists_id BIGINT = (
        SELECT "ID"
        FROM (SELECT "Check", "State" FROM P2P WHERE "CheckingPeer" = peer_checking) AS p2p_start
        LEFT JOIN (SELECT "ID" FROM Checks WHERE "Peer" = peer_tested AND "Task" = task_name) AS checks_peer_task
        ON p2p_start."Check" = checks_peer_task."ID"
        GROUP BY "ID"
        HAVING COUNT("State") = 1);
    checks_new_id BIGINT = 0;
BEGIN
    IF status_p2p = 'Start' THEN
        IF p2p_start_exists_id != 0 THEN
            RAISE NOTICE 'This "%" task "%" peer and "%" checking peer in the "Start" status is already exist!',
                          task_name, peer_tested, peer_checking;
        ELSE
            INSERT INTO Checks ("Peer", "Task")
            VALUES (peer_tested, task_name) RETURNING "ID" INTO checks_new_id;
            INSERT INTO P2P ("Check", "CheckingPeer", "State", "Time")
            VALUES (checks_new_id, peer_checking, status_p2p, time_p2p);
        END IF;
    ELSE
        IF p2p_start_exists_id != 0 THEN
            INSERT INTO P2P ("Check", "CheckingPeer", "State", "Time")
            VALUES (p2p_start_exists_id, peer_checking, status_p2p, time_p2p);
        ELSE
           RAISE NOTICE 'This "%" task "%" peer and "%" checking peer was not started!', task_name, peer_tested, peer_checking;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

/*
 Task_2: Написать процедуру добавления проверки Verter'ом
 ==============================
 p2p_success_exists_id - проверяем, что такой Checks.ID есть в таблице P2P и он находится в статусе Success.
 ------------------------------
 p2p_success_exists_id: NULL - Checks.ID не существует в таблице Verter (Нет совпадений -> временная таблица
                               не определена;
                        1 - Checks.ID встречается один раз (status "Start");
                        2 - Checks.ID встречается два раза (status "Start" и status "Success/Failure").
 ------------------------------
 Есть проверки на то, что:
    1. Проверка прошла успешно в P2P;
    2. Нет ли уже существующего "Start" в таблице Verter, если мы кладем со статусом "Start";
    3. Нет ли уже существующего "Success/Failure" в таблице Verter, если мы кладем со статусом "Success/Failure";
    4. Если кладем со статусом "Success/Failure", в таблице Verter должен быть этот же Checks.ID со статусом "Start" в
       таблице Verter.
 */
CREATE OR REPLACE PROCEDURE fill_verter(IN peer_tested VARCHAR, IN task_name VARCHAR(25), IN status_p2p p2p."State"%TYPE,
    IN time_p2p TIME)
AS $$
DECLARE
    p2p_success_exists_id BIGINT = (
        SELECT checks_specific."ID" FROM (
            SELECT * FROM Checks
            WHERE "Peer" = peer_tested AND "Task" = task_name
            ORDER BY "ID" DESC
            LIMIT 1) AS checks_specific
        INNER JOIN P2P
        ON P2P."Check" = checks_specific."ID"
        WHERE P2P."State" = 'Success');
    verter_what_status_exists BIGINT = 0;
BEGIN
    IF p2p_success_exists_id is NULL THEN
        RAISE NOTICE 'This "%" task with "%" peer is not in the "Success" P2P status!', task_name, peer_tested;
    ELSIF status_p2p = 'Start' THEN
        verter_what_status_exists = (SELECT COUNT("Check") FROM Verter WHERE "Check" = p2p_success_exists_id);
        IF verter_what_status_exists = 0 THEN
            INSERT INTO Verter ("Check", "State", "Time")
            VALUES (p2p_success_exists_id, status_p2p, time_p2p);
        ELSE
            RAISE NOTICE 'This "%" task with "%" peer is already in the "Start" Verter status!',task_name, peer_tested;
        END IF;
    ELSE
        verter_what_status_exists = (SELECT COUNT("Check") FROM Verter WHERE "Check" = p2p_success_exists_id);
        IF verter_what_status_exists = 1  THEN
            INSERT INTO Verter ("Check", "State", "Time")
            VALUES (p2p_success_exists_id, status_p2p, time_p2p);
        ELSIF verter_what_status_exists = 0 THEN
            RAISE NOTICE 'This "%" task with "%" peer is not in the "Start" Verter status!', task_name, peer_tested;
        ELSE
            RAISE NOTICE 'This "%" task with "%" peer is already checked by Verter!', task_name, peer_tested;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Task_3
CREATE OR REPLACE FUNCTION fnc_trg_transferred_point() RETURNS TRIGGER AS
$$
DECLARE
    peer_start VARCHAR = (SELECT "Peer" FROM Checks WHERE Checks."ID" = NEW."Check");
BEGIN
    UPDATE TransferredPoints SET "PointsAmount" = "PointsAmount" + 1 WHERE "CheckingPeer" = NEW.CheckingPeer AND "CheckedPeer" = peer_start;
    IF NOT FOUND THEN
        INSERT INTO TransferredPoints ("CheckingPeer", "CheckedPeer", "PointsAmount")
        VALUES (NEW.CheckingPeer, peer_start, 1);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transferred_point
    AFTER INSERT ON P2P
    FOR EACH ROW
    WHEN(NEW.State = 'Start')
    EXECUTE FUNCTION fnc_trg_transferred_point();

-- Task_4
CREATE OR REPLACE FUNCTION fnc_trg_xp() RETURNS TRIGGER AS
$$
DECLARE
    p2p_success_exists_id BIGINT = (SELECT P2P."Check" FROM P2P WHERE NEW."Check" = P2P."Check" AND P2P."State" = 'Success');
    xp_amount_max_task INT = (
        SELECT Tasks."MaxXP"
        FROM (SELECT * FROM Checks WHERE Checks."ID" = NEW."Check") AS checks_new
        INNER JOIN Tasks
        ON Tasks."Title" = checks_new."Task");
    verter_success_exists_id VARCHAR = (
        SELECT "State" FROM Verter
        WHERE NEW."Check" = Verter."Check"
        ORDER BY "State" DESC
        LIMIT 1);
BEGIN
    IF xp_amount_max_task < NEW."XPAmount" THEN
        RAISE  NOTICE 'XPAmount more than MaxXP!';
        RETURN NULL;
    ELSIF p2p_success_exists_id is NULL THEN
        RAISE NOTICE 'P2P status is not Success!';
        RETURN NULL;
    ELSIF verter_success_exists_id = 'Failure' OR verter_success_exists_id = 'Start' THEN
        RAISE NOTICE 'Verter status is not Success';
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_xp
    BEFORE INSERT ON XP
    FOR EACH ROW
    EXECUTE FUNCTION fnc_trg_xp();


-- TASK_1_3_КЕЙС_1 (Ловим RAISE NOTICE, когда вводятся два раз подряд одинаковые значения со статусом "Start"
CALL fill_p2p('great', 'honest', 'C3_s21_math', 'Start', '11:20');
CALL fill_p2p('great', 'honest', 'C3_s21_math', 'Start', '11:20');
-- TASK_1_КЕЙС_2 (Ловим RAISE NOTICE, когда вводятся два раз подряд одинаковые значения со статусом "Success/Failure"
CALL fill_p2p('great', 'honest', 'C3_s21_math', 'Success', '11:20');
CALL fill_p2p('great', 'honest', 'C3_s21_math', 'Success', '11:20');
CALL fill_p2p('great', 'honest', 'C3_s21_math', 'Failure', '11:20');
-- TASk_2_3_KЕЙС_1 (Если Р2P не SUCCESS. Вернет RAISE NOTICE)
CALL fill_p2p('great', 'honest', 'C3_s21_math', 'Start', '11:20');
CALL fill_verter('great', 'C3_s21_math', 'Start', '12:00');
-- TASk_2_KЕЙС_1 (Если Verter не начат. Но хотим ввести Success или Failure. Вернет RAISE NOTICE)
CALL fill_verter('great', 'C3_s21_math', 'Success', '12:00');
CALL fill_verter('great', 'C3_s21_math', 'Failure', '12:00');
-- TASk_2_KЕЙС_2 (Если Verter уже Start. Вернет RAISE NOTICE)
CALL fill_p2p('great', 'honest', 'C3_s21_math', 'Success', '11:20');
CALL fill_verter('great', 'C3_s21_math', 'Start', '12:00');
CALL fill_verter('great', 'C3_s21_math', 'Start', '12:00');
-- TASk_2_KЕЙС_2 (Если Verter уже SUCCESS или Failure. Но хотим ввести Success или Failure. Вернет RAISE NOTICE)
CALL fill_verter('great', 'C3_s21_math', 'Success', '12:00');
CALL fill_verter('great', 'C3_s21_math', 'Success', '12:00');
-- TASk_4_KЕЙС_1 (Если Verter уже SUCCESS или Failure. Но хотим ввести Success или Failure. Вернет RAISE NOTICE)
INSERT INTO XP ("Check", "XPAmount")
VALUES ((SELECT MAX("ID") FROM Checks), 200);