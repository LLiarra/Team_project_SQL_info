CREATE TABLE Peers
("Nickname" VARCHAR(25) UNIQUE PRIMARY KEY,
 "Birthday" DATE NOT NULL
 );

CREATE TABLE Tasks
("Title" VARCHAR(50) UNIQUE PRIMARY KEY,
 "ParentTask" VARCHAR(50) DEFAULT NULL,
 "MaxXP" NUMERIC NOT NULL,
 CONSTRAINT fk_tasks_parent_task FOREIGN KEY ("ParentTask") REFERENCES Tasks("Title")
 );

CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE Checks
("ID" SERIAL PRIMARY KEY,
 "Peer" VARCHAR(25) NOT NULL,
 "Task" VARCHAR(25) NOT NULL,
 "Date" DATE NOT NULL DEFAULT CURRENT_DATE,
 CONSTRAINT fk_checks_nickname FOREIGN KEY ("Peer") REFERENCES Peers("Nickname"),
 CONSTRAINT fk_checks_task FOREIGN KEY ("Task") REFERENCES Tasks("Title")
);

-- Ускоряет выборки с Peer + Task с сортировкой, которые часто нужны
CREATE INDEX IF NOT EXISTS idx_checks_peer_task ON Checks USING btree("Peer", "Task");

CREATE TABLE P2P
("ID" SERIAL PRIMARY KEY,
 "Check" BIGINT NOT NULL,
 "CheckingPeer" VARCHAR(25) NOT NULL,
 "State" check_status NOT NULL,
 "Time" TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIME,
 CONSTRAINT fk_p2p_check FOREIGN KEY ("Check") REFERENCES Checks("ID"),
 CONSTRAINT fk_p2p_checking_peer FOREIGN KEY ("CheckingPeer") REFERENCES Peers("Nickname")
 );

-- Ускоряет выборки по State, которые часто нужны
CREATE INDEX IF NOT EXISTS idx_p2p_state ON P2P USING btree("State");

CREATE TABLE Verter
("ID" SERIAL PRIMARY KEY,
 "Check" BIGINT NOT NULL,
 "State" check_status NOT NULL,
 "Time" TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIME,
 CONSTRAINT fk_verter_check FOREIGN KEY ("Check") REFERENCES Checks("ID")
 );

-- Ускоряет выборки по State, которые часто нужны
CREATE INDEX IF NOT EXISTS idx_verter_state ON Verter USING btree("State");

CREATE TABLE TransferredPoints
("ID" SERIAL PRIMARY KEY,
 "CheckingPeer" VARCHAR(25) NOT NULL,
 "CheckedPeer" VARCHAR(25) NOT NULL,
 "PointsAmount" INT NOT NULL,
 CONSTRAINT fk_transferred_points_checking_peer FOREIGN KEY ("CheckingPeer") REFERENCES Peers("Nickname"),
 CONSTRAINT fk_transferred_points_checked_peer FOREIGN KEY ("CheckedPeer") REFERENCES Peers("Nickname")
 );

-- Индекс для обеспечения гарантии уникальности записей пар пир1-пир2
-- и уменьшает стоимость JOIN
CREATE UNIQUE INDEX IF NOT EXISTS idx_transferred_points_checkingpeer_checkedpeer_unique ON TransferredPoints USING btree ("CheckingPeer", "CheckedPeer");
-- Ускоряет JOIN по nickname
CREATE INDEX IF NOT EXISTS idx_transferred_points_checkingpeer ON TransferredPoints USING btree ("CheckingPeer");
-- Ускоряет JOIN по nickname
CREATE INDEX IF NOT EXISTS idx_transferred_points_checkedpeer ON TransferredPoints USING btree ("CheckedPeer");

CREATE TABLE Friends
("ID" SERIAL PRIMARY KEY,
 "Peer1" VARCHAR(25) NOT NULL,
 "Peer2" VARCHAR(25) NOT NULL,
 CONSTRAINT fk_friends_peer1 FOREIGN KEY ("Peer1") REFERENCES Peers("Nickname"),
 CONSTRAINT fk_friends_peer2 FOREIGN KEY ("Peer2") REFERENCES Peers("Nickname")
 );

CREATE TABLE Recommendations
("ID" SERIAL PRIMARY KEY,
 "Peer" VARCHAR(25) NOT NULL,
 "RecommendedPeer" VARCHAR(25) NOT NULL,
 CONSTRAINT fk_recommendations_peer FOREIGN KEY ("Peer") REFERENCES Peers("Nickname"),
 CONSTRAINT fk_recommendations_recommended_peer FOREIGN KEY ("RecommendedPeer") REFERENCES Peers("Nickname")
 );

CREATE TABLE XP
("ID" SERIAL PRIMARY KEY,
 "Check" BIGINT NOT NULL,
 "XPAmount" INT NOT NULL,
 CONSTRAINT fk_xp_check FOREIGN KEY ("Check") REFERENCES Checks("ID")
 );

-- Индекс для обеспечения гарантии уникальности записей в XP
-- (Для одной проверки Check только одна запись с опытом)
CREATE UNIQUE INDEX IF NOT EXISTS idx_xp_check_unique ON XP USING btree("Check");

CREATE TABLE TimeTracking
("ID" SERIAL PRIMARY KEY,
 "Peer" VARCHAR(25) NOT NULL,
 "Date" DATE NOT NULL DEFAULT CURRENT_DATE,
 "Time" TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIME,
 "State" INT NOT NULL,
 CONSTRAINT fk_time_tracking_state CHECK ("State" IN (1, 2)),
 CONSTRAINT fk_time_tracking_peer FOREIGN KEY ("Peer") REFERENCES Peers("Nickname")
 );

-- Ускоряет выборки по дате и статусу посещений
CREATE INDEX IF NOT EXISTS idx_checks_date_state ON TimeTracking USING btree("Date", "State");

----import
CREATE OR REPLACE FUNCTION fnc_get_col_names(target_table_name TEXT, delim VARCHAR(1)) RETURNS TEXT AS $$
DECLARE
    schema TEXT := (SELECT "current_schema"());
BEGIN
RETURN (SELECT string_agg(format('%s', quote_ident(cols.column_name)), delim)
        FROM (SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = schema AND table_name = target_table_name AND column_name <> 'ID') AS cols);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE pr_import_table(target_table_name TEXT, path TEXT, delim VARCHAR(1)) LANGUAGE plpgsql AS $$
    DECLARE
    schema TEXT := (SELECT "current_schema"());

    BEGIN
    EXECUTE format('COPY %1$s(%3$s) FROM %2$L WITH DELIMITER ''%4$s'' CSV HEADER', target_table_name,
                                                                                   path,
                                                                                    fnc_get_col_names(target_table_name, delim),
                                                                                    delim);
    IF EXISTS(SELECT 1
              FROM information_schema.columns
              WHERE table_schema = schema AND table_name = target_table_name AND column_name = 'id')
    THEN
        EXECUTE format('SELECT setval(''%1$s_id_seq'', (SELECT MAX(id) FROM %1$s), false);', target_table_name);
    END IF;
END;
$$;

-- Создаем глобальную переменную
SET path_to_project.var TO '/home/harmonic/Project/SQL/SQL2_Info21_v1.0-0/';

CALL pr_import_table('peers', current_setting('path_to_project.var') || 'src/import/peers.csv', ',');
CALL pr_import_table('tasks', current_setting('path_to_project.var') || 'src/import/tasks.csv', ',');
CALL pr_import_table('checks', current_setting('path_to_project.var') || 'src/import/checks.csv', ',');
CALL pr_import_table('p2p', current_setting('path_to_project.var') || 'src/import/p2p.csv', ',');
CALL pr_import_table('verter', current_setting('path_to_project.var') || 'src/import/verter.csv', ',');
CALL pr_import_table('transferredpoints',current_setting('path_to_project.var') || 'src/import/transferredPoints.csv', ',');
CALL pr_import_table('friends', current_setting('path_to_project.var') || 'src/import/friends.csv', ',');
CALL pr_import_table('recommendations', current_setting('path_to_project.var') || 'src/import/recommendations.csv', ',');
CALL pr_import_table('timetracking', current_setting('path_to_project.var') || 'src/import/timeTracking.csv', ',');
CALL pr_import_table('xp', current_setting('path_to_project.var') || 'src/import/xp.csv', ',');

----export
CREATE OR REPLACE PROCEDURE pr_export_table(target_table_name TEXT, path TEXT, delim VARCHAR(1)) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('COPY (SELECT * FROM %1$s) TO %2$L WITH DELIMITER ''%3$s'' CSV HEADER', target_table_name, path, delim);
END;
$$;

CALL pr_export_table('peers', current_setting('path_to_project.var') || 'src/export/peers.csv', ',');
CALL pr_export_table('tasks', current_setting('path_to_project.var') || 'src/export/tasks.csv', ',');
CALL pr_export_table('checks', current_setting('path_to_project.var') || 'src/export/checks.csv', ',');
CALL pr_export_table('p2p', current_setting('path_to_project.var') || 'src/export/p2p.csv', ',');
CALL pr_export_table('verter', current_setting('path_to_project.var') || 'src/export/verter.csv', ',');
CALL pr_export_table('transferredpoints',current_setting('path_to_project.var') || 'src/export/transferredPoints.csv', ',');
CALL pr_export_table('friends', current_setting('path_to_project.var') || 'src/export/friends.csv', ',');
CALL pr_export_table('recommendations', current_setting('path_to_project.var') || 'src/export/recommendations.csv', ',');
CALL pr_export_table('timetracking', current_setting('path_to_project.var') || 'src/export/timeTracking.csv', ',');
CALL pr_export_table('xp', current_setting('path_to_project.var') || 'src/export/xp.csv', ',');

SHOW search_path;
