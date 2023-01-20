CREATE OR REPLACE PROCEDURE pr_part3_task12(ref REFCURSOR, count INT DEFAULT 1) LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
WITH all_friends AS (
    SELECT DISTINCT ON (all_friends."Peer1", all_friends."Peer2") *
    FROM ((SELECT "Peer1", "Peer2" FROM friends)
           UNION ALL
          (SELECT "Peer2", "Peer1" FROM friends)) AS all_friends
)
SELECT  p."Nickname" AS "Peer",
       COUNT("Peer2") AS "FriendsCount"
FROM peers p
LEFT JOIN all_friends af ON p."Nickname" = af."Peer1"
GROUP BY p."Nickname"
ORDER BY "FriendsCount" DESC
LIMIT count;
END; $$;

BEGIN;
CALL pr_part3_task12('task12', 6);
FETCH ALL IN task12;
COMMIT;