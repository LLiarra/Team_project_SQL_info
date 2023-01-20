CREATE OR REPLACE PROCEDURE pr_part3_task10(ref refcursor) AS $$
BEGIN
    OPEN ref FOR
WITH all_friends AS (
    SELECT DISTINCT ON (all_friends."Peer1", all_friends."Peer2") *
    FROM ((SELECT "Peer1", "Peer2" FROM friends)
          UNION ALL
          (SELECT "Peer2", "Peer1" FROM friends)) AS all_friends
), count_rec AS (
SELECT af."Peer1",
       r."RecommendedPeer",
       COUNT(r."RecommendedPeer") AS count
FROM all_friends af
LEFT JOIN recommendations r ON af."Peer2" = r."Peer"
WHERE af."Peer1" <> r."RecommendedPeer"
GROUP BY af."Peer1", r."RecommendedPeer"
ORDER BY "Peer1"
)
SELECT DISTINCT ON (cr."Peer1") cr."Peer1" AS "Peer",
                    cr."RecommendedPeer"
FROM count_rec cr
WHERE cr.count = (SELECT MAX(cr2.count) FROM count_rec cr2 WHERE cr2."Peer1" = cr."Peer1");
END; $$ LANGUAGE plpgsql;

BEGIN;
CALL pr_part3_task10('task10');
FETCH ALL IN task10;
COMMIT;
