DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS utenti_gusti_affini;
DROP TABLE IF EXISTS utenti_gusti_affini_exploded;
DROP TABLE IF EXISTS user_pairs2intersection;
DROP TABLE IF EXISTS user_pairs2intersection_selectionGreaterThan2;


CREATE TABLE docs (id INT, productId STRING, userId STRING, helpfulnessNum INT, helpfulnessDen INT, score INT, time STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH './Documenti/BigData/first_project_baronato/Quadruple_Reviews.csv' 
--LOAD DATA LOCAL INPATH './Documents/GitHub/first_project_baronato/Double_Reviews.csv' 
	OVERWRITE INTO TABLE docs;


-- Creazione di una tabella temporanea contenente gli utenti con score >= 4 per almeno 3 prodotti
CREATE TABLE utenti_gusti_affini AS
    SELECT userId, collect_set(productId) AS prodotti_condivisi
    FROM(   SELECT userId, productId
            FROM(   SELECT userId, productId, score
                    FROM docs
                    WHERE score >= 4) t
            GROUP BY userId, productId) t
    GROUP BY userId
    HAVING COUNT(*) >= 3;


CREATE TABLE utenti_gusti_affini_exploded AS
    SELECT userId, productId
    FROM utenti_gusti_affini LATERAL VIEW explode(prodotti_condivisi) exploded_productId AS productId;


-- Join tra la tabella temporanea utenti_gusti_affini per ottenere gli utenti che condividono prodotti
-- e l'elenco dei prodotti condivisi per ciascun gruppo
CREATE TABLE user_pairs2intersection AS
    SELECT t1.userId as user1, t2.userId as user2, collect_set(t1.productId) AS prodotti_condivisi, size(collect_list(t1.productId)) AS size_list
    FROM utenti_gusti_affini_exploded t1
    JOIN utenti_gusti_affini_exploded t2 ON t1.productId = t2.productId AND t1.userId != t2.userId
    GROUP BY t1.userId, t2.userId 
    ORDER BY t1.userId;


CREATE TABLE user_pairs2intersection_selectionGreaterThan2 AS
    SELECT user1, user2, prodotti_condivisi
    FROM user_pairs2intersection
    WHERE size_list > 2;


INSERT OVERWRITE DIRECTORY '/user/pietro/output/HIVE/third_job'
--INSERT OVERWRITE DIRECTORY '/user/andrea/output/third_job/hive_parsed'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
STORED AS TEXTFILE
SELECT set_users, prodotti_condivisi
FROM(   SELECT prodotti_condivisi, collect_set(user1) AS set_users, size(collect_set(user1)) as num_users
        FROM user_pairs2intersection_selectionGreaterThan2
        GROUP BY prodotti_condivisi) subview
WHERE num_users >= 2
ORDER BY set_users[0];


-- SELECT *
-- FROM user_pairs2intersection_selectionGreaterThan2;


DROP TABLE docs;
DROP TABLE utenti_gusti_affini;
DROP TABLE utenti_gusti_affini_exploded;
DROP TABLE user_pairs2intersection;
DROP TABLE user_pairs2intersection_selectionGreaterThan2;