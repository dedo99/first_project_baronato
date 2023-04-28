DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS year_productId2Text;
DROP TABLE IF EXISTS reviews_for_yearProductId;

CREATE TABLE docs (id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNum INT, helpfulnessDen INT, score INT, time STRING, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/pietro/Documenti/BigData/Dataset_Progetto1/Reviews.csv' 
	OVERWRITE INTO TABLE docs;

ADD FILE /home/pietro/Documenti/BigData/first_project_baronato/first_job/hive/date_convert.py;

CREATE TABLE year_productId2Text AS
	SELECT TRANSFORM(docs.time, docs.productId, docs.text) 
	USING 'python3 /home/pietro/Documenti/BigData/first_project_baronato/first_job/hive/date_convert.py'
	AS year, productId, text
	FROM docs;


CREATE TABLE reviews_for_yearProductId AS
	SELECT year, productId, count(*) as total_reviews
	FROM year_productId2Text
	GROUP BY year, productId
	ORDER BY year, total_reviews DESC;

SELECT * FROM	
(SELECT year, productId, total_reviews, ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_reviews) AS rank
FROM reviews_for_yearProductId) x
WHERE rank<=10;

DROP TABLE docs;
DROP TABLE year_productId2Text;
DROP TABLE reviews_for_yearProductId;
