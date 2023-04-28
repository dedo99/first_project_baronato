DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS year_productId2Text;
DROP TABLE IF EXISTS reviews_for_yearProductId;
DROP TABLE IF EXISTS wordcount_for_reviews;

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

CREATE TABLE wordcount_for_reviews AS
	SELECT year, productId, word, COUNT(*) AS count
	FROM year_productId2Text LATERAL VIEW EXPLODE(SPLIT(text, ' ')) text as word
	GROUP BY year, productId, word
	ORDER BY year, productId, count DESC;

SELECT a.year, a.productId, a.total_reviews, b.word, b.count
FROM
	(SELECT * FROM	
		(SELECT year, productId, total_reviews, ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_reviews DESC) AS rank
		FROM reviews_for_yearProductId) x
	WHERE rank<=10) a
LEFT JOIN
	(SELECT * FROM	
		(SELECT year, productId, word, count, ROW_NUMBER() OVER (PARTITION BY year, productId ORDER BY count DESC) AS rank
		FROM wordcount_for_reviews) x
	WHERE rank<=5) b
ON (a.year == b.year AND a.productId == b.productId)
ORDER BY a.year, a.productId, a.total_reviews, b.count;

DROP TABLE docs;
DROP TABLE year_productId2Text;
DROP TABLE reviews_for_yearProductId;
DROP TABLE wordcount_for_reviews;
