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

CREATE TABLE wordcount_for_reviews AS
	SELECT * FROM (
		SELECT year, productId, word, COUNT(*) AS count, ROW_NUMBER() OVER (PARTITION BY year, productId ORDER BY count) AS rank
		FROM year_productId2Text LATERAL VIEW EXPLODE(SPLIT(text, ' ')) text as word
		) x
	GROUP BY year, productId, word
	ORDER BY year, productId, count DESC
	WHERE rank <= 5;


--GET TOP 10 PRODUCTS FOR EACH YEAR
--SELECT * FROM	
--(SELECT year, productId, total_reviews, ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_reviews DESC) AS rank
--FROM reviews_for_yearProductId) x
--WHERE rank<=10;

--GET TOP 5 WORDS FOR EACH YEAR AND PRODUCT
--SELECT * FROM	
--(SELECT year, productId, word, count, ROW_NUMBER() OVER (PARTITION BY year, productId ORDER BY count) AS rank
--FROM wordcount_for_reviews) x
--WHERE rank <= 5;
