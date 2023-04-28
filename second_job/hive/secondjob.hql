DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS userId2utility;
DROP TABLE IF EXISTS userId2AvgUtility;

CREATE TABLE docs (id INT, productId STRING, userId STRING, profileName STRING, HelpfulnessNumerator INT, HelpfulnessDenominator INT, score INT, time STRING, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


LOAD DATA LOCAL INPATH './Documents/GitHub/first_project_baronato/Reviews.csv' 
	OVERWRITE INTO TABLE docs;


CREATE TABLE userId2utility AS 
    SELECT userId, if(helpfulnessNum = 0 || helpfulnessDen = 0, 0, helpfulnessNum/helpfulnessDen) AS utility
    FROM docs;

-- SELECT userId, utility FROM userId2utility ORDER BY utility DESC;


CREATE TABLE userId2AvgUtility AS
    SELECT userId, AVG(utility) as apprezzamento
    FROM userId2utility
    GROUP BY userId;

SELECT userId, apprezzamento FROM userId2AvgUtility ORDER BY apprezzamento DESC;

DROP TABLE docs;
DROP TABLE userId2utility;
DROP TABLE userId2AvgUtility;