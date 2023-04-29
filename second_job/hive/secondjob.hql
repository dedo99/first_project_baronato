DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS userId2utility;
DROP TABLE IF EXISTS userId2AvgUtility;

CREATE TABLE docs (id INT, ProductId STRING, UserId STRING, HelpfulnessNumerator INT, HelpfulnessDenominator INT, Score INT, Time STRING, Text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH './Documents/GitHub/first_project_baronato/Reviews_parsed.csv' 
	OVERWRITE INTO TABLE docs;


CREATE TABLE userId2utility AS 
    SELECT userId, IF(COALESCE(HelpfulnessNumerator, 0) = 0 OR COALESCE(HelpfulnessDenominator, 0) = 0, 0, HelpfulnessNumerator/HelpfulnessDenominator) AS utility--IF(HelpfulnessNumerator = 0 || HelpfulnessDenominator = 0, 0, HelpfulnessNumerator/HelpfulnessDenominator) AS utility
    FROM docs;


-- SELECT userId, utility FROM userId2utility ORDER BY userId DESC, utility DESC;

CREATE TABLE userId2AvgUtility AS
    SELECT userId, AVG(utility) as apprezzamento
    FROM userId2utility
    GROUP BY userId;

INSERT OVERWRITE LOCAL DIRECTORY './Desktop/output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT userId, apprezzamento FROM userId2AvgUtility ORDER BY apprezzamento DESC;

DROP TABLE docs;
DROP TABLE userId2utility;
DROP TABLE userId2AvgUtility;