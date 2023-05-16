import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parser
args = parser.parse_args()
input_filepath = args.input_path

# inizializzazione SparkSession
spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()

# custom schema
custom_schema = StructType([
    StructField(name="Id", dataType=IntegerType(), nullable=True),
    StructField(name="ProductId", dataType=StringType(), nullable=True),
    StructField(name="UserId", dataType=StringType(), nullable=True),
    StructField(name="HelpfulnessNumerator", dataType=IntegerType(), nullable=True),
    StructField(name="HelpfulnessDenominator", dataType=IntegerType(), nullable=True),
    StructField(name="Score", dataType=IntegerType(), nullable=True),
    StructField(name="Time", dataType=IntegerType(), nullable=True), 
    StructField(name="Text", dataType=StringType(), nullable=True)])

reviews_DF = spark.read.csv(input_filepath, header = True, schema = custom_schema).cache()

reviews_DF.show()
reviews_DF.printSchema()
reviews_DF.createOrReplaceTempView("reviews")

userid_productid_list_DF = spark.sql("SELECT userId, collect_set(productId) AS prodotti_condivisi "
                                       "FROM( SELECT userId, productId FROM( SELECT userId, productId," 
                                       "score FROM reviews WHERE score >= 4) t GROUP BY userId, productId) t "
                                       "GROUP BY userId HAVING COUNT(*) >= 3")

userid_productid_list_DF.createOrReplaceTempView("userid_productid_list")

userid_product_exploded_DF = spark.sql("SELECT userId, productId FROM userid_productid_list LATERAL VIEW "
                                       "explode(prodotti_condivisi) exploded_productId AS productId;")

userid_product_exploded_DF.createOrReplaceTempView("userid_product_exploded")

pairsIntersection_DF = spark.sql("SELECT t1.userId as user1, t2.userId as user2, collect_set(t1.productId) "
                                 "AS prodotti_condivisi, size(collect_list(t1.productId)) AS size_list "
                                 "FROM userid_product_exploded t1 JOIN userid_product_exploded t2 "
                                 "ON t1.productId = t2.productId AND t1.userId != t2.userId GROUP BY t1.userId, "
                                 "t2.userId ORDER BY t1.userId")

pairsIntersection_DF.createOrReplaceTempView("pairsIntersection")

pairsIntersGreater2_DF = spark.sql("SELECT user1, user2, prodotti_condivisi FROM pairsIntersection "
                                   "WHERE size_list > 2")

pairsIntersGreater2_DF.createOrReplaceTempView("pairsIntersGreater2")

final_DF = spark.sql("SELECT set_users, prodotti_condivisi FROM (SELECT prodotti_condivisi, "
                     "collect_set(user1) AS set_users, size(collect_set(user1)) as num_users "
                     "FROM pairsIntersGreater2 GROUP BY prodotti_condivisi) subview "
                     "WHERE num_users >= 2 ORDER BY set_users[0]")

final_to_write = final_DF.withColumn('set_users', col("set_users").cast("string"))\
    .withColumn('prodotti_condivisi', col("prodotti_condivisi").cast("string"))

final_to_write.show()

final_to_write.coalesce(1).write.format('csv').save("/user/spark_sql/third_job/output", header='true')

# $SPARK_HOME/bin/spark-submit --master local Documents/GitHub/first_project_baronato/third_job/spark_sql/third_job.py --input_path file:///home/andrea/Documents/GitHub/first_project_baronato/Half_Reviews.csv