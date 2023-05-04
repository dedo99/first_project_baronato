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

userId_utility_DF = reviews_DF.withColumn("Utility", when(col("HelpfulnessNumerator") == 0, 0).when(col("HelpfulnessDenominator") == 0, 0).otherwise(col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))).select("UserId", "Utility")

userId_appreciation_DF = userId_utility_DF.groupBy("UserId").mean("Utility").withColumnRenamed("avg(Utility)","Appreciation")

sorted_userId_appreciation_DF = userId_appreciation_DF.sort("Appreciation", ascending = False)

sorted_userId_appreciation_DF.show()

# salvataggio output su file in hdfs
sorted_userId_appreciation_DF.coalesce(1).write.format('csv').save("/user/spark_sql/second_job/output", header='true')


# $SPARK_HOME/bin/spark-submit --master local Documents/GitHub/first_project_baronato/second_job/spark_sql/second_job.py --input_path file:///home/andrea/Documents/GitHub/first_project_baronato/Reviews_parsed.csv

# TEMPI DI ESECUZIONE SU DATASET ORIGINALE
# real	0m21,731s
# user	1m8,225s
# sys	0m1,788s