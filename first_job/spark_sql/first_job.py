import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, collect_list
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

year_productId_DF = reviews_DF.groupBy("Time", "ProductId").count().sort("count", ascending = False).show()

year_productId_concatenatedText_DF = reviews_DF.groupBy("Time", "ProductId").agg(concat_ws(" ", collect_list("Text")).alias("concatenated_text")).show()

year_DF = list(reviews_DF.select("Time").distinct())

print(year_DF)


# $SPARK_HOME/bin/spark-submit --master local Documents/GitHub/first_project_baronato/first_job/spark_sql/first_job.py --input_path file:///home/andrea/Documents/GitHub/first_project_baronato/Reviews_parsed.csv