import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, collect_list, collect_set
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

year_productId_DF = reviews_DF.groupBy("Time", "ProductId").count().sort("count", ascending=False)
year_productId_DF.show()

list_year = reviews_DF.select(collect_set("Time")).first()[0]

# creare il DataFrame vuoto
empty_schema = StructType([StructField("Time", IntegerType(), True), StructField("ProductId", StringType(), True), StructField("count", IntegerType(), True)])
top_10_year_productId_DF = spark.createDataFrame([], empty_schema)

for year in list_year:
    temp = year_productId_DF.select("Time", "ProductId", "count").where(col("Time") == year).limit(10)
    top_10_year_productId_DF = top_10_year_productId_DF.union(temp)

top_10_year_productId_DF.show()


year_productId_concatenatedText_DF = reviews_DF.groupBy("Time", "ProductId").agg(concat_ws(" ", collect_list("Text")).alias("concatenated_text"))
year_productId_concatenatedText_DF.show()
year_productId_concatenatedText_DF.createOrReplaceTempView("year_productId_concatenatedText")


year_productId_countWords_SQL = spark.sql("SELECT Time, ProductId, word, COUNT(*) as count_word FROM (SELECT Time, ProductId, EXPLODE(SPLIT(concatenated_text, ' ')) as word FROM year_productId_concatenatedText) GROUP BY Time, ProductId, word")
year_productId_countWords_SQL.show()
year_productId_countWords_SQL.createOrReplaceTempView("year_productId_countWords_SQL")


top_5_year_productId_countWords_SQL = spark.sql("SELECT Time, ProductId, word, count_word FROM (SELECT Time, ProductId, word, count_word, ROW_NUMBER() OVER (PARTITION BY Time, ProductId ORDER BY count_word DESC) AS rank FROM year_productId_countWords_SQL) WHERE rank <= 5")
top_5_year_productId_countWords_SQL.show()

top10_year_productId_top5_words_DF = top_10_year_productId_DF.join(top_5_year_productId_countWords_SQL, ["Time", "ProductId"], "inner").sort("Time", "ProductId","count", ascending=True)
top10_year_productId_top5_words_DF.show()


# $SPARK_HOME/bin/spark-submit --master local Documents/GitHub/first_project_baronato/first_job/spark_sql/first_job.py --input_path file:///home/andrea/Documents/GitHub/first_project_baronato/Reviews_parsed.csv