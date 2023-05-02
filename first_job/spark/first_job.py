from pyspark.sql import SparkSession
import argparse
from heapq import nlargest
from operator import add

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
#parser.add_argument("--output_path", type=str, help="Output file path")

args = parser.parse_args()
# input_filepath, output_filepath = args.input_path, args.output_path
input_filepath = args.input_path


spark = SparkSession.builder.appName("FIRST_JOB").getOrCreate()

n = 10
indicies = [1, 6, 7]

# tabella in un unico RDD
table_RDD = spark.sparkContext.textFile(input_filepath).cache()

# RDD con righe divise
rows_RDD = table_RDD.map(f=lambda line: line.split(','))

# RDD con solamente ProductId, Year e Text
productID_year_text_RDD = rows_RDD.map(lambda x: [x[idx] for idx in indicies])

# RDD con (year, productId), text
year_productID_one_RDD = productID_year_text_RDD.map(f=lambda lista: ((lista[1], lista[0]), 1))

# count by year e productid, group by year e top10 
top10_year_RDD = year_productID_one_RDD.reduceByKey(add) \
            .map(lambda x: (x[0][0], (x[1], x[0][1]))).groupByKey() \
            .map(lambda x: (x[0], nlargest(n, x[1])))

result = top10_year_RDD.collect()

# # stampa del risultato
print(result)
