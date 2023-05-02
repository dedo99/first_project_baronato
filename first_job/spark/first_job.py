from pyspark.sql import SparkSession
import argparse
from numpy import mean

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
# parser.add_argument("--output_path", type=str, help="Output file path")

args = parser.parse_args()
input_filepath = args.input_path


spark = SparkSession.builder.appName("FIRST_JOB").getOrCreate()

indicies = [1, 6, 7]

# tabella in un unico RDD
table_RDD = spark.sparkContext.textFile(input_filepath).cache()

# RDD con righe divise
rows_RDD = table_RDD.map(f=lambda line: line.split(','))

# RDD con solamente UserId, HelpfulnessNumerator e Denominator
productID_year_text_RDD = rows_RDD.map(lambda x: [x[idx] for idx in indicies])

year_productID_one_RDD = productID_year_text_RDD.map(f=lambda lista: (lista[1] + "~" + lista[0], 1))

year_productID_count_RDD = year_productID_one_RDD.reduceByKey(lambda v1, v2: v1+v2)\
    .sortBy(lambda key_count: key_count[1], ascending = False)

def split_key(coppia):
    year, productId = coppia[0].split('~')
    count = coppia[1]
    return (year, (productId, count))

year_RDD = year_productID_count_RDD.map(split_key)\
    .sortBy(lambda year_Productcount: year_Productcount[0], ascending = False)

results = rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(add)\
    .map(lambda x: (x[0][0], (x[1], x[0][1]))).groupByKey()\
        .map(lambda x: (x[0], nlargest(n, x[1])))

# azione necessaria per l'esecuzione di tutte le trasformazioni precedenti
# result = year_RDD.collect()
result = year_RDD.top(10, lambda x: x[0])

# stampa del risultato
print(result)
