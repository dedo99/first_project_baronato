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

n1 = 10
n2 = 5
indicies = [1, 6, 7]

# tabella in un unico RDD
table_RDD = spark.sparkContext.textFile(input_filepath).cache()

# RDD con righe divise
rows_RDD = table_RDD.map(f=lambda line: line.split(','))

# RDD con solamente ProductId, Year e Text
productID_year_text_RDD = rows_RDD.map(lambda x: [x[idx] for idx in indicies])

# RDD con (year, productId), 1
year_productID_one_RDD = productID_year_text_RDD.map(f=lambda lista: ((lista[1], lista[0]), 1))

# count by year e productid, group by year e top10 
top10_year_RDD = year_productID_one_RDD.reduceByKey(add) \
            .map(lambda x: (x[0][0], (x[1], x[0][1]))).groupByKey() \
            .map(lambda x: (x[0], nlargest(n1, x[1])))   

# lista con coppie (anno, prodotto) --> perchè il formato top10_year_RDD è diverso
def extract(lista):
    coppie = []
    for year in lista:
        for elem in year[1]:
            coppie.append((year[0], elem[1]))
    return coppie

year_product = extract(top10_year_RDD.collect())

# RDD con anno, prodotto e testo della recensione filtrato per year_product
productID_year_text_filtered_RDD = productID_year_text_RDD.filter(f=lambda x: (x[1], x[0]) in year_product)

# RDD con (anno, prodotto, parola), 1 (per il conteggio)
year_product_word_RDD = productID_year_text_filtered_RDD \
    .flatMap(lambda x: (((x[1], x[0], word), 1) for word in x[2].split(' ')))

# DA AGGIUSTARE!!!
top5_words_RDD = year_product_word_RDD.reduceByKey(add) \
    .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1]))).groupBy(lambda x: (x[0][0], x[0][1])) \
    .map(lambda x: (x[0], nlargest(n2, x[1][1])))

# # stampa del risultato
print(top5_words_RDD.collect())
