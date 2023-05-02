#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession

# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
#parser.add_argument("--output_path", type=str, help="Output folder path")

# parse parameters
args = parser.parse_args()
input_filepath = args.input_path
#input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Spark second job") \
    .getOrCreate()

indicies = [2, 3, 4]

def get_usefulness(row):
    # SERVE PER L'HEADER
    try:
        num = int(row[1])
        den = int(row[2])
    except ValueError:
        return(row[0], 0.0)
    if (num == 0 or den == 0):
        return (row[0], 0.0)
    return (row[0], float(num/den))

# tabella in un unico RDD
table_RDD = spark.sparkContext.textFile(input_filepath).cache()

# RDD con righe divise
rows_RDD = table_RDD.map(f=lambda line: line.split(','))

# RDD con solamente UserId, HelpfulnessNumerator e Denominator
user_helpfulnessND_RDD = rows_RDD.map(lambda x: [x[idx] for idx in indicies])

# RDD con UserId e HelpfulnessNumerator/Denominator
user_helpfulness_RDD = user_helpfulnessND_RDD.map(get_usefulness)

# RDD con media Helpfulness per utente
avg_by_user =user_helpfulness_RDD \
    .groupByKey() \
    .mapValues(lambda x: sum(x) / len(x)) \
    .collectAsMap()

avg_by_user = {k: v for k, v in sorted(avg_by_user.items(), key=lambda item: item[1], reverse=True)}
print(avg_by_user)