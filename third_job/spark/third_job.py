from pyspark.sql import SparkSession
import argparse
from heapq import nlargest
from operator import add

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession.builder.appName("THIRD_JOB").getOrCreate()

indicies = [1, 2, 5]

# tabella in un unico RDD
input_RDD = spark.sparkContext.textFile(input_filepath).cache()

# per rimuovere l'header
header = input_RDD.first()

input2_RDD = input_RDD.filter(lambda row: row != header)

# RDD con righe divise
rows_RDD = input2_RDD.map(lambda line: line.split(','))

# RDD con solamente ProductId, userID e Score
productID_userID_score_RDD = rows_RDD.map(lambda x: [x[idx] for idx in indicies])

productID_userID_RDD = productID_userID_score_RDD.filter(lambda list: int(list[2])>=4).map(lambda list: (list[1], list[0]))

userID_productList_RDD = productID_userID_RDD.groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda pair: len(pair[1])>2)

def lists_intersect(l1, l2):
    if len(l1) == len(l2) == 0:  # both empty
        return True
    if len(set(l1).intersection(l2)) > 2:
        return True
    return False

def flat_list_user(pair):
    frozset_product = pair[0]
    list_pair_users = list(pair[1])
    set_user = set()
    for pair_users in list_pair_users:
        set_user.add(pair_users[0])
        set_user.add(pair_users[1])
    return (frozset_product, set_user)


cartesian_RDD = userID_productList_RDD.cartesian(userID_productList_RDD).filter(lambda x: x[0][0] != x[1][0] and lists_intersect(x[0][1], x[1][1])) \
    .map(lambda x: (frozenset(set(x[0][1]).intersection(x[1][1])), (x[0][0], x[1][0]))).groupByKey().map(flat_list_user)


# azione necessaria per l'esecuzione di tutte le trasformazioni precedenti
result = cartesian_RDD.collect()
# stampa del risultato
print(result)