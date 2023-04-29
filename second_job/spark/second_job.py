from pyspark.sql import SparkSession
import argparse
from numpy import mean

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


spark = SparkSession.builder.appName("SECOND_JOB").getOrCreate()

# restituisce un RDD dopo avere caricato il file dato in input
input_RDD = spark.sparkContext.textFile(input_filepath)

def userId_utility(coppia):
    helpfulness = coppia[1][0].split('~')
    try:
        helpfulnessNum = int(helpfulness[0])
        helpfulnessDen = int(helpfulness[1])
        if helpfulnessNum == 0 or helpfulnessDen == 0:
            utility = 0
        else:
            utility = float(helpfulnessNum)/float(helpfulnessDen) 
        return (coppia[0], (float(utility), 1))
    except ValueError:
        return (coppia[0], (float(0), 1))


# splitta i record in campi sulla base della virgola 
rows = input_RDD.map(lambda line: line.split(","))

# viene creata una coppia dove la chiave è userid e il valore è una coppia dove:
# --il primo elemento è helpfulnessNum~helpfulnessDen
# --il secondo è 1 (utilizzato per il conteggio)
userid_helpfulness_RDD = rows.map(lambda row: (row[2], (row[3] + '~' + row[4], 1)))

# viene effettuato il calcolo dell'utility come rapporto tra helpfulnessNum e helpfulnessDen
userid_utility_RDD = userid_helpfulness_RDD.map(userId_utility)

# viene effettuata la somma delle utility e il conteggio sulla coppie con la stessa chiave
sum_utility_count_RDD = userid_utility_RDD.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))

# viene calcolata l'apprezzamento come media delle utility (rapporto tra la somma di tutte le utility della stessa chiave e il loro conteggio)
appreciation_RDD = sum_utility_count_RDD.map(lambda el: (el[0], float(el[1][0]/el[1][1])))

# ordinamento della struttura creata tramite l'instruzione precedente sulla base dell'apprezzamento
sorted_appreciation_RDD = appreciation_RDD.sortBy(lambda userid_appreciation: userid_appreciation[1], ascending = False)

# azione necessaria per l'esecuzione di tutte le trasformazioni precedenti
result = sorted_appreciation_RDD.collect()

# stampa del risultato
print(result)

# salvataggio del risutato su file
sorted_appreciation_RDD.saveAsTextFile(output_filepath)