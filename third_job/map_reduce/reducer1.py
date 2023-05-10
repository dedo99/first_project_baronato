#!/usr/bin/env python3
"""reducer.py"""

import logging
import sys

# logging.basicConfig(filename='map_reduce_parsed.log', level=logging.INFO)
logging.basicConfig(level=logging.INFO)


# dict con la lista di prodotti piaciuti per ogni utente
user2products = {}

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    current_userid, current_productId  = line.split("\t")

    if current_userid not in user2products:
        user2products[current_userid] = []
    
    user2products[current_userid].append(current_productId)

count = 0
for user in user2products:
    if len(user2products[user]) > 2:
        print('%s\t%s' % (user, " ".join(user2products[user])))
    else:
        count += 1
        logging.info("Number of users removed: %i", count)

logging.shutdown()
# $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar -mapper ./Documenti/BigData/first_project_baronato/third_job/map_reduce/mapper1.py -reducer ./Documenti/BigData/first_project_baronato/third_job/map_reduce/reducer1.py -input /user/pietro/input/Reviews_parsed.csv -output /user/pietro/output/thirdjob_1_iter
# $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar -mapper ./Documenti/BigData/first_project_baronato/third_job/map_reduce/mapper2.py -reducer ./Documenti/BigData/first_project_baronato/third_job/map_reduce/reducer2.py -input /user/pietro/output/thirdjob_1_iter -output /user/pietro/output/thirdjob
