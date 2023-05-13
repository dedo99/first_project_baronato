#!/usr/bin/env python3
"""reducer.py"""

import logging
import sys

# logging.basicConfig(filename='map_reduce_parsed.log', level=logging.INFO)
logging.basicConfig(level=logging.INFO)


def intersection(lst1, lst2):
    intersect = set(lst1).intersection(lst2)
    return list(intersect)

user2products = {}

for line in sys.stdin:

    #   line = line.strip()

    user, product_string = line.split("\t")

    product_string = product_string.strip()

    product_list = product_string.split(",")

    user2products[user] = product_list


logging.info("Number of users: %i", len(user2products))

#sorted_user2products = dict(sorted(user2products.items(), key=lambda x: len(x[1]), reverse=True))

# utente -> prodotti 
# utenti -> prodotti in comune

# PER OGNI COPPIA DI UTENTI ABBIAMO LE INTERSEZIONI RILEVANTI TRA I GUSTI
common_product2users = {}
users = list(user2products.keys())
logging.info(user2products['A1004703RC79J9'])

num_compararisons = 0

# confronti tra tutte le liste degli utenti
for i in range(0, len(users)-1):
    for j in range(1, len(users)):
        common_elements = intersection(user2products[users[i]], user2products[users[j]])
        num_compararisons += 1
        if len(common_elements) >= 3:
            fs = frozenset(common_elements)
            if fs not in common_product2users:
                common_product2users[fs] = []
            # controllo che questi utenti non siano già presenti 
            # nella lista associata a questa intersezione
            if users[i] not in common_product2users[fs]:
                common_product2users[fs].append(users[i])
            if users[j] not in common_product2users[fs]:
                common_product2users[fs].append(users[j])
        #logging.info()
    #if num_compararisons >= 1000000:
    #    break
        
        
    

# sort invertendo chiave e valore del dizionario
sorted_by_first_user = {k: v for k, v in sorted(common_product2users.items(), key=lambda item: item[1][0])}


for product_list in sorted_by_first_user:
    if len(sorted_by_first_user[product_list]) > 1:
        print("%s\t%s" % (product_list, list(sorted_by_first_user[product_list])))

logging.shutdown()
