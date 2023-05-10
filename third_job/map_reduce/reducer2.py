#!/usr/bin/env python3
"""reducer.py"""

import logging
import sys

# logging.basicConfig(filename='map_reduce_parsed.log', level=logging.INFO)
logging.basicConfig(level=logging.INFO)


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

user2products = {}

for line in sys.stdin:

    #line = line.strip()

    user, product_string = line.split("\t")

    product_list = product_string.split(" ")

    user2products[user] = product_list


logging.info("Number of users: %i", len(user2products))

# utente -> prodotti 
# utenti -> prodotti in comune

# PER OGNI COPPIA DI UTENTI ABBIAMO LE INTERSEZIONI RILEVANTI TRA I GUSTI
common_product2users = {}
users = list(user2products.keys())


num_compararisons = 0

# confronti tra tutte le liste degli utenti
for i in range(0, len(users)-1):
    for j in range(1, len(users)):
        common_elements = intersection(user2products[users[i]], user2products[users[j]])
        if len(common_elements) < 3:
            continue
        fs = frozenset(common_elements)
        if fs not in common_product2users:
            common_product2users[fs] = []
        # controllo che questi utenti non siano già presenti 
        # nella lista associata a questa intersezione
        if users[i] not in common_product2users[fs]:
            common_product2users[fs].append(users[i])
        if users[j] not in common_product2users[fs]:
            common_product2users[fs].append(users[j])
        num_compararisons += 1
        logging.info("Number of comparisons: %i", num_compararisons)
    

# sort invertendo chiave e valore del dizionario
sorted_users2common_product = {v: k for k, v in sorted(common_product2users.items(),
                                                      key=lambda item: item[1][0], reverse=True)}


for lista_user in sorted_users2common_product:
    print("%s\t%s" % (", ".join(lista_user), ", ".join(list(sorted_users2common_product[lista_user]))))

logging.shutdown()
