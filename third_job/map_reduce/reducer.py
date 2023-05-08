#!/usr/bin/env python3
"""reducer.py"""

import sys

def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

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

# utente -> prodotti 
# utenti -> prodotti in comune

# PER OGNI COPPIA DI UTENTI ABBIAMO LE INTERSEZIONI RILEVANTI TRA I GUSTI
common_product2users = {}
users = list(user2products.keys())


# confronti tra tutte le liste degli utenti
for i in range(0, len(users)-1):
    for j in range(1, len(users)):
        common_elements = intersection(user2products[users[i]], user2products[users[j]])
        if len(common_elements) < 3:
            continue
        fs = frozenset(common_elements)
        if fs not in common_product2users:
            common_product2users[fs] = []
        common_product2users[fs].append([users[i], users[j]])
    
for lista in common_product2users:
    print("%s\t%s" % (lista, common_product2users[lista]))