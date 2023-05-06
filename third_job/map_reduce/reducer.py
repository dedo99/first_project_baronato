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
couple_users2intersection = {}

# confronti tra tutte le liste degli utenti
for user1 in user2products:
    for user2 in user2products: 
        if user1 == user2:
            continue
        intersect = intersection(user2products[user1], user2products[user2])
        if len(intersect) < 3:
            continue
        if (user1, user2) not in couple_users2intersection or (user2, user1) not in couple_users2intersection :
            couple_users2intersection[(user1, user2)] = intersection

for user1, user2 in couple_users2intersection:
    print('(%s, \t%s)\t%s' % (user1, user2, '[' + ', '.join(couple_users2intersection[(user1, user2)]) + ']'))
