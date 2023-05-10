#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    fields_csv = line.split(",")

    # prendere campi necessari
    product_id = fields_csv[1]
    user_id = fields_csv[2]
    score = fields_csv[5]

    try:
        score = int(score)
    except ValueError:
        continue

    # Passo al reduce solamente gli elementi con score >= 4
    if score >= 4:
        print("%s\t%s" % (user_id, product_id))
    else:
        continue
