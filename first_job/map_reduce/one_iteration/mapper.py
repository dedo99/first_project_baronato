#!/usr/bin/env python3
"""mapper.py"""

import sys
import datetime

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    fields_csv = line.split(",")

    year = fields_csv[6]
    try:
        int_year = int(year)
    except ValueError:
        continue

    product_id = fields_csv[1]
    text = fields_csv[7]

    # salvare nel file chunk per reduce i valori di interesse
    print("%s\t%s" % (str(int_year) + '~' + product_id, text))

    # (k, v) = (int_year~product_id, text)
    
