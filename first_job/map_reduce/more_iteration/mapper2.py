#!/usr/bin/env python3
"""mapper.py"""

import sys
import datetime

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    year_productID, text = line.split("\t")

    # salvare nel file chunk per reduce i valori di interesse
    print("%s\t%s" % (year_productID, text))

    # (k, v) = (int_year~product_id, text)
    
