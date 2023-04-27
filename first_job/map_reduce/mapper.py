#!/usr/bin/env python3
"""mapper.py"""

import sys
import datetime

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    fields_csv = line.split(",")

    # converti il tempo Unix in una data
    try:
        date_time = datetime.datetime.fromtimestamp(int(fields_csv[7]))
    except ValueError:
        continue

    # stampa la data formattata ed estre l'anno
    date = date_time.strftime('%Y-%m-%d %H:%M:%S')
    year = date.split('-')[0]
    try:
        int_year = int(year)
    except ValueError:
        continue

    product_id = fields_csv[1]
    text = fields_csv[9]

    # salvare nel file chunk per reduce i valori di interesse
    print("%s\t%s" % (str(int_year) + '~' + product_id, text))

    # (k, v) = (int_year~product_id, text)