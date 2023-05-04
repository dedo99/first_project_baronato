#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    fields_csv = line.split(",")

    # prendere campi necessari
    user_id = fields_csv[2]
    helpfulnessNumerator = fields_csv[3]
    helpfulnessDenominator = fields_csv[4]

    try:
        helpfulnessNumerator = int(helpfulnessNumerator)
        helpfulnessDenominator = int(helpfulnessDenominator)
    except ValueError:
        continue

    # salvare nel file chunk per reduce i valori di interesse
    print("%s\t%s" % (user_id, str(helpfulnessNumerator) + '~' + str(helpfulnessDenominator)))