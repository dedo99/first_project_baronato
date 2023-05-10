#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    user, product_string = line.split("\t")

    print("%s\t%s" % (user, product_string))
