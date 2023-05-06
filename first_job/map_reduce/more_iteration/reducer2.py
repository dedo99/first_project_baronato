#!/usr/bin/env python3
"""reducer.py"""

import sys

# (k, v) = (year, product_id), count

year_productId2top5words = {}

for line in sys.stdin:

    line = line.strip()

    try:
        current_year_productid, current_text = line.split('\t')
    except ValueError:
        continue

    current_year, current_productid = current_year_productid.split('~')

    words = current_text.split(' ')

    try:
        current_year = int(current_year)
    except ValueError:
        continue

    word2count = {}

    for word in words:
        if word not in word2count:
            word2count[word] = 0
        word2count[word] += 1
    
    # sort by count
    sorted_word2count = {k: v for k, v in sorted(word2count.items(),
                                                        key=lambda item: item[1], reverse=True)}
    
    year_productId2top5words[(current_year, current_productid)] = (list(sorted_word2count.keys())[:5],
                                                                   list(map(str, list((sorted_word2count.values()))))[:5])


for (year, productId) in year_productId2top5words:
    print("%i\t%s\t%s\t%s" % (year, productId, ", ".join(year_productId2top5words[(year, productId)][0]),
                                               ", ".join(year_productId2top5words[(year, productId)][1])))
