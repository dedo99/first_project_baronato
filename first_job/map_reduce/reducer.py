#!/usr/bin/env python3
"""reducer.py"""

import sys

year_productid2count = {}

for line in sys.stdin:

    word2count = {}

    line = line.strip()

    current_year_productid, current_text = line.split('\t')

    current_year, current_productid = current_year_productid.split('~')

    words = current_text.split(' ')

    try:
        current_year = int(current_year)
    except ValueError:
        continue

    if (current_year, current_productid) not in year_productid2count:
        year_productid2count[(current_year, current_productid)] = 0
    
    year_productid2count[(current_year, current_productid)] += 1

    for word in words:
        if word not in word2count:
            word2count[word] = 0
        word2count[word] += 1

sorted_yearProductId2count = {k: v for k, v in sorted(year_productid2count.items(), key=lambda item: item[1], reverse=True)}

year2listProductId = {}

for (year, productid) in sorted_yearProductId2count:
    if year not in year2listProductId:
        year2listProductId[year] = [productid]
    if year in year2listProductId and len(year2listProductId[year])<10:
        year2listProductId[year].append(productid)

for year in year2listProductId:
    print("%i\t[%s]" % (year, ", ".join(year2listProductId[year])))
    
