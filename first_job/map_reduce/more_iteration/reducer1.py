#!/usr/bin/env python3
"""reducer.py"""

import sys

# (k, v) = (year, product_id), count
year_productid2words_count = {}

for line in sys.stdin:

    line = line.strip()
    try:
        current_year_productid, current_text = line.split('\t')
    except ValueError:
        continue

    current_year, current_productid = current_year_productid.split('~')

    try:
        current_year = int(current_year)
    except ValueError:
        continue

    # conteggio
    if (current_year, current_productid) not in year_productid2words_count:
        year_productid2words_count[(current_year, current_productid)] = [current_text, 1]
    else:
        year_productid2words_count[(current_year, current_productid)][0] = year_productid2words_count[(current_year, current_productid)][0] + ' ' + current_text
        year_productid2words_count[(current_year, current_productid)][1] += 1

# sort by count
sorted_yearProductId2count = {k: v for k, v in sorted(year_productid2words_count.items(),
                                                      key=lambda item: item[1][1], reverse=True)}


year2listProductId = {}

# get top10 by year
for (year, productid) in sorted_yearProductId2count:
    if year not in year2listProductId:
        year2listProductId[year] = [productid]
        print("%s\t%s" % (str(year) + '~' + productid, sorted_yearProductId2count[(year, productid)][0]))
    if year in year2listProductId and len(year2listProductId[year])<10:
        year2listProductId[year].append(productid)
        print("%s\t%s" % (str(year) + '~' + productid, sorted_yearProductId2count[(year, productid)][0]))