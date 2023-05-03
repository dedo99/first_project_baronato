#!/usr/bin/env python3
"""reducer.py"""

import sys

# (k, v) = (year, product_id), count
year_productid2count = {}
year_productid2wordcount = {}

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

    # conteggio
    if (current_year, current_productid) not in year_productid2count:
        year_productid2count[(current_year, current_productid)] = 0
    
    year_productid2count[(current_year, current_productid)] += 1

    if (current_year, current_productid) not in year_productid2wordcount:
        year_productid2wordcount[(current_year, current_productid)] = {}
    for word in words:
        if word not in year_productid2wordcount[(current_year, current_productid)]:
            year_productid2wordcount[(current_year, current_productid)][word] = 0
        year_productid2wordcount[(current_year, current_productid)][word] += 1


# sort by count
sorted_yearProductId2count = {k: v for k, v in sorted(year_productid2count.items(),
                                                      key=lambda item: item[1], reverse=True)}

sorted_yearProductId2wordcount = {}
for i in year_productid2wordcount:
    sorted_yearProductId2wordcount[i] = {k: v for k, v in sorted(year_productid2wordcount[i].items(),
                                                                 key=lambda item: item[1], reverse=True)}
year2listProductId = {}

# get top10 by year
for (year, productid) in sorted_yearProductId2count:
    if year not in year2listProductId:
        year2listProductId[year] = [productid]
    if year in year2listProductId and len(year2listProductId[year])<10:
        year2listProductId[year].append(productid)

year_productId2top5words = {}
for year in year2listProductId:
    for product in year2listProductId[year]:
        year_productId2top5words[(year, product)] = list(sorted_yearProductId2wordcount[(year, product)].keys())[:5]

for year, product in sorted(year_productId2top5words):
    print("%i\t%s\t%s" % (year, product, ", ".join(year_productId2top5words[(year, product)])))
    
