#!/usr/bin/env python3
"""reducer.py"""

import sys

userid2listReviews = {}
sortedByHelpfulness = {}

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    current_userid, currentUtility  = line.split("\t")

    try:
        currentUtility = float(currentUtility)
    except ValueError:
        continue

    if current_userid not in userid2listReviews:
        userid2listReviews[current_userid] = []

    userid2listReviews[current_userid].append(currentUtility)

for userid in userid2listReviews:
    sortedByHelpfulness[userid] = sum(userid2listReviews[userid])/len(userid2listReviews[userid])

sortedByHelpfulness = {k: v for k, v in sorted(sortedByHelpfulness.items(), key=lambda item: item[1], reverse=True)}

for userid in sortedByHelpfulness:
    print("%s\t%f" % (userid, sortedByHelpfulness[userid]))
    

