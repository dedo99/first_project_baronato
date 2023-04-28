#!/usr/bin/env python3
"""reducer.py"""

import sys

userid2listReviews = {}
sortedByHelpfulness = {}

for line in sys.stdin:
    # eliminazione degli spazi bianchi
    line.strip()

    # separazione dei campi del file sulla virgola
    current_userid, currentHelpfulness  = line.split("\t")

    currentHelpfulnessNumerator, currentHelpfulnessDenominator = currentHelpfulness.split("~")

    try:
        currentHelpfulnessNumerator = int(currentHelpfulnessNumerator)
        currentHelpfulnessDenominator = int(currentHelpfulnessDenominator)
    except ValueError:
        continue

    if current_userid not in userid2listReviews:
        userid2listReviews[current_userid] = []

    if currentHelpfulnessNumerator == 0 or currentHelpfulnessDenominator == 0:
        userid2listReviews[current_userid].append(0)
    else:
        userid2listReviews[current_userid].append(float(currentHelpfulnessNumerator/currentHelpfulnessDenominator))

for userid in userid2listReviews:
    sortedByHelpfulness[userid] = sum(userid2listReviews[userid])/len(userid2listReviews[userid])

sortedByHelpfulness = {k: v for k, v in sorted(sortedByHelpfulness.items(), key=lambda item: item[1], reverse=True)}

for userid in sortedByHelpfulness:
    print("%s\t%f" % (userid, sortedByHelpfulness[userid]))
    

