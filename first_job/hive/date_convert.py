#!/usr/bin/env python3
import sys
from datetime import datetime

OUTPUT_FORMAT = '%Y-%m-%d %H:%M:%S'

# read input from stdin
for line in sys.stdin:

    try:
        # remove whitespaces and trailing characters
        line = line.strip()

        # parse name and unix date using TAB as a separator
        unix_date, productId, text = line.split("\t")

        # try to convert the unix date to an integer
        try:
            unix_date = int(unix_date)
        except ValueError:
            continue

        # build a datetime object from the unix time
        datetime_obj = datetime.utcfromtimestamp(unix_date)

        # get the output date string
        out_date_str = datetime_obj.strftime(OUTPUT_FORMAT)
        year = out_date_str.split('-')[0]

        # print output items to stdout, using TAB as a separator
        print("\t".join([year, productId, text]))

    except:
        import sys
        print(sys.exc_info())
