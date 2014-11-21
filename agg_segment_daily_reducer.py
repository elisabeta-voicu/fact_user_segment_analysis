#!/usr/bin/env python

import sys
from collections import defaultdict
from datetime import date, timedelta


def increment_date_range(counter, segment_type, user_segment, start_date, end_date, increment=timedelta(days=1)):
    while start_date < end_date:
        counter[(segment_type, user_segment, start_date)] += 1
        start_date += increment


def reducer(source, end_date=date.today()):

    (prev_id, prev_type, prev_segment, prev_date) = (None, None, None, None)
    counter = defaultdict(int)

    for line in source:

        # tests are CSV, Hive uses tabs.
        if line.count('\t') == 0:
            line = line.replace(',', '\t')
        line = line.replace('"', '')

        # skip header
        if line.startswith('user_id'):
            continue

        (user_id, segment_type, user_segment, year, month, day) = line.rstrip().split('\t')

        segment_start_date = date(int(year), int(month), int(day))

        if prev_id:
            if (prev_id, prev_type) == (user_id, segment_type):
                # Assume input is distributed by user_id and segment_type, but validate date sort
                if prev_date > segment_start_date:
                    raise Exception('Error: {0} after {1} for user_id {2} - check distribute and sort'.format(segment_start_date, prev_date, prev_id))

                increment_date_range(counter, prev_type, prev_segment, prev_date, segment_start_date)
            else:
                increment_date_range(counter, prev_type, prev_segment, prev_date, end_date)

        (prev_id, prev_type, prev_segment, prev_date) = (user_id, segment_type, user_segment, segment_start_date)

    # End of input - extend the last segment until today
    increment_date_range(counter, prev_type, prev_segment, prev_date, end_date)

    for (segment_type, user_segment, dt) in sorted(counter):
        yield([str(dt), segment_type, user_segment, str(counter[(segment_type, user_segment, dt)])])


if __name__ == "__main__":
    for results in reducer(sys.stdin):
        print '\t'.join(results)

