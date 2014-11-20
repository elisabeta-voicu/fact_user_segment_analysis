#!/usr/bin/env python

import sys
from collections import defaultdict
from datetime import date


def reducer( input, today=date.today() ):
    
    prev_id = None
    counter = defaultdict( int )

    for line in input:

        # tests are CSV, Hive uses tabs.
        if line.count( '\t' ):
            (user_id, segment_type, user_segment, year, month, day) = line.rstrip().split( '\t' )
        else:
            (user_id, segment_type, user_segment, year, month, day) = line.rstrip().split( ',' )

        event_date = date(int(year),int(month),int(day))

        if prev_id:
            if ( prev_id, prev_type ) == ( user_id, segment_type ):
                if user_segment != prev_segment:
                    counter[(prev_type, prev_segment, user_segment, event_date)] += 1
            else:
                counter[(prev_type, prev_segment, 'Current Segment', today)] += 1

        (prev_id, prev_type, prev_segment) = (user_id, segment_type, user_segment)

    if prev_id:
        counter[(prev_type, prev_segment, 'Current Segment', today)] += 1

    for ( (prev_type, prev_segment, user_segment, event_date), count) in counter.items():
        yield( [ prev_type, prev_segment, user_segment, str(event_date), str(count) ] )

if __name__ == "__main__":
    for results in reducer(sys.stdin):
        print '\t'.join( results )
