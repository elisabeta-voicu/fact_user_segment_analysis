#!/usr/bin/python

import agg_segment_change_reducer
from datetime import date
import unittest


class AggSegementFlowReducerTest(unittest.TestCase):
    """Tests for agg_segment_change_reducer"""

    def reduce( self, input ):
        results_gen = agg_segment_change_reducer.reducer( iter(input), date(2014,11,12) )
        results     = [ r for r in results_gen ]
        return( results )


    def test_exec_ok(self):
        """check that it executes"""
        r = self.reduce( [ '1500868,Value Segment,Loyals,2014,11,8', 
                           '1500868,Value Segment,Casual,2014,11,10',
                           '9999999,Value Segment,Casual,2014,11,10', ] )
        self.assertEqual( len(r), 2 )

    def test_ignore_null_transitions(self):
        """check that it executes"""
        r = self.reduce( [ '1500868,Value Segment,Loyals,2014,11,6', 
                           '1500868,Value Segment,Casual,2014,11,8',
                           '1500868,Value Segment,Casual,2014,11,10', ] )
        self.assertEqual( len(r), 2 )

    def test_counting_changes(self):
        """check that it counts changes correctly"""
        r = self.reduce( [ '1500868,Value Segment,Loyals,2014,11,4', 
                           '1500868,Value Segment,Casual,2014,11,6',
                           '1500868,Value Segment,Undecided,2014,11,8',
                           '1500868,Value Segment,Superstar,2014,11,10' ] )
        self.assertEqual( len(r), 4 )

    def test_other_segment_types(self):
        """check that it processes all segment types"""
        r = self.reduce( [ '1500868,Value Segment,Loyals,2014,11,4', 
                           '1500868,Value Segment,Changed,2014,11,6', 
                           '1500868,Other Segment,Casual,2014,11,4',
                           '1500868,Other Segment,Changed,2014,11,6' ] )
        self.assertEqual( len(r), 4 )

    def test_output_contents(self):
        """check that the columns are all correct"""
        r = self.reduce( [ '1500868,Value Segment,Loyals,2014,11,4', 
                           '1500868,Value Segment,Changed,2014,11,6' ] )
        self.assertEqual( len(r), 2 )
        self.assertEqual( r[0], ['Value Segment', 'Loyals', 'Changed', '2014-11-06', '1'] )
        self.assertEqual( r[1], ['Value Segment', 'Changed', 'Current Segment', '2014-11-12', '1'] )

    def test_output_contents_multiple(self):
        """check that the columns are all correct when > 1 user"""
        r = self.reduce( [ '1500868,Value Segment,Changed,2014,11,6',
                           '9999999,Value Segment,Changed,2014,11,6' ] )
        self.assertEqual( len(r), 1 )
        self.assertEqual( r[0], ['Value Segment', 'Changed', 'Current Segment', '2014-11-12', '2'] )


if __name__ == '__main__':
    unittest.main()

