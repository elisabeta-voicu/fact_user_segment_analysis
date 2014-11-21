#!/usr/bin/python

import agg_segment_daily_reducer
from datetime import date
import unittest


class AggSegementDailyReducerTest(unittest.TestCase):
    """Tests for agg_segment_daily_reducer"""

    def reduce(self, source):
        results_gen = agg_segment_daily_reducer.reducer(iter(source), date(2014, 11, 12))
        results = [r for r in results_gen]
        return results


    def test_exec_ok(self):
        """check that it executes"""
        r = self.reduce(['1500868,Value Segment,Loyals,2014,11,8',])
        self.assertEqual(len(r), 4)

    def test_segment_fields_output_ok(self):
        """check that fields are output correctly"""
        r = self.reduce(['1500868,Value Segment,Loyals,2014,11,11',])
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0][0], '2014-11-11')
        self.assertEqual(r[0][1], 'Value Segment')
        self.assertEqual(r[0][2], 'Loyals')
        self.assertEqual(r[0][3], '1')

    def test_ignore_header(self):
        """check that it ignores the header"""
        r = self.reduce(['user_id,segment_type,user_segment,fact_year,fact_month,fact_day',
                         '1500868,Value Segment,Loyals,2014,11,8',])
        self.assertEqual(len(r), 4)

    def test_process_null_transition(self):
        """check that it process a null transition correctly"""
        r = self.reduce(['1500868,Value Segment,Loyals,2014,11,6',
                         '1500868,Value Segment,Loyals,2014,11,8',])
        self.assertEqual(len(r), 6)

    def test_correct_change_of_segment(self):
        """detect when the segment changes"""
        r = self.reduce(['1500868,Value Segment,Casual,2014,11,6',
                         '1500868,Value Segment,Loyals,2014,11,8',])
        self.assertEqual(len([x for x in r if x[2] == 'Casual']), 2)
        self.assertEqual(len([x for x in r if x[2] == 'Loyals']), 4)

    def test_change_of_segment_type(self):
        """detect when the segment type changes"""
        r = self.reduce(['1500868,Value Segment,Casual,2014,11,6',
                         '1500868,Other Segment,Loyals,2014,11,8',])
        self.assertEqual(len([x for x in r if x[2] == 'Casual']), 6)
        self.assertEqual(len([x for x in r if x[2] == 'Loyals']), 4)

    def test_change_of_user_id(self):
        """detect when the user id changes"""
        r = self.reduce(['1500868,Value Segment,Casual,2014,11,6',
                         '9999999,Value Segment,Loyals,2014,11,8',])
        self.assertEqual(len([x for x in r if x[2] == 'Casual']), 6)
        self.assertEqual(len([x for x in r if x[2] == 'Loyals']), 4)

    def test_change_of_everything(self):
        """detect when everything changes"""
        r = self.reduce(['1500868,Value Segment,Casual,2014,11,6',
                         '9999999,Other Segment,Loyals,2014,11,8',])
        self.assertEqual(len([x for x in r if x[2] == 'Casual']), 6)
        self.assertEqual(len([x for x in r if x[2] == 'Loyals']), 4)

    def test_change_of_year(self):
        """handles year boundaries"""
        r = self.reduce(['1500868,Value Segment,Casual,2013,11,6',])
        self.assertEqual(len(r), 371)
        self.assertEqual(len([x for x in r if x[2] != 'Casual']), 0)

    def test_future_dates(self):
        """handles year boundaries"""
        r = self.reduce(['1500868,Value Segment,Casual,2014,12,6',])
        self.assertEqual(len(r), 0)

    def test_users_column(self):
        """counts users correctly"""
        r = self.reduce(['1500868,Value Segment,Casual,2014,11,6',
                         '8888888,Value Segment,Casual,2014,11,6',
                         '9999999,Value Segment,Casual,2014,11,8',])
        # 6 days, 2 x 2 concurrent users, 4 x 3 concurrent users
        self.assertEqual(len(r), 6)
        self.assertEqual(len([x for x in r if x[3] == '2']), 2)
        self.assertEqual(len([x for x in r if x[3] == '3']), 4)

    def test_dates_out_of_order(self):
        """not sorted by date"""
        with self.assertRaises(Exception):
            self.reduce(['1500868,Value Segment,Loyals,2014,11,8',
                         '1500868,Value Segment,Loyals,2014,11,6',])

if __name__ == '__main__':
    unittest.main()

