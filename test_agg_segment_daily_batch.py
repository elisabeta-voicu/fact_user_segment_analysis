#!/usr/bin/python


import mock
import unittest
import agg_segment_daily_batch


class AggSegmentDailyBatchTest(unittest.TestCase):
    """Tests for agg_segment_daily_batch"""
    
    @mock.patch('agg_segment_daily_batch.pyhs2.connect')
    def test_aggregate( self, mock_pyhs2_connect ):

        agg_segment_daily_batch.aggregate()
        mock_pyhs2_connect.assert_called(1)


if __name__ == '__main__':
    unittest.main()

