#!/usr/bin/python

import pyhs2

def aggregate():
    with pyhs2.connect( host          = 'hive.athena.we7.local',
                        port          = 10000,
                        authMechanism = "KERBEROS",
                        user          = '',
                        password      = '',
                        database      = 'davec_sandbox'
                      ) as conn:

        with conn.cursor() as cur:

            cur.execute('''add file hdfs://athena/user/davec/agg_segment_change_reducer.py''' )

            ## Hive chooses only one reducer by default (28 minutes). Force 15 (2.5 mins).
            #cur.execute('''set mapred.reduce.tasks=15''' )
            cur.execute('''create table if not exists davec_sandbox.agg_segment_change (
                              segment_type     string,
                              before_segment   string,
                              after_segment    string,
                              change_date      string,
                              users            int
                            )
                        ''' )
            cur.execute('''

                            insert overwrite table davec_sandbox.agg_segment_change
                            select segment_type,
                                   before_segment,
                                   after_segment,
                                   change_date,
                                   cast(users as int)

                              from (
                                   select transform(*)
                                    using 'python agg_segment_change_reducer.py'
                                       as ( segment_type,
                                            before_segment,
                                            after_segment,
                                            change_date,
                                            users
                                          )
                                   from (
                                          select user_id,
                                                 segment_type,
                                                 user_segment,
                                                 fact_year,
                                                 fact_month,
                                                 fact_day
                                          from events_super_mart.fact_user_segment
                                          --test--where segment_type = 'Value Segment'
                                            --test--and fact_year = 2014
                                            --test--and fact_month = 11
                                            --test--and fact_day = 12
                                          distribute by user_id, segment_type
                                          sort by user_id,
                                                  segment_type,
                                                  fact_year,
                                                  cast(fact_month as int),
                                                  cast(fact_day as int)
                                   ) user_segments
                               ) segment_changes

                        ''' )

if __name__ == "__main__":
    aggregate()

