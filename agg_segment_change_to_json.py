#!/usr/bin/python

import json
import pyhs2
import itertools
import math


nodes  = {}
edges  = []
weight = 0
colors = { "Loyals":           'green', 
           "Been and Gone":    'red',
           "Up and coming":    'grey',
           "Getting to grips": 'lightblue',
           "Unconvinced":      'yellow',
           "Not streamed":     'pink',
           "Casual":           'orange',
           "Superstars":       'lightgreen' }


with pyhs2.connect( host          = 'hive.athena.we7.local',
                    port          = 10000,
                    authMechanism = "KERBEROS",
                    user          = '',
                    password      = '',
                    database      = 'davec_sandbox'
                  ) as conn:

    with conn.cursor() as cur:

        cur.execute('''select segment_type,
                              before_segment,
                              after_segment,
                              sum(users)
                       from davec_sandbox.agg_segment_change
                       group by segment_type,
                                before_segment,
                                after_segment
                    ''' )

        for segement_type, before_segment, after_segment, sum_users in cur.fetchall():

            #if segement_type != 'Life-stage':
            if segement_type != 'Value Segment':
                continue

            color = 'lightgrey'
            if before_segment in colors:
                color = colors[before_segment]

            if not before_segment in nodes:
                nodes[before_segment] = { 'title':  before_segment,
                                          'id':     before_segment,
                                          'color':  color,
                                          'weight': 0,
                                         }

            nodes[before_segment]['weight'] += int(sum_users)
            weight += int(sum_users)

            edges.append( { 'from':      before_segment,
                            'to':        after_segment,
                            'color':     color,
                            'label':     sum_users,
                            'sum_users': int(sum_users),
                          }
                        )

#
# Clean up and enhance the nodes and edges
#

for node in nodes.values():
    node['label'] = node['title'] + '\n' + \
                    str(node['weight']) + ' events\n' + \
                    str( node['weight'] * 100 / weight ) + '% of customers'

for edge in edges:
    edge['value'] = 100 * edge['sum_users'] / nodes[edge['from']]['weight'] 
    edge['label'] = str(edge['value']) + '%'

    if edge['value'] < 6:
        edge['from'] = 'discarded'      # Dummy node - edge is not displayed

with open( 'agg_segment_change.json', 'wb' ) as out:
    out.write( 'var data=' )
    json.dump( { 'edges': edges, 'nodes': nodes.values() }, out, indent=4 )

