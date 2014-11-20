
Daily batches:

    agg_segment_daily_batch.py
    - requires agg_segment_daily_reducer.py
    - populates davec_sandbox.agg_segment_daily

    agg_segment_change_batch.py
    - requires agg_segment_change_reducer.py
    - populates davec_sandbox.agg_segment_change

Presentation

    agg_segment_change_to_json.py
    - creates agg_segment_change.json
    - used by agg_segment_change.html

Tests

    # python -m unittest discover  

    Will find:
    - test_agg_segment_daily_batch.py
    - test_agg_segment_change_reducer.py
    - test_agg_segment_daily_reducer.py


