#!/usr/bin/env python

import math

import duckdb
import polars as pl
import tqdm
from google.cloud import bigquery
from datetime import timedelta

client = bigquery.Client()
db = duckdb.connect("data.db")

source_table = "`ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`"
source_filters = """
(
  dataSource = 'BNR' -- BaneNOR
  OR dataSource = 'ATB' -- Trøndelag
  OR dataSource = 'FLT' -- Flytoget
)
"""

rs = client.query(f"""
    select min(recordedAtTime) as min_record, 
           max(recordedAtTime) as max_record 
    from {source_table} where {source_filters}
""").result()
row = list(rs)[0]

start, end = row.get("min_record"), row.get("max_record")
page_delta = timedelta(days=30)
page_count = math.ceil(((end - start) / page_delta))
page_boundaries = [
    start + n * page_delta
    for n in range(page_count + 1) # include last page
]

created = False
for page_start, page_end in tqdm.tqdm(zip(page_boundaries, page_boundaries[1:])):
    job = client.query(
        f"""
SELECT 
  recordedAtTime,
  lineRef,
  directionRef,
  operatingDate,
  vehicleMode,
  extraJourney,
  journeyCancellation,
  stopPointRef,
  sequenceNr,
  stopPointName,
  originName,
  destinationName,
  extraCall,
  stopCancellation,
  estimated,
  aimedArrivalTime,
  arrivalTime,
  aimedDepartureTime,
  departureTime
FROM {source_table}
WHERE (recordedAtTime >= '{page_start.isoformat()}' AND recordedAtTime < '{page_end.isoformat()}')
  AND {source_filters}
"""
    )
    tab = job.to_arrow()
    db.register("data_batch", tab)
    if created:
        db.execute("insert into arrivals select * from data_batch")
    else:
        db.execute("create table arrivals as select * from data_batch")
        created = True
    db.unregister("data_batch")
