#!/usr/bin/env python
"""
Create the `legs` table in a DuckDB instance containing the `arrivals` table.

This joins each `arrival` with the `arrival` at the previous stop for each
journey and augments it with geolocation information, such as distance and
a bound for speed.
"""

import argparse
from argparse import ArgumentParser

import numpy as np
import pandas as pd
import duckdb
import psutil

parser = ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument(
    "duckdb_file", help="Path to database file containing `arrivals` and `quays` tables"
)
parser.add_argument(
    "--max_cpus",
    default=psutil.cpu_count(logical=False) - 1,
    help="Limit the number of cores used",
    type=int,
)
parser.add_argument(
    "--memory-limit-gb",
    default=int(0.8 * psutil.virtual_memory().available / 1e9),
    help="GB of memory to allow DuckDB to use (default 80% of available)",
    type=int,
)

opts = parser.parse_args()
db = duckdb.connect(opts.duckdb_file)
db.execute(f"set threads = {opts.max_cpus}")

join_size = db.sql("select count(*) from arrivals").fetchone()[0]
print(f"Execute {join_size} x {join_size} join.")

db.execute("""
create or replace table legs as select
  now.lineRef as lineRef,
  previous.stopPointName as previous_stop_name,
  previous.stopPointRef as previous_stop,
  now.stopPointName as stop_name,
  now.stopPointRef as stop,
  now.datedServiceJourneyId as datedServiceJourneyId,
  now.arrivalTime as time,
  now.aimedArrivalTime as planned_time,
  extract(epoch from now.aimedArrivalTime - previous.aimedArrivalTime) as planned_duration,
  extract(epoch from now.arrivalTime - previous.arrivalTime) as actual_duration,
  actual_duration - planned_duration as deviation,
  extract(epoch from time - planned_time) as delay
from arrivals previous join arrivals now on
  previous.datedServiceJourneyId = now.datedServiceJourneyId
  and previous.sequenceNr + 1 = now.sequenceNr
where
  abs(deviation) < 1800 and abs(delay) < 3600
  and not (now.journeyCancellation or now.stopCancellation or now.extraCall);
""")

df: pd.DataFrame = db.sql("""
with unique_legs as (
   select distinct previous_stop, stop from legs
)
select
    ul.previous_stop,
    prev_quay.location_longitude as prev_lon,
    prev_quay.location_latitude as prev_lat,
    ul.stop,
    cur_quay.location_longitude as now_lon,
    cur_quay.location_latitude as now_lat
from unique_legs ul
    join quays prev_quay on ul.previous_stop = prev_quay.id
    join quays cur_quay on ul.stop = cur_quay.id
""").df()

print(f"Calculating haversine distances for {len(df)} unique previous -> next pairs.")

# This is hopefully a correct haversine distance calculation, giving the distance
# between two points on the surface of a perfect sphere with Earths radius. I think this
# would look even more gnarly in SQL. We're using fast vectorized numpy operations here,
# so this is completely dominated by finding the distinct previous/next stops anyway
earth_radius_km = 6378.0
lon1 = np.radians(df.prev_lon)
lat1 = np.radians(df.prev_lat)
lat2 = np.radians(df.now_lat)
lon2 = np.radians(df.now_lon)

delta_lon = lon2 - lon1
delta_lat = lat2 - lat1

hav_a = np.sin(delta_lat / 2) ** 2
hav_b = np.sin(delta_lon / 2) ** 2
distance_km = (
    earth_radius_km
    * 2
    * np.arcsin(np.sqrt(hav_a + np.cos(lat1) * np.cos(lat2) * hav_b))
)
leg_distances = df.assign(air_distance_km=distance_km)
db.execute("create or replace table distances as select * from leg_distances")
leg_distances.air_distance_km.describe()

print(
    "Adding distance information to `legs` (drops rows without geolocation or speed >= 250 km/h)"
)
db.execute("""
create or replace table legs as 
select 
  *,
  air_distance_km / (actual_duration / 3600) as speed_km_h_upper_bound 
from legs join distances using(previous_stop, stop)
where
  previous_stop != stop and
  actual_duration > 0 and air_distance_km / (actual_duration / 3600) < 250
""")
print("Showing example data from result.")
df = db.sql("select * from legs order by random() limit 1").df().T
print(df)
print("Done.")
