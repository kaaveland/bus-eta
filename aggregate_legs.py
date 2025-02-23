#!/usr/bin/env python
"""
Aggregate the `legs` table in a DuckDB instance.

This produces aggregations broken down by stop, previous stop, year, month and hour of the day.

Writes three tables:

- `leg_stats` has aggregations broken down by previous stop, stop
- `stop_stats` has aggregations broken down by stop -- all legs that end at `stop`, together
- `line_stop` maps which `lineRef`s that have been seen at which previous stop, stop for a year, month, hour
"""

import argparse

import duckdb
import psutil

parser = argparse.ArgumentParser(
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
db.execute(f"set threads = {opts.max_cpus};")
db.execute(f"set memory_limit = '{opts.memory_limit_gb}GB';")


def aggregations(col: str) -> str:
    return f"""
    {{
     'min': min({col}),
     'max': max({col}),
     'percentiles': quantile_disc({col}, [.1, .25, .5, .75, .9]),
     'median': median({col}),
     'mean': round(mean({col})),
     'stddev': round(stddev({col}))
    }} as {col}_stats
"""


db.sql(f"""
create or replace table leg_stats as
select
    stop_name as stop,
    previous_stop_name as previous_stop,
    extract(year from time) as year,
    extract(month from time) as month,
    extract(hour from time) as hour,
    count(*) as count,
    round(mean(air_distance_km), 2) as air_distance_km,
    median(planned_duration) as planned_duration,
    first(now_lon) as stop_lon,
    first(now_lat) as stop_lat,
    first(prev_lon) as prev_lon,
    first(prev_lat) as prev_lat,
    first(now_lon) * .01 + first(prev_lon) * .99 as map_lon,
    first(now_lat) * .01 + first(prev_lat) * .99 as map_lat,
    {aggregations('actual_duration')},
    {aggregations('delay')},
    {aggregations('deviation')}
from legs
group by stop_name, previous_stop_name, year, month, hour
having count(*) >= 28 -- require at least daily data
""")
print("Done creating leg_stats. Sample:")
print(db.sql("select * from leg_stats order by random() limit 1").df().T)


db.sql(f"""
create or replace table stop_stats as
select
    stop_name as stop,
    extract(year from time) as year,
    extract(month from time) as month,
    extract(hour from time) as hour,
    count(*) as count,
    first(now_lon) as map_lon,
    first(now_lat) as map_lat,
    {aggregations('delay')},
    {aggregations('deviation')}
from legs
group by stop_name, year, month, hour
having count(*) >= 28 -- require at least daily data
""")
print("Done creating stop_stats. Sample:")
print(db.sql("select * from stop_stats order by random() limit 1").df().T)

db.sql("""
create or replace table stop_line as
select
  lineRef,
  previous_stop_name as previous_stop,
  stop_name as stop,
  extract(year from time) as year,
  extract(month from time) as month,
  extract(hour from time) as hour
from legs  
group by all
""")

print("Done creating stop_line. Sample:")
print(db.sql("select * from stop_line order by random() limit 1").df().T)
