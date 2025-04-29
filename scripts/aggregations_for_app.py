"""
Produce aggregated data suitable for making a dash app.

This writes the following files:

- leg_stats.parquet
- stop_line.parquet
- datasource_line.parquet
- datasources.parquet

It requires:

- legs.parquet
- arrivals.parquet
"""

import argparse
from datetime import date, timedelta

import psutil
import duckdb
import tqdm

parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument(
    "data",
    help="Path for parquet archives in (use s3:// for direct-to-s3, which requires DuckDB secret)",
)
parser.add_argument(
    "--from-date",
    default=date.today() - timedelta(days=365 * 2),
    help="The first date to sync from (default two years ago)",
    type=date.fromisoformat,
)
parser.add_argument(
    "--db", default=":memory:", help="DuckDB database to place temporary tables in"
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
    help="GB of memory to allow DuckDB to use (default 80%% of available)",
    type=int,
)
opts = parser.parse_args()

db = duckdb.connect(opts.db)
db.execute(f"set threads = {opts.max_cpus}")
db.execute(f"set memory_limit = '{opts.memory_limit_gb}GB'")

legs = f"{opts.data}/legs.parquet"

db.execute(f"""
COPY (
    SELECT DISTINCT dataSource, lineRef
    FROM read_parquet('{legs}/*/*', hive_partitioning=true)
) TO '{opts.data}/datasource_line.parquet'
""")

db.execute(f"""
COPY (
    SELECT dataSource, max_by(dataSourceName, operatingDate) as dataSourceName
    FROM read_parquet('{opts.data}/arrivals.parquet/*/*', hive_partitioning=true)
    GROUP BY dataSource
) TO '{opts.data}/datasources.parquet'
""")

db.execute(f"""
COPY (
    SELECT DISTINCT lineRef, dataSource, from_stop, to_stop
    FROM read_parquet('{opts.data}/legs.parquet/*/*', hive_partitioning=true)
) TO '{opts.data}/stop_line.parquet'
""")

available_partitions = {
    row[0]
    for row in db.query(
        f"select distinct month from read_parquet('{legs}/*/*', hive_partitioning=true)"
    ).fetchall()
}
try:
    completed_partitions = {
        row[0]
        for row in db.query(
            f"""
            select distinct month from read_parquet('{opts.data}/leg_stats.parquet/*/*')
            union
            select distinct month from read_parquet('{opts.data}/line_stats.parquet/*/*')
            """
        ).fetchall()
    }
except duckdb.IOException as _not_exists:
    completed_partitions = set()

required_partitions = (available_partitions - completed_partitions) | {
    max(available_partitions)
}

agg = f"""
copy (
with hourly as (
  from read_parquet('{opts.data}/legs.parquet/*/*', hive_partitioning=true)
  select 
    dataSource, 
    from_stop, 
    to_stop,
    month,
    extract(hour from start_time) as hour,
    quantile_disc(
      actual_duration, .75
    ) as hourly_quartile,
    median(actual_duration) :: int4 as hourly_duration,
    median(delay) :: int4 as hourly_delay,
    median(deviation) :: int4 as hourly_deviation,
    count(*) as hourly_count
  where
    extract(weekday from start_time) != 0 and extract(weekday from start_time) != 6
    and month = $month  
  group by month, hour, dataSource, from_stop, to_stop
), monthly as (
  from read_parquet('{opts.data}/legs.parquet/*/*', hive_partitioning=true)
  select 
    dataSource,
    from_stop, 
    to_stop,
    month,
    median(actual_duration) :: int4 as monthly_duration,
      quantile_disc(
      actual_duration, .75
    ) as monthly_quartile,
    median(delay) :: int4 as monthly_delay,
    median(deviation) :: int4 as monthly_deviation,
    count(*) as monthly_count,
    round(any_value(air_distance_km), 1) as air_distance_km,
    any_value(from_lat) as from_lat,
    any_value(from_lon) as from_lon,
    any_value(to_lat) as to_lat,
    any_value(to_lon) as to_lon,
  where 
    extract(weekday from start_time) != 0 and extract(weekday from start_time) != 6
    and month = $month  
  group by month, dataSource, from_stop, to_stop
)
from hourly join monthly using(dataSource, from_stop, to_stop, month)
where hourly_count > 20 and air_distance_km > 0.001 and month = $month
) to '{opts.data}/leg_stats.parquet' (format parquet, partition_by (month), overwrite_or_ignore);
"""

line_agg = f"""
copy (
with hourly as (
  from read_parquet('{opts.data}/legs.parquet/*/*', hive_partitioning=true)
  select 
    dataSource, 
    from_stop, 
    to_stop,
    month,
    lineRef,
    directionRef,
    extract(hour from start_time) as hour,
    quantile_disc(
      actual_duration, .75
    ) as hourly_quartile,
    median(actual_duration) :: int4 as hourly_duration,
    median(delay) :: int4 as hourly_delay,
    median(deviation) :: int4 as hourly_deviation,
    count(*) as hourly_count
  where 
    extract(weekday from start_time) != 0 and extract(weekday from start_time) != 6
    and month = $month 
  group by month, hour, dataSource, lineRef, directionRef, from_stop, to_stop
), monthly as (
  from read_parquet('{opts.data}/legs.parquet/*/*', hive_partitioning=true)
  select 
    dataSource,
    from_stop, 
    to_stop,
    month,
    lineRef,
    directionRef,
    median(actual_duration) :: int4 as monthly_duration,
      quantile_disc(
      actual_duration, .75
    ) as monthly_quartile,
    median(delay) :: int4 as monthly_delay,
    median(deviation) :: int4 as monthly_deviation,
    count(*) as monthly_count,
    round(any_value(air_distance_km), 1) as air_distance_km,
    any_value(from_lat) as from_lat,
    any_value(from_lon) as from_lon,
    any_value(to_lat) as to_lat,
    any_value(to_lon) as to_lon,
  where 
    extract(weekday from start_time) != 0 and extract(weekday from start_time) != 6
    and month = $month  
  group by month, dataSource, lineRef, directionRef, from_stop, to_stop
)
from hourly join monthly using(dataSource, from_stop, to_stop, month)
where hourly_count >= 20 and air_distance_km > 0.001 and month = $month
) to '{opts.data}/line_stats.parquet' (format parquet, partition_by (month), overwrite_or_ignore);
"""

for month in tqdm.tqdm(sorted(required_partitions)):
    duckdb.execute(agg, parameters=dict(month=month))
    duckdb.execute(line_agg, parameters=dict(month=month))
