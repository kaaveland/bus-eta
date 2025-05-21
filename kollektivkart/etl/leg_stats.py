from datetime import date
from os.path import join
import logging

from duckdb.duckdb import DuckDBPyConnection

from .partitioning import available_monthly_partitions


_datasources = """
COPY (
    SELECT dataSource, max_by(dataSourceName, operatingDate) as dataSourceName
    FROM read_parquet($arrivals, hive_partitioning=true)
    GROUP BY dataSource
) TO '{dest}' (format parquet, overwrite);
"""


def write_datasources(db: DuckDBPyConnection, root: str):
    arrivals = join(root, "arrivals.parquet/*/*")
    datasources = join(root, "datasources.parquet")
    db.execute(
        _datasources.format(dest=datasources), parameters=dict(arrivals=arrivals)
    )


_datasource_lines = """
COPY (
    SELECT DISTINCT dataSource, lineRef
    FROM read_parquet($legs, hive_partitioning=true)
) TO '{dest}' (format parquet, overwrite);
"""


def write_datasource_lines(db: DuckDBPyConnection, root: str):
    legs = join(root, "legs.parquet/*/*")
    dest = join(root, "datasource_line.parquet")
    db.execute(_datasource_lines.format(dest=dest), parameters=dict(legs=legs))


_stop_line = """
COPY (
    SELECT DISTINCT lineRef, dataSource, from_stop, to_stop
    FROM read_parquet($legs, hive_partitioning=true)
) TO '{dest}' (format parquet, overwrite);
"""


def write_stop_line(db: DuckDBPyConnection, root: str):
    legs = join(root, "legs.parquet/*/*")
    dest = join(root, "stop_line.parquet")
    db.execute(_stop_line.format(dest=dest), parameters=dict(legs=legs))


def leg_stats_partitions(db: DuckDBPyConnection, root, invalidate) -> set[date]:
    available = available_monthly_partitions(db, join(root, "legs.parquet"))
    have = available_monthly_partitions(
        db, join(root, "leg_stats.parquet"), use_trunc=False
    )
    recalculate = {max(available)}
    need = recalculate | (available - have)
    if invalidate:
        return available
    else:
        return need


_leg_stats = """
with hourly as (
  from read_parquet($legs, hive_partitioning=true)
  select 
    dataSource, 
    from_stop, 
    to_stop,
    date_trunc('month', operatingDate) as month,
    extract(hour from start_time) as hour,
    quantile_disc(
      actual_duration, .75
    ) as hourly_quartile,
    median(actual_duration) :: int2 as hourly_duration,
    median(delay) :: int2 as hourly_delay,
    median(deviation) :: int2 as hourly_deviation,
    mean(actual_duration) :: int2 as mean_hourly_duration,
    count(*) as hourly_count
  where
    extract(weekday from start_time) != 0 and extract(weekday from start_time) != 6
    and month = $month
  group by month, hour, dataSource, from_stop, to_stop
), monthly as (
  from read_parquet($legs, hive_partitioning=true)
  select 
    dataSource,
    from_stop, 
    to_stop,
    date_trunc('month', operatingDate) as month,
    median(actual_duration) :: int2 as monthly_duration,
      quantile_disc(
      actual_duration, .75
    ) as monthly_quartile,
    median(delay) :: int2 as monthly_delay,
    median(deviation) :: int2 as monthly_deviation,
    mean(actual_duration) :: int2 as mean_monthly_duration,
    count(*) as monthly_count,
    any_value(air_distance_meters) as air_distance_meters,
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
where hourly_count > 20 and air_distance_meters > 50 and month = $month
"""


def write_leg_stats(db: DuckDBPyConnection, root: str, invalidate: bool):
    partitions = leg_stats_partitions(db, root, invalidate)
    dest = join(root, "leg_stats.parquet")
    legs = join(root, "legs.parquet/*/*")
    for partition in partitions:
        logging.info("Write leg stats for partition %s", partition.isoformat())
        query = f"COPY ({_leg_stats}) TO '{dest}' (format parquet, partition_by (month), overwrite_or_ignore);"
        db.execute(query, parameters=dict(month=partition, legs=legs))


def run_job(db: DuckDBPyConnection, root: str, invalidate: bool):
    logging.info("Write datasources")
    write_datasources(db, root)
    logging.info("Write stops for lines")
    write_stop_line(db, root)
    logging.info("Write datasource lines")
    write_datasource_lines(db, root)
    logging.info("Write leg stats")
    write_leg_stats(db, root, invalidate)
