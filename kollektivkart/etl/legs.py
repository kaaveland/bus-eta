import logging
from datetime import date
from os.path import join
from duckdb.duckdb import DuckDBPyConnection

from .partitioning import available_daily_partitions


def create_stopdata(db: DuckDBPyConnection, root: str):
    stops = join(root, "stops.parquet")
    quays = join(root, "quays.parquet")

    db.execute(
        """
    create or replace temporary table stopdata as
    from read_parquet($stops) stops join read_parquet($quays) quays
    on stops.id = quays.stopPlaceRef
    select 
      quays.id as quay_id,
      stops.id as stop_id,
      coalesce(stops.location_latitude, quays.location_latitude) as lat,
      coalesce(stops.location_longitude, quays.location_longitude) as lon,
      coalesce(stops.name, quays.name) as name
    """,
        parameters=dict(stops=stops, quays=quays),
    )


_clean_arrivals = """
create or replace temporary table clean_arrivals as 
with arrivals as (
  from read_parquet($arrivals, hive_partitioning=true) where operatingDate = $partition
)
from 
  ((from arrivals join stopdata on stopPointRef = stopdata.quay_id select *)
  union all (from arrivals join stopdata on stopPointRef = stopdata.stop_id select *)) arrivals
select
  lineRef,
  directionRef,
  operatingDate,
  serviceJourneyId,
  operatorRef,
  extraJourney,
  name as stop,
  lat as lat,
  lon as lon,
  sequenceNr,
  originName,
  destinationName,
  aimedArrivalTime,
  arrivalTime,
  aimedDepartureTime,
  departureTime,
  dataSource,
  dataSourceName,
window journey as (
  partition by (serviceJourneyId, operatingDate) order by sequenceNr
), stops as (
  partition by (serviceJourneyId, operatingDate, stop_id, quay_id) order by sequenceNr
)
qualify 
  row_number() over stops = 1 AND NOT (
    bool_or(extraCall) over journey
    or bool_or(estimated) over journey
    or bool_or(journeyCancellation) over journey
    or bool_or(stopCancellation) over journey
  )
"""


def create_clean_arrivals(db: DuckDBPyConnection, root: str, partition: date):
    arrivals = join(root, "arrivals.parquet/*/*")
    db.execute(_clean_arrivals, parameters=dict(arrivals=arrivals, partition=partition))


_create_legs = """
from clean_arrivals
select
  operatingDate,
  lineRef,
  dataSource,
  directionRef,
  serviceJourneyId,
  lag(sequenceNr) over w as sequenceNr,

  coalesce(
    lag(arrivalTime) over w,
    lag(departureTime) over w
  ) as start_time,
  
  (extract(epoch from arrivalTime - coalesce(
    lag(arrivalTime) over w,
    lag(departureTime) over w
  ))) :: int4 as actual_duration,

  (extract(epoch from aimedArrivalTime - coalesce(
    lag(aimedArrivalTime) over w,
    lag(aimedDepartureTime) over w
  ))) :: int4 as planned_duration,

  (extract (epoch from arrivalTime - aimedArrivalTime)) :: int4 as delay,
  actual_duration - planned_duration as deviation,

  stop as to_stop,
  lag(stop) over w as from_stop,
  lat as to_lat,
  lon as to_lon,
  lag(lat) over w as from_lat,
  lag(lon) over w as from_lon,
  st_distance_spheroid(st_point(from_lat, from_lon), st_point(to_lat, to_lon)) :: int as air_distance_meters
where abs(delay) < 7200
window w as (
  partition by (operatingDate, serviceJourneyId) order by sequenceNr asc
)
qualify
  from_stop is not null 
  and start_time is not null 
  and planned_duration is not null 
  and planned_duration between 0 and 7200
  and abs(deviation) < 7200
  and air_distance_meters > 0
  and actual_duration > 1
  and (air_distance_meters / 1000) / (actual_duration / 3600) < 250
order by operatingDate, from_stop, lineRef
"""


def create_legs(db: DuckDBPyConnection, root: str):
    legs = join(root, "legs.parquet")
    db.execute(
        f"COPY ({_create_legs}) to '{legs}' (format parquet, partition_by (operatingDate), overwrite_or_ignore);"
    )


def run_job(db: DuckDBPyConnection, root: str, invalidate: bool):
    logging.info("Calculate legs")
    source_partitions = available_daily_partitions(db, join(root, "arrivals.parquet"))
    destination_partitions = (
        available_daily_partitions(db, join(root, "legs.parquet"))
        if not invalidate
        else set()
    )
    need = source_partitions - destination_partitions
    create_stopdata(db, root)
    logging.info("Need to calculate %s partitions", len(need))
    for partition in sorted(need):
        logging.info("Calculate legs for partition %s", partition.isoformat())
        create_clean_arrivals(db, root, partition)
        create_legs(db, root)
