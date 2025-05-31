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
    or abs(coalesce(extract(epoch from arrivalTime - coalesce(
            lag(arrivalTime) over journey,
            lag(departureTime) over journey
          )), 0) - coalesce(extract(epoch from aimedArrivalTime - coalesce(
            lag(aimedArrivalTime) over journey,
            lag(aimedDepartureTime) over journey
          )), 0)) > 7200
  ) AND (
    (extract (epoch from coalesce(
        arrivalTime - aimedArrivalTime,
        departureTime - aimedDepartureTime))) is null
    or abs((extract (epoch from coalesce(
        arrivalTime - aimedArrivalTime,
        departureTime - aimedDepartureTime)))) < 7200
  )
"""


def create_clean_arrivals(db: DuckDBPyConnection, root: str, partition: date):
    arrivals = join(root, "arrivals.parquet/*/*")
    db.execute(_clean_arrivals, parameters=dict(arrivals=arrivals, partition=partition))


_discover_route_name = """
with journeys as (
  from clean_arrivals
  select
    operatingDate,
    dataSource,
    serviceJourneyId,
    lineRef,
    directionRef,
    max_by(stop, sequenceNr) as destination,
    min_by(stop, sequenceNr) as origin
  group by all
), counts as (
  from journeys
  select
    operatingDate,
    dataSource,
    lineRef,
    directionRef,
    destination,
    origin,
    count(*) as count
  group by all
)
from counts
select
  operatingDate,
  dataSource,
  lineRef,
  directionRef,
  max_by(destination, count) as destination,
  max_by(origin, count) as origin
group by all
"""


def create_route_name(db: DuckDBPyConnection, root: str):
    route_name = join(root, "route_name.parquet")
    q = f"copy ({_discover_route_name}) to '{route_name}' (format parquet, partition_by (operatingDate), overwrite_or_ignore);"
    db.execute(q)


_create_legs = """
with canonical as (
    from read_parquet($route_name, hive_partitioning=true)
    select
      operatingDate, lineRef, dataSource, directionRef,
      min_by(directionRef, operatingDate) over (
        partition by (dataSource, lineRef, origin, destination)
      ) as direction
)
from clean_arrivals join canonical using(operatingDate, lineRef, dataSource, directionRef)
select
  operatingDate,
  lineRef,
  dataSource,
  directionRef,
  canonical.direction as direction,  
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

  (extract (epoch from coalesce(
          arrivalTime - aimedArrivalTime,
          departureTime - aimedDepartureTime))
  ) :: int4 as delay,

  actual_duration - planned_duration as deviation,

  stop as to_stop,
  lag(stop) over w as from_stop,
  lat as to_lat,
  lon as to_lon,
  lag(lat) over w as from_lat,
  lag(lon) over w as from_lon,
  st_distance_spheroid(st_point(from_lat, from_lon), st_point(to_lat, to_lon)) :: int as air_distance_meters
window w as (
  partition by (operatingDate, serviceJourneyId) order by sequenceNr asc
)
qualify
  abs(delay) < 7200
  and from_stop is not null 
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
    route_name = join(root, "route_name.parquet/*/*")
    db.execute(
        f"COPY ({_create_legs}) to '{legs}' (format parquet, partition_by (operatingDate), overwrite_or_ignore);",
        parameters=dict(route_name=route_name),
    )


def run_job(db: DuckDBPyConnection, root: str, invalidate: bool, from_date: date):
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
    for partition in sorted(p for p in need if p >= from_date):
        logging.info("Calculate legs for partition %s", partition.isoformat())
        create_clean_arrivals(db, root, partition)
        create_route_name(db, root)
        create_legs(db, root)
