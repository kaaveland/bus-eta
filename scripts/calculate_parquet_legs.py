"""
Produce partitioned parquet files of all detected legs, filtering out improbable legs
"""

import argparse
from datetime import date, timedelta

import numpy as np
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


db.execute(
    """
CREATE OR REPLACE TABLE stops AS
SELECT * FROM read_parquet($stops);
""",
    parameters=dict(
        stops=f"{opts.data}/stops.parquet",
    ),
)
db.execute(
    """
CREATE OR REPLACE TABLE quays AS
SELECT * FROM read_parquet($quays);
""",
    parameters=dict(
        quays=f"{opts.data}/quays.parquet",
    ),
)
db.execute("""
CREATE OR REPLACE TABLE stopdata AS
SELECT
    quays.id as quay_id,
    stops.id as stop_id,
    coalesce(stops.location_latitude, quays.location_latitude) as lat,
    coalesce(stops.location_longitude, quays.location_longitude) as lon,
    coalesce(stops.name, quays.name) as name
FROM stops join quays on stops.id = quays.stopPlaceRef
""")

available_partitions = {
    row[0]
    for row in db.query(
        f"select distinct date_trunc('month', operatingDate) from read_parquet('{opts.data}/arrivals.parquet/*/*', hive_partitioning=true)"
    ).fetchall()
}

legs = f"{opts.data}/legs.parquet"

try:
    completed_partitions = {
        row[0]
        for row in db.sql(
            "select distinct date_trunc('month', operatingDate) from read_parquet($legs, hive_partitioning=true)",
            params=dict(legs=f"{legs}/*/*"),
        ).fetchall()
    }
except duckdb.IOException as _not_exists:
    completed_partitions = set()

required_partitions = (available_partitions - completed_partitions) | {
    max(available_partitions)  # include the latest partition always
}

legs_view = f"""
CREATE OR REPLACE TEMPORARY TABLE legs AS
SELECT
  operatingDate,
  date_trunc('month', operatingDate) as month,
  lineRef,
  dataSource,
  directionRef,

  coalesce(
    lag(arrivalTime) over w,
    lag(departureTime) over w
  ) as start_time,

  round(extract(epoch from arrivalTime - coalesce(
    lag(arrivalTime) over w,
    lag(departureTime) over w
  )), 2) :: float as actual_duration,

  round(extract(epoch from aimedArrivalTime - coalesce(
    lag(aimedArrivalTime) over w,
    lag(aimedDepartureTime) over w
  )), 2) :: float as planned_duration,

  round(extract (epoch from arrivalTime - aimedArrivalTime), 2) :: float as delay,
  actual_duration - planned_duration as deviation,

  stopPointRef as to_stop,
  lag(stopPointRef) over w as from_stop,
  serviceJourneyId
FROM read_parquet('{opts.data}/arrivals.parquet/*/*', hive_partitioning=true)
WHERE
  date_trunc('month', operatingDate) = $operatingDate
  AND NOT (estimated OR journeyCancellation OR extraCall OR stopCancellation)
  AND abs(delay) < 3600
WINDOW W AS (
  PARTITION BY (operatingDate, serviceJourneyId) ORDER BY sequenceNr asc
)
QUALIFY
  from_stop IS NOT NULL
  AND actual_duration > 0
  AND abs(deviation) < 1800;
"""

unique_legs = """
WITH unique_legs AS (
    SELECT DISTINCT from_stop, to_stop
    FROM legs
    WHERE from_stop != to_stop
)
SELECT
    legs.from_stop,
    legs.to_stop,
    prev.name as prev_name,
    now.name as now_name,
    prev.lat as prev_lat,
    prev.lon as prev_lon,
    now.lat as now_lat,
    now.lon as now_lon
FROM unique_legs legs
  JOIN stopdata prev
    ON legs.from_stop = prev.quay_id OR legs.from_stop = prev.stop_id
  JOIN stopdata now
    on legs.to_stop = now.quay_id OR legs.to_stop = now.stop_id
"""

legs_pq = f"""
COPY (
SELECT
    legs.* exclude(from_stop, to_stop),
    ld.prev_name as from_stop,
    ld.now_name as to_stop,

    ld.prev_lat :: float as from_lat,
    ld.prev_lon :: float as from_lon,

    ld.now_lat :: float as to_lat,
    ld.now_lon :: float as to_lon,

    round(ld.air_distance_km, 2) :: float as air_distance_km,
    round(ld.air_distance_km / (actual_duration / 3600), 2) :: float as speed_kmh
FROM legs
  JOIN leg_distances ld USING(from_stop, to_stop)
WHERE speed_kmh < 250
  AND ld.prev_name != ld.now_name
ORDER BY operatingDate, from_stop, lineRef -- try to get good clustering for compression purposes
) TO '{opts.data}/legs.parquet' (format parquet, partition_by (month), overwrite_or_ignore);
"""

for operating_date in tqdm.tqdm(sorted(required_partitions)):
    db.sql(legs_view, params=dict(operatingDate=operating_date))
    df = db.sql(unique_legs).df()
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
    db.execute(legs_pq)
