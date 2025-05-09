import logging
from os.path import join

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from pyarrow import Table
from datetime import date, timedelta

from google.cloud import bigquery

from .partitioning import available_daily_partitions

_fetch_arrivals = """
SELECT
  recordedAtTime,
  lineRef,
  directionRef,
  operatingDate,
  serviceJourneyId,
  operatorRef,
  extraJourney,
  journeyCancellation,
  stopPointRef,
  sequenceNr,
  originName,
  destinationName,
  extraCall,
  stopCancellation,
  aimedArrivalTime,
  arrivalTime,
  aimedDepartureTime,
  departureTime,
  dataSource,
  dataSourceName,
  estimated
FROM `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`
WHERE operatingDate = @operating_date
"""


def fetch_arrivals_partition(client: bigquery.Client, partition: date) -> Table:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("operating_date", "DATE", partition)
        ]
    )
    return client.query(_fetch_arrivals, job_config=job_config).to_arrow()


_fetch_quays = """
SELECT
  id,
  version,
  publicCode,
  name,
  shortName,
  description,
  location_longitude,
  location_latitude,
  stopPlaceRef
FROM `ent-data-sharing-ext-prd.national_stop_registry.quays_last_version`
"""


def fetch_quays(client: bigquery.Client) -> Table:
    return client.query(_fetch_quays).to_arrow()


_fetch_stops = """
SELECT
  id,
  version,
  publicCode,
  transportMode,
  name,
  shortName,
  description,
  location_longitude,
  location_latitude,
  topographicPlaceRef,
  alternativeNames,
  tariffZoneRefs,
  fareZoneRefs,
  validBetween,
  parentRef
FROM `ent-data-sharing-ext-prd.national_stop_registry.stop_places_last_version`
"""


def fetch_stops(client: bigquery.Client) -> Table:
    return client.query(_fetch_stops).to_arrow()


def sync_stops(client: bigquery.Client, db: DuckDBPyConnection, root: str):
    stops = fetch_stops(client)
    dest = join(root, "stops.parquet")
    db.register("stops", stops)
    db.execute(f"copy stops to '{dest}';")
    db.unregister("stops")


def sync_quays(client: bigquery.Client, db: DuckDBPyConnection, root: str):
    quays = fetch_quays(client)
    dest = join(root, "quays.parquet")
    db.register("quays", quays)
    db.execute(f"copy quays to '{dest}';")
    db.unregister("quays")


def sync_arrivals(
    client: bigquery.Client,
    db: DuckDBPyConnection,
    root: str,
    from_date: date,
    to_date: date,
):
    dest = join(root, "arrivals.parquet")
    wanted = {
        from_date + timedelta(days=i)
        for i in range((to_date - from_date) // timedelta(days=1))
    }
    available = available_daily_partitions(db, dest)
    need = wanted - available
    for partition in need:
        logging.info("Syncing %s from arrivals", partition.isoformat())
        batch = fetch_arrivals_partition(client, partition)
        db.register("batch", batch)
        duckdb.execute(
            f"copy batch to '{dest}' (format parquet, partition_by (operatingDate), overwrite_or_ignore);"
        )
        db.unregister("batch")


def run_job(
    client: bigquery.Client,
    db: DuckDBPyConnection,
    root: str,
    from_date: date,
    to_date: date,
):
    logging.info("Syncing stops from BQ")
    sync_stops(client, db, root)
    logging.info("Syncing quays from BQ")
    sync_quays(client, db, root)
    logging.info("Syncing arrivals from BQ")
    sync_arrivals(client, db, root, from_date, to_date)
