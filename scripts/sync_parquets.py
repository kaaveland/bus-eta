import argparse
from datetime import date, timedelta

import duckdb
import tqdm
from google.cloud import bigquery

parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument(
    "output",
    help="Path to place parquet archives in (use s3:// for direct-to-s3, which requires DuckDB secret)",
)
parser.add_argument(
    "--from-date",
    default=date.today() - timedelta(days=365 * 2),
    help="The first date to sync from (default two years ago)",
    type=date.fromisoformat,
)
parser.add_argument(
    "--to-date",
    default=date.today() - timedelta(days=1),
    help="The last day to include (default yesterday)",
    type=date.fromisoformat,
)
parser.add_argument(
    "--skip-stops",
    default=False,
    action="store_true",
    help="Skip the stops and quays table",
)
opts = parser.parse_args()
client = bigquery.Client()

fetch_arrivals = """
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
arrivals_dest = f"{opts.output}/arrivals.parquet"

try:
    known_partitions_rs = duckdb.query(
        "select distinct operatingDate from read_parquet($dest, hive_partitioning=true)",
        params=dict(dest=f"{arrivals_dest}/*/*"),
    ).fetchall()
    known_partitions = {row[0] for row in known_partitions_rs}
except duckdb.IOException as new:
    known_partitions = set()

desired_partitions = {
    opts.from_date + timedelta(days=i)
    for i in range((opts.to_date - opts.from_date) // timedelta(days=1))
}
missing_partitions = desired_partitions - known_partitions

fetch_stops = """
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
stops_dest = f"{opts.output}/stops.parquet"

fetch_quays = """
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
quays_dest = f"{opts.output}/quays.parquet"

if not opts.skip_stops:
    quays = client.query(fetch_quays).to_arrow()
    duckdb.query(f"copy quays to '{quays_dest}';")
    stops = client.query(fetch_stops).to_arrow()
    duckdb.query(f"copy stops to '{stops_dest}';")

for partition in tqdm.tqdm(sorted(missing_partitions)):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("operating_date", "DATE", partition)
        ]
    )
    batch = client.query(fetch_arrivals, job_config=job_config).to_arrow()
    duckdb.query(
        f"copy batch to '{arrivals_dest}' (format parquet, partition_by (operatingDate), overwrite_or_ignore);"
    )
